#!/usr/bin/env python3
"""Download the authoritative source files required for the NJ Transit accessibility analysis.

This GitHub Actions acquisition step exists because the analysis container has no outbound
network access. It preserves original source archives, records URLs and checksums, and
produces a machine-readable GTFS inventory. The analytical calculations are performed from
these exact files by the production code included in the final package.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path("transit_accessibility_output")
CORE = ROOT / "core"
OSM = ROOT / "osm"
GTFS = ROOT / "gtfs"
META = ROOT / "metadata"
for d in (CORE, OSM, GTFS, META):
    d.mkdir(parents=True, exist_ok=True)

RETRIEVED_AT = datetime.now(timezone.utc).isoformat()
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "NJ-Transit-Accessibility-Study/1.0 (public-data acquisition; contact: liam.blank@gmail.com)"
})


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{stamp}] {message}", flush=True)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path, *, timeout: int = 180, retries: int = 4) -> dict[str, Any]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            log(f"Download {url} -> {dest}")
            with SESSION.get(url, stream=True, timeout=(30, timeout), allow_redirects=True) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(dest.suffix + ".part")
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                if tmp.stat().st_size == 0:
                    raise RuntimeError("empty response")
                tmp.replace(dest)
                return {
                    "ok": True,
                    "requested_url": url,
                    "resolved_url": str(r.url),
                    "status_code": r.status_code,
                    "bytes": dest.stat().st_size,
                    "sha256": sha256(dest),
                    "last_modified": r.headers.get("Last-Modified", ""),
                    "etag": r.headers.get("ETag", ""),
                }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            log(f"Attempt {attempt}/{retries} failed: {last_error}")
            time.sleep(min(20, attempt * 4))
    return {"ok": False, "requested_url": url, "error": last_error}


def read_zip_csv(path: Path, member: str, **kwargs: Any) -> pd.DataFrame:
    with zipfile.ZipFile(path) as zf:
        names = {Path(n).name.lower(): n for n in zf.namelist() if not n.endswith("/")}
        key = member.lower()
        if key not in names:
            return pd.DataFrame()
        with zf.open(names[key]) as f:
            return pd.read_csv(f, low_memory=False, **kwargs)


def gtfs_date_range(path: Path) -> tuple[str, str]:
    starts: list[str] = []
    ends: list[str] = []
    fi = read_zip_csv(path, "feed_info.txt", dtype=str)
    if not fi.empty:
        for col, target in (("feed_start_date", starts), ("feed_end_date", ends)):
            if col in fi.columns:
                target.extend(fi[col].dropna().astype(str).tolist())
    cal = read_zip_csv(path, "calendar.txt", dtype=str)
    if not cal.empty:
        if "start_date" in cal.columns:
            starts.extend(cal["start_date"].dropna().astype(str).tolist())
        if "end_date" in cal.columns:
            ends.extend(cal["end_date"].dropna().astype(str).tolist())
    cd = read_zip_csv(path, "calendar_dates.txt", dtype=str)
    if not cd.empty and "date" in cd.columns:
        dates = cd["date"].dropna().astype(str).tolist()
        starts.extend(dates)
        ends.extend(dates)
    clean = lambda xs: sorted(x for x in xs if re.fullmatch(r"\d{8}", x or ""))
    s = clean(starts)
    e = clean(ends)
    return (s[0] if s else "", e[-1] if e else "")


def inspect_gtfs(path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        with zipfile.ZipFile(path) as zf:
            basenames = {Path(n).name.lower() for n in zf.namelist() if not n.endswith("/")}
            required = {"agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
            out["valid_zip"] = True
            out["required_files_present"] = required.issubset(basenames) and (
                "calendar.txt" in basenames or "calendar_dates.txt" in basenames
            )
            out["members"] = len(basenames)
        agencies = read_zip_csv(path, "agency.txt", dtype=str)
        routes = read_zip_csv(path, "routes.txt", dtype=str)
        stops = read_zip_csv(path, "stops.txt", dtype=str)
        trips = read_zip_csv(path, "trips.txt", dtype=str)
        start, end = gtfs_date_range(path)
        out.update({
            "agency_names": " | ".join(sorted(set(agencies.get("agency_name", pd.Series(dtype=str)).dropna().astype(str))))[:1000],
            "route_count": int(len(routes)),
            "trip_count": int(len(trips)),
            "stop_count": int(len(stops)),
            "route_types": ",".join(sorted(set(routes.get("route_type", pd.Series(dtype=str)).dropna().astype(str)))),
            "service_start": start,
            "service_end": end,
        })
        if {"stop_lat", "stop_lon"}.issubset(stops.columns):
            lat = pd.to_numeric(stops["stop_lat"], errors="coerce")
            lon = pd.to_numeric(stops["stop_lon"], errors="coerce")
            out.update({
                "min_lat": float(lat.min()) if lat.notna().any() else None,
                "max_lat": float(lat.max()) if lat.notna().any() else None,
                "min_lon": float(lon.min()) if lon.notna().any() else None,
                "max_lon": float(lon.max()) if lon.notna().any() else None,
            })
    except Exception as exc:
        out.update({"valid_zip": False, "required_files_present": False, "inspect_error": f"{type(exc).__name__}: {exc}"})
    return out


core_sources = [
    {
        "id": "tiger_blocks_2020_nj",
        "url": "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_34_tabblock20.zip",
        "path": CORE / "tl_2020_34_tabblock20.zip",
        "description": "2020 Census tabulation blocks, New Jersey",
    },
    {
        "id": "tiger_state_2024",
        "url": "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip",
        "path": CORE / "tl_2024_us_state.zip",
        "description": "2024 TIGER/Line state boundary",
    },
    {
        "id": "lodes_wac_2023_nj",
        "url": "https://lehd.ces.census.gov/data/lodes/LODES8/nj/wac/nj_wac_S000_JT00_2023.csv.gz",
        "path": CORE / "nj_wac_S000_JT00_2023.csv.gz",
        "description": "2023 LODES workplace area characteristics, all jobs",
    },
    {
        "id": "osm_geofabrik_nj",
        "url": "https://download.geofabrik.de/north-america/us/new-jersey-latest.osm.pbf",
        "path": OSM / "new-jersey-latest.osm.pbf",
        "description": "Current OpenStreetMap New Jersey extract",
    },
]
source_rows: list[dict[str, Any]] = []
for src in core_sources:
    result = download(src["url"], src["path"], timeout=600)
    source_rows.append({
        "source_id": src["id"],
        "description": src["description"],
        "retrieved_at": RETRIEVED_AT,
        "local_file": str(src["path"]),
        **result,
    })
    if not result.get("ok"):
        raise SystemExit(f"Required source failed: {src['id']}: {result}")

log("Download 2020 PL block population and housing-unit counts")
county_fips = [
    "001", "003", "005", "007", "009", "011", "013", "015", "017", "019", "021",
    "023", "025", "027", "029", "031", "033", "035", "037", "039", "041",
]
pl_frames: list[pd.DataFrame] = []
pl_errors: list[dict[str, str]] = []
for county in county_fips:
    params = {
        "get": "NAME,P1_001N,H1_001N",
        "for": "block:*",
        "in": f"state:34 county:{county}",
    }
    url = "https://api.census.gov/data/2020/dec/pl"
    try:
        r = SESSION.get(url, params=params, timeout=180)
        r.raise_for_status()
        payload = r.json()
        frame = pd.DataFrame(payload[1:], columns=payload[0])
        pl_frames.append(frame)
        log(f"PL county {county}: {len(frame):,} blocks")
    except Exception as exc:
        pl_errors.append({"county": county, "error": f"{type(exc).__name__}: {exc}"})
if pl_errors:
    (META / "pl_download_errors.json").write_text(json.dumps(pl_errors, indent=2), encoding="utf-8")
    raise SystemExit(f"2020 PL block API failures: {pl_errors}")
pl = pd.concat(pl_frames, ignore_index=True)
pl["GEOID20"] = pl["state"] + pl["county"] + pl["tract"] + pl["block"]
pl = pl.rename(columns={"P1_001N": "population_2020", "H1_001N": "housing_units_2020"})
pl[["GEOID20", "population_2020", "housing_units_2020"]].to_csv(CORE / "nj_2020_block_population_housing.csv", index=False)
source_rows.append({
    "source_id": "census_pl_2020_blocks_nj",
    "description": "2020 Decennial PL 94-171 block population and housing units",
    "retrieved_at": RETRIEVED_AT,
    "requested_url": "https://api.census.gov/data/2020/dec/pl",
    "resolved_url": "county-by-county API queries",
    "ok": True,
    "bytes": (CORE / "nj_2020_block_population_housing.csv").stat().st_size,
    "sha256": sha256(CORE / "nj_2020_block_population_housing.csv"),
    "local_file": str(CORE / "nj_2020_block_population_housing.csv"),
})

ntd_url = "https://data.transportation.gov/resource/2u7n-ub22.csv?$limit=50000"
ntd_path = META / "fta_gtfs_weblinks.csv"
ntd_result = download(ntd_url, ntd_path, timeout=300)
source_rows.append({
    "source_id": "fta_gtfs_weblinks",
    "description": "FTA General Transit Feed Specification Weblinks register",
    "retrieved_at": RETRIEVED_AT,
    "local_file": str(ntd_path),
    **ntd_result,
})

catalog_dir = Path("work/mobility-database-catalogs")
if catalog_dir.exists():
    shutil.rmtree(catalog_dir)
subprocess.run(
    ["git", "clone", "--depth", "1", "https://github.com/MobilityData/mobility-database-catalogs.git", str(catalog_dir)],
    check=True,
)

catalog_by_id: dict[int, dict[str, Any]] = {}
for path in catalog_dir.glob("catalogs/sources/gtfs/schedule/*.json"):
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        sid = int(obj["mdb_source_id"])
        obj["_catalog_path"] = str(path)
        catalog_by_id[sid] = obj
    except Exception:
        continue

curated = [
    {"key": "njt_bus", "mdb": 508, "class": "local_bus", "core": True},
    {"key": "njt_rail", "mdb": 509, "class": "rail", "core": True},
    {"key": "path", "mdb": 517, "class": "rapid_transit", "core": True},
    {"key": "patco", "mdb": 3035, "class": "rapid_transit", "core": True},
    {"key": "septa_bus", "mdb": 502, "class": "local_bus", "core": True},
    {"key": "septa_rail", "mdb": 503, "class": "rail", "core": True},
    {"key": "ny_waterway", "mdb": 3192, "class": "ferry", "core": True},
    {"key": "boxcar", "mdb": 3105, "class": "commuter_bus", "core": False},
    {"key": "amtrak", "mdb": 11, "class": "intercity_rail", "core": False},
    {"key": "megabus", "mdb": 2321, "class": "intercity_bus", "core": False},
    {"key": "trailways_adp", "mdb": 823, "class": "intercity_bus", "core": False},
    {"key": "trailways_nyp", "mdb": 824, "class": "intercity_bus", "core": False},
    {"key": "academy", "mdb": 209, "class": "commuter_bus", "core": False},
    {"key": "peter_pan", "mdb": 496, "class": "intercity_bus", "core": False},
]
extras = [
    {"key": "seastreak", "provider": "Seastreak", "name": "Ferry", "class": "ferry", "core": True, "direct": "http://seastreak.com/api/transit/google_transit.zip", "fallback": ""},
    {"key": "lanta", "provider": "Lehigh and Northampton Transportation Authority", "name": "Bus", "class": "local_bus", "core": False, "direct": "https://realtimelanta.availtec.com/InfoPoint/GTFS-Zip.ashx", "fallback": ""},
    {"key": "princeton_tigertransit", "provider": "Princeton University", "name": "TigerTransit", "class": "local_bus", "core": False, "direct": "https://princeton.tripshot.com/v1/gtfs.zip", "fallback": ""},
    {"key": "trans_bridge", "provider": "Trans-Bridge Lines", "name": "Bus", "class": "intercity_bus", "core": False, "direct": "https://www.njtransit.com/Trans-Bridge_bus_data.zip", "fallback": ""},
    {"key": "rutgers_transit", "provider": "Rutgers University", "name": "Campus Bus", "class": "local_bus", "core": False, "direct": "https://rutgers.tripshot.com/v1/gtfs.zip", "fallback": ""},
]

feed_specs: list[dict[str, Any]] = []
for item in curated:
    obj = catalog_by_id.get(item["mdb"], {})
    urls = obj.get("urls", {})
    feed_specs.append({
        **item,
        "provider": obj.get("provider", item["key"]),
        "name": obj.get("name", ""),
        "direct": urls.get("direct_download", ""),
        "fallback": urls.get("latest", ""),
        "license_url": urls.get("license", ""),
        "catalog_path": obj.get("_catalog_path", ""),
    })
feed_specs.extend(extras)

feed_inventory: list[dict[str, Any]] = []
seen_hashes: dict[str, str] = {}
for spec in feed_specs:
    key = spec["key"]
    dest = GTFS / f"{key}.zip"
    attempts: list[dict[str, Any]] = []
    result: dict[str, Any] = {"ok": False, "error": "no URL"}
    used_url = ""
    for url in [spec.get("direct", ""), spec.get("fallback", "")]:
        if not url:
            continue
        used_url = url
        result = download(url, dest, timeout=600, retries=3)
        attempts.append(result)
        if result.get("ok"):
            inspected = inspect_gtfs(dest)
            if inspected.get("valid_zip") and inspected.get("required_files_present"):
                break
            result = {**result, "ok": False, "error": f"invalid GTFS archive: {inspected}"}
    inspected = inspect_gtfs(dest) if dest.exists() else {}
    row = {
        "feed_key": key,
        "provider": spec.get("provider", ""),
        "feed_name": spec.get("name", ""),
        "service_class": spec.get("class", ""),
        "core_feed": bool(spec.get("core", False)),
        "mdb_source_id": spec.get("mdb", ""),
        "direct_url": spec.get("direct", ""),
        "mirror_url": spec.get("fallback", ""),
        "license_url": spec.get("license_url", ""),
        "retrieval_date": RETRIEVED_AT,
        "download_url_used": result.get("resolved_url", used_url),
        "download_ok": bool(result.get("ok")),
        "download_error": result.get("error", ""),
        "file_bytes": dest.stat().st_size if dest.exists() else 0,
        "sha256": sha256(dest) if dest.exists() else "",
        "attempts_json": json.dumps(attempts),
        **inspected,
    }
    if row["sha256"]:
        if row["sha256"] in seen_hashes:
            row["duplicate_of"] = seen_hashes[row["sha256"]]
        else:
            seen_hashes[row["sha256"]] = key
            row["duplicate_of"] = ""
    else:
        row["duplicate_of"] = ""
    feed_inventory.append(row)
    log(f"GTFS {key}: ok={row['download_ok']} dates={row.get('service_start','')}..{row.get('service_end','')} stops={row.get('stop_count',0)}")

pd.DataFrame(feed_inventory).to_csv(META / "gtfs_download_inventory.csv", index=False)
(META / "gtfs_download_inventory.json").write_text(json.dumps(feed_inventory, indent=2, default=str), encoding="utf-8")
pd.DataFrame(source_rows).to_csv(META / "source_download_inventory.csv", index=False)
(META / "source_download_inventory.json").write_text(json.dumps(source_rows, indent=2, default=str), encoding="utf-8")

manifest_rows: list[dict[str, Any]] = []
for path in sorted(ROOT.rglob("*")):
    if path.is_file():
        manifest_rows.append({"path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size, "sha256": sha256(path)})
pd.DataFrame(manifest_rows).to_csv(META / "file_manifest.csv", index=False)
with (META / "SHA256SUMS.txt").open("w", encoding="utf-8") as f:
    for row in manifest_rows:
        f.write(f"{row['sha256']}  {row['path']}\n")

run_summary = {
    "retrieved_at": RETRIEVED_AT,
    "core_source_count": len(core_sources) + 2,
    "gtfs_attempted": len(feed_inventory),
    "gtfs_downloaded": sum(bool(r.get("download_ok")) for r in feed_inventory),
    "gtfs_valid": sum(bool(r.get("valid_zip")) and bool(r.get("required_files_present")) for r in feed_inventory),
    "total_bytes": sum(p.stat().st_size for p in ROOT.rglob("*") if p.is_file()),
}
(META / "acquisition_summary.json").write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
log(f"Acquisition complete: {json.dumps(run_summary)}")
