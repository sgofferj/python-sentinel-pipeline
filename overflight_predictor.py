#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# overflight_predictor.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Predicts the next overflight of Sentinel satellites over specified search areas.
Fetches TLEs from Celestrak and uses Skyfield for orbital propagation.
Supports multiple bounding boxes and attempts to label them using .env variable names.
Generates GeoJSON swath overlays for the viewer.
"""

import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from skyfield.api import Topos, load, EarthSatellite
import functions as func
import constants as c

# NORAD IDs for active Sentinel satellites (as of 2026 estimate)
SENTINELS = {
    "S1": {
        "Sentinel-1A": 39634,
        "Sentinel-1B": 41456,
        "Sentinel-1C": 62261,
        "Sentinel-1D": 66315,
    },
    "S2": {
        "Sentinel-2A": 40697,
        "Sentinel-2B": 42063,
        "Sentinel-2C": 60989,
    },
    "S3": {
        "Sentinel-3A": 41335,
        "Sentinel-3B": 43437,
    },
}

SWATH_WIDTH_M = {"S1": 250_000, "S2": 290_000, "S3": 1_400_000}
EARTH_RADIUS_M = 6_371_000
STEP_MINUTES = 1


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Forward azimuth (degrees) from point 1 to point 2."""
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _offset(lat: float, lon: float, bearing_deg: float, dist_m: float) -> "tuple[float, float]":
    """Returns (lon, lat) of point at distance/bearing from origin."""
    rlat = math.radians(lat)
    rlon = math.radians(lon)
    brg = math.radians(bearing_deg)
    d = dist_m / EARTH_RADIUS_M

    rlat2 = math.asin(
        math.sin(rlat) * math.cos(d) + math.cos(rlat) * math.sin(d) * math.cos(brg)
    )
    rlon2 = rlon + math.atan2(
        math.sin(brg) * math.sin(d) * math.cos(rlat), math.cos(d) - math.sin(rlat) * math.sin(rlat2)
    )
    return math.degrees(rlon2), math.degrees(rlat2)


def _swath_segment(
    lat1: float, lon1: float, lat2: float, lon2: float, swath_half: float
) -> List[List[float]]:
    """Returns a GeoJSON Polygon ring for the swath between two sub-satellite points."""
    brg = _bearing(lat1, lon1, lat2, lon2)
    # Perpendicular directions (left / right of travel)
    brg_left = (brg + 90) % 360
    brg_right = (brg - 90) % 360

    # Four corners: forward-right, forward-left, backward-left, backward-right
    fr_lon, fr_lat = _offset(lat2, lon2, brg_right, swath_half)
    fl_lon, fl_lat = _offset(lat2, lon2, brg_left, swath_half)
    bl_lon, bl_lat = _offset(lat1, lon1, brg_left, swath_half)
    br_lon, br_lat = _offset(lat1, lon1, brg_right, swath_half)

    return [[fr_lon, fr_lat], [fl_lon, fl_lat], [bl_lon, bl_lat], [br_lon, br_lat], [fr_lon, fr_lat]]


def get_boxes_union(box_str: str) -> Optional[tuple[float, float, float, float]]:
    """Returns the combined bounding box of all configured areas, or None."""
    boxes = func.get_boxes(box_str)
    if not boxes:
        return None
    w, s, e, n = 180.0, 90.0, -180.0, -90.0
    for b in boxes:
        try:
            bw, bs, be, bn = map(float, b.split(","))
            w = min(w, bw)
            s = min(s, bs)
            e = max(e, be)
            n = max(n, bn)
        except (ValueError, TypeError):
            continue
    if w >= e or s >= n:
        return None
    return (w, s, e, n)


def get_env_mapping() -> Dict[str, str]:
    """Reads .env to map coordinate strings back to their variable names (e.g. 'gulf')."""
    mapping: Dict[str, str] = {}
    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(env_path):
        return mapping

    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if re.match(r"^-?\d+\.?\d*,-?\d+\.?\d*,-?\d+\.?\d*,-?\d+\.?\d*$", val):
                        label = key.replace("BOX_", "").lower()
                        mapping[val] = label
    except Exception:
        pass
    return mapping


TLE_CACHE_TTL = timedelta(hours=2)
TLE_CACHE_PATH = os.path.join(c.DATA_DIR, "output", ".tle_cache.json")


def fetch_tles() -> Dict[int, List[str]]:
    """Fetches TLEs from Celestrak for all Sentinel NORAD IDs concurrently.
    Results are cached to disk for TLE_CACHE_TTL to avoid redundant requests.
    """
    now = datetime.now(timezone.utc)

    # Try loading from cache
    cached: dict[str, Any] = {}
    if os.path.exists(TLE_CACHE_PATH):
        try:
            with open(TLE_CACHE_PATH, "r") as f:
                cached = json.load(f)
            cached_at = datetime.fromisoformat(cached.get("cached_at", "2000-01-01T00:00:00+00:00"))
            if now - cached_at < TLE_CACHE_TTL:
                tles: dict[int, list[str]] = {}
                for k, v in cached.get("tles", {}).items():
                    tles[int(k)] = v
                if tles:
                    return tles
        except Exception:
            pass

    # Fetch fresh TLEs
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import requests  # type: ignore[import-untyped]

    ids: set[int] = set()
    for sat_type in SENTINELS.values():
        ids.update(sat_type.values())

    def _fetch_one(norad_id: int) -> tuple[int, list[str]] | None:
        try:
            url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=tle"
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            lines = resp.text.strip().splitlines()
            if len(lines) >= 3:
                return (norad_id, [lines[0].strip(), lines[1], lines[2]])
        except Exception:
            pass
        return None

    tles = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_one, nid): nid for nid in ids}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                nid, data = result
                tles[nid] = data

    # Write to cache
    if tles:
        try:
            os.makedirs(os.path.dirname(TLE_CACHE_PATH), exist_ok=True)
            with open(TLE_CACHE_PATH, "w") as f:
                json.dump({"cached_at": now.isoformat(), "tles": {str(k): v for k, v in tles.items()}}, f, indent=2)
        except Exception:
            pass

    return tles


def get_pass_tracks(
    sat_ids: Dict[str, int],
    tles: Dict[int, List[str]],
    observer: Any,
    ts: Any,
    t0: Any,
    t1: Any,
    num_passes: int = 1,
) -> List[Dict[str, Any]]:
    """
    Finds the next N satellite passes and returns full tracks (rise to set) with 5-min steps.
    Returns list of {satellite, steps: [(lat, lon, dt_utc), ...]}.
    """
    all_passes: List[tuple[Any, Any, Any, str]] = []

    for name, norad_id in sat_ids.items():
        if norad_id not in tles:
            continue
        sat_data = tles[norad_id]
        satellite = EarthSatellite(sat_data[1], sat_data[2], sat_data[0], ts)
        t_events, events = satellite.find_events(observer, t0, t1, altitude_degrees=30.0)

        rise_t = None
        for ti, event in zip(t_events, events):
            if event == 0:  # Rise
                rise_t = ti
            elif event == 2 and rise_t is not None:  # Set
                set_t = ti
                all_passes.append((rise_t, set_t, satellite, name))
                rise_t = None

    if not all_passes:
        return []

    all_passes.sort(key=lambda x: x[0].utc_datetime())
    selected = all_passes[:num_passes]

    tracks = []
    for rise_t, set_t, satellite, name in selected:
        rise_dt = rise_t.utc_datetime().replace(tzinfo=timezone.utc)
        set_dt = set_t.utc_datetime().replace(tzinfo=timezone.utc)

        steps = []
        dt = rise_dt
        while dt <= set_dt:
            ti = ts.from_datetime(dt)
            geocentric = satellite.at(ti)
            subpoint = geocentric.subpoint()
            lat = subpoint.latitude.degrees
            lon = subpoint.longitude.degrees
            steps.append((lat, lon, dt))
            dt += timedelta(minutes=STEP_MINUTES)

        last_step_dt = steps[-1][2]
        if (set_dt - last_step_dt).total_seconds() > 30:
            ti = ts.from_datetime(set_dt)
            geocentric = satellite.at(ti)
            subpoint = geocentric.subpoint()
            steps.append((subpoint.latitude.degrees, subpoint.longitude.degrees, set_dt))

        tracks.append({"satellite": name, "steps": steps})

    return tracks


BoxTuple = tuple[float, float, float, float]


def _parse_boxes(boxes_list: List[str]) -> List[BoxTuple]:
    """Pre-parse box strings into (w, s, e, n) float tuples."""
    result: List[BoxTuple] = []
    for b in boxes_list:
        try:
            w, s, e, n = map(float, b.split(","))
            result.append((w, s, e, n))
        except (ValueError, TypeError):
            continue
    return result


def _point_near_boxes(lat: float, lon: float, parsed_boxes: List[BoxTuple], margin_deg: float) -> bool:
    """Check if (lat, lon) is within margin_deg of any bounding box."""
    for w, s, e, n in parsed_boxes:
        if s - margin_deg <= lat <= n + margin_deg and w - margin_deg <= lon <= e + margin_deg:
            return True
    return False


def build_swath_geojson(
    mission: str,
    pass_track: Dict[str, Any],
) -> Dict[str, Any]:
    """Builds a GeoJSON FeatureCollection of swath rectangles for a pass."""
    steps = pass_track["steps"]
    swath_half = SWATH_WIDTH_M[mission] / 2.0

    features = []
    for i in range(len(steps) - 1):
        lat1, lon1, dt1 = steps[i]
        lat2, lon2, dt2 = steps[i + 1]

        ring = _swath_segment(lat1, lon1, lat2, lon2, swath_half)

        ts_str = dt1.strftime("%Y-%m-%dT%H:%M:%SZ")

        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {
                "timestamp": ts_str,
                "mission": mission,
                "satellite": pass_track["satellite"],
            },
        })

    return {"type": "FeatureCollection", "features": features}


def write_overpass_geojson(mission: str, fc: Dict[str, Any]) -> None:
    """Writes a GeoJSON file for a mission's overpass to output/visual/."""
    filename = f"overpass_{mission.lower()}.geojson"
    out_dir = os.path.join(c.DIRS["OUT"], "visual")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, separators=(",", ":"))

    n = len(fc.get("features", []))
    print(f"  Wrote {out_path} ({n} swath segments)", flush=True)


def get_next_pass(
    bbox_str: str, sat_ids: Dict[str, int], tles: Dict[int, List[str]]
) -> Optional[Dict[str, Any]]:
    """Legacy: returns peak pass info for a single box."""
    try:
        west, south, east, north = map(float, bbox_str.split(","))
        center_lat = (south + north) / 2
        center_lon = (west + east) / 2
        observer = Topos(latitude_degrees=center_lat, longitude_degrees=center_lon)

        ts = load.timescale()
        now = datetime.now(timezone.utc)
        t0 = ts.from_datetime(now)
        t1 = ts.from_datetime(now + timedelta(days=5))

        best_pass = None

        for name, norad_id in sat_ids.items():
            if norad_id not in tles:
                continue

            sat_data = tles[norad_id]
            satellite = EarthSatellite(sat_data[1], sat_data[2], sat_data[0], ts)
            t_events, events = satellite.find_events(observer, t0, t1, altitude_degrees=30.0)

            for ti, event in zip(t_events, events):
                if event == 1:  # Peak
                    pass_time = ti.utc_datetime().replace(tzinfo=timezone.utc)
                    if best_pass is None or pass_time < best_pass["raw_time"]:
                        best_pass = {
                            "satellite": name,
                            "raw_time": pass_time,
                        }

        if best_pass:
            return {
                "satellite": best_pass["satellite"],
                "time": best_pass["raw_time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "raw_time": best_pass["raw_time"],
            }
    except Exception:
        pass
    return None


def predict_all() -> List[Dict[str, Any]]:
    """
    Predicts next overflights for all areas and satellites.
    Also writes GeoJSON swath overlays for S1 and S2 to output/visual/.
    Returns the legacy list of next-pass labels for the inventory.
    """
    _t_start = time.time()

    tles = fetch_tles()
    _t_tle = time.time()
    if not tles:
        return []

    coord_to_label = get_env_mapping()

    s1_boxes = func.get_boxes(os.getenv("S1_BOX", ""))
    s2_boxes = func.get_boxes(os.getenv("S2_BOX", ""))
    s3_boxes = func.get_boxes(os.getenv("S3_BOX", ""))

    s1_union = get_boxes_union(os.getenv("S1_BOX", ""))
    s2_union = get_boxes_union(os.getenv("S2_BOX", ""))
    s3_union = get_boxes_union(os.getenv("S3_BOX", ""))

    ts = load.timescale()
    now = datetime.now(timezone.utc)
    t0 = ts.from_datetime(now)
    t1 = ts.from_datetime(now + timedelta(days=5))

    num_s1_passes = int(os.getenv("S1_FORECAST_PASSES", "1"))
    num_s2_passes = int(os.getenv("S2_FORECAST_PASSES", "1"))
    num_s3_passes = int(os.getenv("S3_FORECAST_PASSES", "1"))

    all_predictions: List[Dict[str, Any]] = []

    mission_configs = [
        ("S1", s1_boxes, SENTINELS["S1"], num_s1_passes, s1_union),
        ("S2", s2_boxes, SENTINELS["S2"], num_s2_passes, s2_union),
        ("S3", s3_boxes, SENTINELS["S3"], num_s3_passes, s3_union),
    ]

    # --- Per-mission propagation and detection ---
    for mission, boxes, sat_group, num_passes, union in mission_configs:
        if not union or not boxes:
            write_overpass_geojson(mission, {"type": "FeatureCollection", "features": []})
            continue

        parsed_boxes = _parse_boxes(boxes)
        if not parsed_boxes:
            write_overpass_geojson(mission, {"type": "FeatureCollection", "features": []})
            continue

        swath_half = SWATH_WIDTH_M[mission] / 2.0
        margin_deg = math.degrees(swath_half / EARTH_RADIUS_M)

        pass_tracks: List[Dict[str, Any]] = []
        _t_prop = time.time()

        for name, norad_id in sat_group.items():
            if norad_id not in tles:
                continue
            sat_data = tles[norad_id]
            satellite = EarthSatellite(sat_data[1], sat_data[2], sat_data[0], ts)

            steps: List[tuple[float, float, Any]] = []
            step_td = timedelta(minutes=STEP_MINUTES)
            end_dt = t1.utc_datetime()
            dt = t0.utc_datetime()
            while dt <= end_dt:
                ti = ts.from_datetime(dt)
                geocentric = satellite.at(ti)
                subpoint = geocentric.subpoint()
                lat = subpoint.latitude.degrees
                lon = subpoint.longitude.degrees
                steps.append((lat, lon, dt))
                dt += step_td

            near_indices: set[int] = set()
            for i, (lat, lon, _) in enumerate(steps):
                if _point_near_boxes(lat, lon, parsed_boxes, margin_deg):
                    near_indices.add(i)
                    if i > 0:
                        near_indices.add(i - 1)
                    if i < len(steps) - 1:
                        near_indices.add(i + 1)

            sorted_idx = sorted(near_indices)
            current_pass: list[int] = []
            for i in sorted_idx:
                if not current_pass or i == current_pass[-1] + 1:
                    current_pass.append(i)
                else:
                    if len(current_pass) >= 2:
                        seg = [steps[j] for j in current_pass]
                        pass_tracks.append({"satellite": name, "steps": seg})
                    current_pass = [i]
            if len(current_pass) >= 2:
                seg = [steps[j] for j in current_pass]
                pass_tracks.append({"satellite": name, "steps": seg})

        pass_tracks.sort(key=lambda t: t["steps"][0][2])
        pass_tracks = pass_tracks[:num_passes]

        if pass_tracks:
            all_features = []
            for pt in pass_tracks:
                fc = build_swath_geojson(mission, pt)
                all_features.extend(fc["features"])
            write_overpass_geojson(mission, {"type": "FeatureCollection", "features": all_features})

            _t_now = time.time()
            print(f"  {mission} propagation & detection: {_t_now - _t_prop:.2f}s", flush=True)

            peak_time = pass_tracks[0]["steps"][len(pass_tracks[0]["steps"]) // 2][2]
            for bbox_str in boxes:
                label = coord_to_label.get(bbox_str, "")
                suffix = f" ({label})" if label else ""
                all_predictions.append({
                    "label": f"{mission}{suffix}",
                    "time": peak_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "raw_time": peak_time,
                })
        else:
            write_overpass_geojson(mission, {"type": "FeatureCollection", "features": []})
            _t_now = time.time()
            print(f"  {mission} propagation & detection: {_t_now - _t_prop:.2f}s (0 passes)", flush=True)

    all_predictions.sort(key=lambda x: x["raw_time"])
    for p in all_predictions:
        del p["raw_time"]

    _t_end = time.time()
    print(f"  TLE fetch: {_t_tle - _t_start:.2f}s  Total: {_t_end - _t_start:.2f}s", flush=True)

    return all_predictions


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    print(json.dumps(predict_all(), indent=2))
