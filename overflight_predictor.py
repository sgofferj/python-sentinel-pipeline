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
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from skyfield.api import Topos, load, EarthSatellite
import functions as func

# NORAD IDs for active Sentinel satellites (as of 2026 estimate)
# S1A: 39634, S1C: 62235 (launched Dec 2024)
# S2A: 40697, S2B: 42063, S2C: 61005 (launched Sept 2024)
SENTINELS = {
    "S1": {
        "Sentinel-1A": 39634,
        "Sentinel-1C": 62235,
    },
    "S2": {
        "Sentinel-2A": 40697,
        "Sentinel-2B": 42063,
        "Sentinel-2C": 61005,
    },
}

TLE_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=resource&FORMAT=tle"
# Alternatively, use specific IDs:
# https://celestrak.org/NORAD/elements/gp.php?CATNR=39634,62235,40697,42063,61005&FORMAT=tle


def fetch_tles() -> Dict[int, List[str]]:
    """Fetches TLEs from Celestrak."""
    try:
        ts = load.timescale()
        # We can't easily use skyfield's load.tle_file directly for specific filtered IDs from Celestrak
        # easily without saving to a file, so we'll just download and parse.
        import requests

        ids: List[int] = []
        for sat_type in SENTINELS.values():
            ids.extend(sat_type.values())


        url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={','.join(map(str, ids))}&FORMAT=tle"
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        lines = response.text.strip().splitlines()
        tles = {}
        for i in range(0, len(lines), 3):
            name = lines[i].strip()
            line1 = lines[i + 1]
            line2 = lines[i + 2]
            norad_id = int(line2[2:7])
            tles[norad_id] = [name, line1, line2]
        return tles
    except Exception as e:
        print(f"Error fetching TLEs: {e}", flush=True)
        return {}


def get_next_overflight(
    bbox_str: str, sat_ids: Dict[str, int], tles: Dict[int, List[str]]
) -> Optional[Dict[str, Any]]:
    """Calculates the next overflight of any satellite in the group over the BBOX."""
    if not bbox_str:
        return None

    try:
        west, south, east, north = map(float, bbox_str.split(","))
        # Use the center of the BBOX as the observer point
        center_lat = (south + north) / 2
        center_lon = (west + east) / 2
        observer = Topos(latitude_degrees=center_lat, longitude_degrees=center_lon)

        ts = load.timescale()
        now = datetime.now(timezone.utc)
        t0 = ts.from_datetime(now)
        t1 = ts.from_datetime(now + timedelta(days=5))  # Look 5 days ahead

        best_pass = None

        for name, norad_id in sat_ids.items():
            if norad_id not in tles:
                continue

            sat_data = tles[norad_id]
            satellite = EarthSatellite(sat_data[1], sat_data[2], sat_data[0], ts)

            # Find passes over the observer
            # altitude_degrees threshold: Sentinel footprint is roughly 250km wide (S1) or 290km (S2)
            # Sentinel altitude is ~700-800km.
            # Tan(theta) = (Width/2) / Altitude.
            # For S2: 145 / 786 = ~0.18 -> theta = ~10.5 degrees.
            # So a 80 degree elevation (90-10.5) is needed for center? No, elevation from horizon.
            # 10.5 degrees from nadir means the satellite is visible at ~80 degrees elevation if it's perfectly passing over.
            # Actually, to be in the footprint, it doesn't need to be 90 degrees.
            # We use a conservative 30 degrees for "nearby" and 70+ for "likely overflight".
            # Let's use 60 degrees as a threshold for "meaningful overflight" for narrow-swath optical.
            t, events = satellite.find_events(observer, t0, t1, altitude_degrees=30.0)

            for ti, event in zip(t, events):
                if event == 1:  # Peak of pass
                    pass_time = ti.utc_datetime().replace(tzinfo=timezone.utc)
                    if best_pass is None or pass_time < best_pass["time"]:
                        best_pass = {
                            "satellite": name,
                            "time": pass_time.isoformat().replace("+00:00", "Z"),
                            "raw_time": pass_time,
                        }

        if best_pass:
            del best_pass["raw_time"]
            return best_pass
    except Exception as e:
        print(f"Error predicting overflight for {bbox_str}: {e}", flush=True)

    return None


def predict_all() -> Dict[str, Any]:
    """Predicts next overflights for both S1 and S2."""
    tles = fetch_tles()
    if not tles:
        return {}

    # Load boxes from environment
    s1_boxes = func.get_boxes(os.getenv("S1_BOX"))
    s2_boxes = func.get_boxes(os.getenv("S2_BOX"))

    results = {}

    if s1_boxes:
        # For simplicity, we take the first box or the "most important" one.
        # Usually users have one main area.
        res = get_next_overflight(s1_boxes[0], SENTINELS["S1"], tles)
        if res:
            results["S1"] = res

    if s2_boxes:
        res = get_next_overflight(s2_boxes[0], SENTINELS["S2"], tles)
        if res:
            results["S2"] = res

    return results


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    print(predict_all())
