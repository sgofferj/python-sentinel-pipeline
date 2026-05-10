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
Supports multiple bounding boxes and returns the earliest pass for each sensor type.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from skyfield.api import Topos, load, EarthSatellite
import functions as func

# NORAD IDs for active Sentinel satellites (as of 2026 estimate)
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


def fetch_tles() -> Dict[int, List[str]]:
    """Fetches TLEs from Celestrak for Sentinels individually."""
    import requests

    ids: List[int] = []
    for sat_type in SENTINELS.values():
        ids.extend(sat_type.values())

    tles = {}
    for norad_id in ids:
        try:
            url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=tle"
            response = requests.get(url, timeout=20)
            response.raise_for_status()

            lines = response.text.strip().splitlines()
            if len(lines) >= 3:
                name = lines[0].strip()
                line1 = lines[1]
                line2 = lines[2]
                tles[norad_id] = [name, line1, line2]
            else:
                print(f"Warning: Malformed TLE response for {norad_id}", flush=True)
        except Exception as e:
            print(f"Error fetching TLE for {norad_id}: {e}", flush=True)

    return tles


def get_earliest_overflight(
    boxes: List[str], sat_ids: Dict[str, int], tles: Dict[int, List[str]]
) -> Optional[Dict[str, Any]]:
    """Calculates the earliest overflight of any satellite in the group over ANY of the boxes."""
    if not boxes:
        return None

    try:
        ts = load.timescale()
        now = datetime.now(timezone.utc)
        t0 = ts.from_datetime(now)
        t1 = ts.from_datetime(now + timedelta(days=5))  # Look 5 days ahead

        best_pass = None

        for bbox_str in boxes:
            west, south, east, north = map(float, bbox_str.split(","))
            # Use the center of the BBOX as the observer point
            center_lat = (south + north) / 2
            center_lon = (west + east) / 2
            observer = Topos(latitude_degrees=center_lat, longitude_degrees=center_lon)

            for name, norad_id in sat_ids.items():
                if norad_id not in tles:
                    continue

                sat_data = tles[norad_id]
                satellite = EarthSatellite(sat_data[1], sat_data[2], sat_data[0], ts)

                # Find passes over the observer
                # altitude_degrees threshold: 30.0 for general proximity
                t, events = satellite.find_events(
                    observer, t0, t1, altitude_degrees=30.0
                )

                for ti, event in zip(t, events):
                    if event == 1:  # Peak of pass
                        pass_time = ti.utc_datetime().replace(tzinfo=timezone.utc)
                        if best_pass is None or pass_time < best_pass["raw_time"]:
                            best_pass = {
                                "satellite": name,
                                "raw_time": pass_time,
                                "bbox": bbox_str,
                            }

        if best_pass:
            return {
                "satellite": best_pass["satellite"],
                "time": best_pass["raw_time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
    except Exception as e:
        print(f"Error predicting overflight: {e}", flush=True)

    return None


def predict_all() -> Dict[str, Any]:
    """Predicts next overflights for both S1 and S2 constellations across all search areas."""
    tles = fetch_tles()
    if not tles:
        return {}

    # Load boxes from environment
    s1_boxes = func.get_boxes(os.getenv("S1_BOX"))
    s2_boxes = func.get_boxes(os.getenv("S2_BOX"))

    results = {}

    if s1_boxes:
        res = get_earliest_overflight(s1_boxes, SENTINELS["S1"], tles)
        if res:
            results["S1"] = res

    if s2_boxes:
        res = get_earliest_overflight(s2_boxes, SENTINELS["S2"], tles)
        if res:
            results["S2"] = res

    return results


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    print(predict_all())
