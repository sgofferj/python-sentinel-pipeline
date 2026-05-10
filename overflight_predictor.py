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
"""

import os
import re
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
                    # If it looks like coordinates, store it
                    if re.match(
                        r"^-?\d+\.?\d*,-?\d+\.?\d*,-?\d+\.?\d*,-?\d+\.?\d*$", val
                    ):
                        # Use lowercase suffix of variable name as label
                        label = key.replace("BOX_", "").lower()
                        mapping[val] = label
    except Exception:
        pass
    return mapping


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
        except Exception:
            pass

    return tles


def get_next_pass(
    bbox_str: str, sat_ids: Dict[str, int], tles: Dict[int, List[str]]
) -> Optional[Dict[str, Any]]:
    """Calculates the next overflight of any satellite in the group over a SINGLE box."""
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
            t, events = satellite.find_events(observer, t0, t1, altitude_degrees=30.0)

            for ti, event in zip(t, events):
                if event == 1:  # Peak of pass
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
    """Predicts next overflights for all areas and satellites."""
    tles = fetch_tles()
    if not tles:
        return []

    coord_to_label = get_env_mapping()

    s1_boxes = func.get_boxes(os.getenv("S1_BOX", ""))
    s2_boxes = func.get_boxes(os.getenv("S2_BOX", ""))

    all_predictions = []

    for mission, boxes, sat_group in [
        ("S1", s1_boxes, SENTINELS["S1"]),
        ("S2", s2_boxes, SENTINELS["S2"]),
    ]:
        for bbox_str in boxes:
            res = get_next_pass(bbox_str, sat_group, tles)
            if res:
                label = coord_to_label.get(bbox_str, "")
                suffix = f" ({label})" if label else ""
                all_predictions.append(
                    {
                        "label": f"{mission}{suffix}",
                        "time": res["time"],
                        "raw_time": res["raw_time"],
                    }
                )

    # Sort all by time
    all_predictions.sort(key=lambda x: x["raw_time"])

    # Clean up raw_time before returning
    for p in all_predictions:
        del p["raw_time"]

    return all_predictions


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    import json

    print(json.dumps(predict_all(), indent=2))
