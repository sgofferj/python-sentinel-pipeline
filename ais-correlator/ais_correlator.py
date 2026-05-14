#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ais_correlator.py
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
AIS Correlator for Sentinel Satellite Imagery.
Minimalist visualization: Single circle and data block per vessel.
Interpolates position at exact acquisition time.
Memory-optimized: Uses windowed drawing for large images.
"""

import os
import sys
import json
import datetime
import math
import shutil
import re
from typing import Dict, List, Any, Tuple, Optional

import requests
import rasterio as rio
from rasterio.warp import transform_bounds
from rasterio.windows import Window
from PIL import Image, ImageDraw, ImageFont
import numpy as np
from pyproj import Transformer

# --- CONFIGURATION ---
AIS_RECORDER_URL = os.getenv("AIS_RECORDER_URL")
AIS_MAX_TIME_MINUTES = int(os.getenv("AIS_MAX_TIME_MINUTES", "30"))


def parse_utc_timestamp(ts_val: Any) -> float:
    """Parses various timestamp formats into a UTC unix timestamp."""
    if ts_val is None:
        return 0.0
    try:
        if isinstance(ts_val, (int, float)):
            return float(ts_val)

        ts_str = str(ts_val).strip()
        # Add 'Z' if missing and not already having an offset
        if "T" in ts_str and not any(x in ts_str for x in ["Z", "+", "-"]):
            ts_str += "Z"

        dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return 0.0


def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_metadata(tif_path: str) -> Dict[str, Any]:
    sidecar_path = tif_path.replace(".tif", ".json")
    if os.path.exists(sidecar_path):
        try:
            with open(sidecar_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
                return {
                    "time": meta["acquisition_time"],
                    "bounds": [
                        meta["bounds"][0][1],
                        meta["bounds"][0][0],
                        meta["bounds"][1][1],
                        meta["bounds"][1][0],
                    ],
                    "is_s1": "S1" in meta.get("product", ""),
                }
        except Exception as e:
            print(f"Warning: Could not read sidecar {sidecar_path}: {e}")

    with rio.open(tif_path) as src:
        bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        filename = os.path.basename(tif_path)
        is_s1 = "S1" in filename
        time_str = "2026-04-07T12:00:00Z"

        m = re.search(r"S1_(\d{8}T\d{6})" if is_s1 else r"-(\d{8}T\d{6}Z)", filename)
        if m:
            t = m.group(1)
            time_str = f"{t[:4]}-{t[4:6]}-{t[6:8]}T{t[9:11]}:{t[11:13]}:{t[13:15]}Z"
        return {"time": time_str, "bounds": bounds, "is_s1": is_s1}


def fetch_vessel_metadata(mmsi: int) -> Dict[str, Any]:
    if not AIS_RECORDER_URL:
        return {}
    for attempt in range(3):
        try:
            resp = requests.get(f"{AIS_RECORDER_URL}/vessels?mmsi={mmsi}", timeout=10)
            if resp.ok:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    return data[0]
                return {}
            if resp.status_code >= 500:
                print(
                    f"AIS Server Error ({resp.status_code}) fetching metadata. Retry {attempt+1}/3..."
                )
                time.sleep(2 * (attempt + 1))
                continue
            break
        except Exception:
            time.sleep(1)
    return {}


def fetch_ais_data(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not AIS_RECORDER_URL:
        raise ValueError("AIS_RECORDER_URL not defined in environment.")

    base_dt = datetime.datetime.fromisoformat(meta["time"].replace("Z", "+00:00"))
    delta = datetime.timedelta(minutes=AIS_MAX_TIME_MINUTES)

    t_from = (base_dt - delta).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_to = (base_dt + delta).strftime("%Y-%m-%dT%H:%M:%SZ")
    b = meta["bounds"]

    params = {
        "start_time": t_from,
        "end_time": t_to,
        "bbox": f"{b[0]},{b[1]},{b[2]},{b[3]}",
    }

    for attempt in range(3):
        try:
            resp = requests.get(
                f"{AIS_RECORDER_URL}/positions", params=params, timeout=60
            )
            if resp.status_code == 200:
                data = resp.json()
                print(f"[DEBUG] AIS Server returned {len(data)} reports.")
                return data

            if resp.status_code >= 500:
                print(f"AIS Server Error ({resp.status_code}). Retry {attempt+1}/3...")
                time.sleep(5 * (attempt + 1))
                continue

            resp.raise_for_status()
        except Exception as e:
            if attempt == 2:
                print(f"AIS Query failed after 3 attempts: {e}")
            else:
                time.sleep(2)

    return []


def plot_on_image(
    tif_path: str, ais_features: List[Dict[str, Any]], target_time_iso: str
):
    """Memory-efficient windowed plotting on large images."""
    out_path = tif_path.replace(".tif", "_AIS.tif")
    target_ts = parse_utc_timestamp(target_time_iso)
    print(f"[DEBUG] Target Unix Timestamp: {target_ts}")

    # Pre-load font
    try:
        font = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 20
        )
    except:  # pylint: disable=bare-except
        font = ImageFont.load_default()

    dummy_img = Image.new("RGBA", (1, 1))
    dummy_draw = ImageDraw.Draw(dummy_img)

    with rio.open(tif_path) as src:
        trans = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
        width, height = src.width, src.height
        print(f"[DEBUG] Image dimensions: {width}x{height}, CRS: {src.crs}")

        tracks: Dict[int, List[Dict[str, Any]]] = {}
        for item in ais_features:
            mmsi = item.get("mmsi")
            if mmsi is not None:
                if mmsi not in tracks:
                    tracks[mmsi] = []
                tracks[mmsi].append(item)

        placed_rects: List[List[float]] = []
        ship_annotations = []

        print(f"Analyzing {len(tracks)} vessel tracks for interpolation...")

        for mmsi, pings in tracks.items():
            # Sort by actual numeric timestamp
            pings.sort(key=lambda x: parse_utc_timestamp(x.get("timestamp")))

            p1, p2 = None, None
            for i in range(len(pings) - 1):
                t1 = parse_utc_timestamp(pings[i].get("timestamp"))
                t2 = parse_utc_timestamp(pings[i + 1].get("timestamp"))
                if t1 > 0 and t2 > 0 and t1 <= target_ts <= t2:
                    p1, p2 = pings[i], pings[i + 1]
                    break

            if not p1:
                # Fallback to latest ping within 2 hours
                latest_ping = pings[-1]
                latest_t = parse_utc_timestamp(latest_ping.get("timestamp"))
                if latest_t > 0 and abs(latest_t - target_ts) < 7200:
                    p1 = latest_ping
                else:
                    # Logic for silent debug: only print first few skipped
                    if len(ship_annotations) < 5:
                        print(
                            f"[DEBUG] Track {mmsi} skipped: No matching time window (Target {target_ts}, Latest {latest_t})"
                        )

            if not p1:
                continue

            # Interpolate or use static
            if p1 and p2:
                t1 = parse_utc_timestamp(p1["timestamp"])
                t2 = parse_utc_timestamp(p2["timestamp"])
                ratio = (target_ts - t1) / (t2 - t1)
                lon = float(p1["longitude"]) + ratio * (
                    float(p2["longitude"]) - float(p1["longitude"])
                )
                lat = float(p1["latitude"]) + ratio * (
                    float(p2["latitude"]) - float(p1["latitude"])
                )
            else:
                lon, lat = float(p1["longitude"]), float(p1["latitude"])

            # Map to pixels
            mx, my = trans.transform(lon, lat)
            py, px = src.index(mx, my)

            # Debug candidate
            if len(ship_annotations) < 3:
                print(
                    f"[DEBUG] Candidate {mmsi}: ({lon:.5f}, {lat:.5f}) -> Pixel ({px}, {py})"
                )

            if not (0 <= px < width and 0 <= py < height):
                continue

            print(f"[DEBUG] HIT: MMSI {mmsi} at pixel {px},{py}")

            v_meta = fetch_vessel_metadata(mmsi)
            name = p1.get("vessel_name") or v_meta.get("vessel_name") or "Unknown"
            imo = v_meta.get("imo") or "N/A"
            callsign = v_meta.get("callSign") or "N/A"

            # Labels and collisions
            text = f"NAME: {name}\nMMSI: {mmsi}\nIMO: {imo}\nCALL: {callsign}"
            bbox = dummy_draw.textbbox((0, 0), text, font=font)
            tw, th, padding = bbox[2] - bbox[0], bbox[3] - bbox[1], 10

            offsets = [
                (50, -th // 2),
                (50, 50),
                (0, 50),
                (-50 - tw, 50),
                (-50 - tw, -th // 2),
                (-50 - tw, -50 - th),
                (0, -50 - th),
                (50, -50 - th),
            ]
            best_pos = (px + 50, py - th // 2)
            for ox, oy in offsets:
                tx, ty = px + ox, py + oy
                rect = [
                    tx - padding,
                    ty - padding,
                    tx + tw + padding,
                    ty + th + padding,
                ]
                if tx < 0 or ty < 0 or tx + tw > width or ty + th > height:
                    continue
                if any(
                    not (
                        rect[2] < r[0]
                        or rect[0] > r[2]
                        or rect[3] < r[1]
                        or rect[1] > r[3]
                    )
                    for r in placed_rects
                ):
                    continue
                best_pos = (tx, ty)
                placed_rects.append(rect)
                break
            else:
                tx, ty = best_pos
                placed_rects.append(
                    [tx - padding, ty - padding, tx + tw + padding, ty + th + padding]
                )

            ship_annotations.append(
                {
                    "px": px,
                    "py": py,
                    "text_pos": best_pos,
                    "text": text,
                    "tw": tw,
                    "th": th,
                    "padding": padding,
                }
            )

        if not ship_annotations:
            print("No vessels plotted on the image area.")
            return

        # 2. Windowed Drawing
        print(f"Plotting {len(ship_annotations)} vessels using windowed updates...")
        shutil.copy(tif_path, out_path)

        with rio.open(out_path, "r+") as dst:
            for ann in ship_annotations:
                px, py = ann["px"], ann["py"]
                tx, ty = ann["text_pos"]
                tw, th, padding = ann["tw"], ann["th"], ann["padding"]

                min_x = int(min(px - 15, tx - padding) - 5)
                max_x = int(max(px + 15, tx + tw + padding) + 5)
                min_y = int(min(py - 15, ty - padding) - 5)
                max_y = int(max(py + 15, ty + th + padding) + 5)

                min_x, max_x = max(0, min_x), min(width, max_x)
                min_y, max_y = max(0, min_y), min(height, max_y)

                if max_x <= min_x or max_y <= min_y:
                    continue

                win = Window(min_x, min_y, max_x - min_x, max_y - min_y)
                if dst.count >= 4:
                    win_data = dst.read([1, 2, 3, 4], window=win)
                else:
                    rgb = dst.read([1, 2, 3], window=win)
                    alpha = np.ones((win.height, win.width), dtype=np.uint8) * 255
                    win_data = np.concatenate([rgb, alpha[np.newaxis, ...]], axis=0)

                win_img = Image.fromarray(np.transpose(win_data, (1, 2, 0)), "RGBA")
                win_draw = ImageDraw.Draw(win_img, "RGBA")

                lpx, lpy = px - min_x, py - min_y
                ltx, lty = tx - min_x, ty - min_y

                win_draw.ellipse(
                    [lpx - 15, lpy - 15, lpx + 15, lpy + 15],
                    outline=(255, 235, 59, 255),
                    width=3,
                )
                win_draw.line(
                    [lpx, lpy, ltx + (tw if lpx > ltx else 0), lty + th // 2],
                    fill=(255, 235, 59, 180),
                    width=2,
                )
                win_draw.rectangle(
                    [
                        ltx - padding,
                        lty - padding,
                        ltx + tw + padding,
                        lty + th + padding,
                    ],
                    fill=(0, 0, 0, 160),
                    outline=(255, 235, 59, 200),
                    width=1,
                )
                win_draw.text(
                    (ltx, lty), ann["text"], font=font, fill=(255, 235, 59, 255)
                )

                final_win_data = np.transpose(np.array(win_img), (2, 0, 1))
                dst.write(final_win_data, window=win)

    print(f"AIS Correlation complete: {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ais_correlator.py <path_to_satellite_tif>")
        sys.exit(1)
    target_tif_arg = sys.argv[1]
    if not os.path.exists(target_tif_arg):
        print(f"Error: File not found {target_tif_arg}")
        sys.exit(1)
    try:
        metadata_main = get_metadata(target_tif_arg)
        ais_data_main = fetch_ais_data(metadata_main)
        if not ais_data_main:
            print("No AIS data found for this time/area.")
        else:
            plot_on_image(target_tif_arg, ais_data_main, metadata_main["time"])
    except Exception as e_main:
        print(f"Error during AIS correlation: {e_main}")
        sys.exit(1)
