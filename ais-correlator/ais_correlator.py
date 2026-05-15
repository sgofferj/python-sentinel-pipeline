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
Strip-based rendering for maximum reliability and memory efficiency.
"""

import os
import sys
import json
import datetime
import math
import re
import time
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
        time_part = ts_str.split("T")[-1] if "T" in ts_str else ts_str

        if not any(x in time_part for x in ["Z", "+", "-"]):
            ts_str += "Z"

        dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


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
        except Exception:
            pass

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
                time.sleep(2 * (attempt + 1))
                continue
            break
        except Exception:
            time.sleep(1)
    return {}


def fetch_ais_data(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not AIS_RECORDER_URL:
        raise ValueError("AIS_RECORDER_URL not defined.")

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
                return resp.json()
            if resp.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
        except Exception:
            time.sleep(2)
    return []


def plot_on_image(
    tif_path: str, ais_features: List[Dict[str, Any]], target_time_iso: str
):
    """Robust strip-based rendering for large images."""
    out_path = tif_path.replace(".tif", "_AIS.tif")
    target_ts = parse_utc_timestamp(target_time_iso)

    try:
        font = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 20
        )
    except:
        font = ImageFont.load_default()

    dummy_draw = ImageDraw.Draw(Image.new("RGBA", (1, 1)))

    with rio.open(tif_path) as src:
        trans = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
        width, height = src.width, src.height
        profile = src.profile.copy()
        
        # Force 4-band RGBA, Tiled, and BIGTIFF for rendering
        profile.update(
            count=4, 
            driver="GTiff", 
            tiled=True, 
            blockxsize=512, 
            blockysize=512, 
            compress="lzw",
            BIGTIFF="YES"
        )

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
            pings.sort(key=lambda x: parse_utc_timestamp(x.get("timestamp")))
            p1, p2 = None, None
            for i in range(len(pings) - 1):
                t1 = parse_utc_timestamp(pings[i].get("timestamp"))
                t2 = parse_utc_timestamp(pings[i + 1].get("timestamp"))
                if t1 > 0 and t2 > 0 and t1 <= target_ts <= t2:
                    p1, p2 = pings[i], pings[i + 1]
                    break

            if not p1:
                latest_ping = pings[-1]
                latest_t = parse_utc_timestamp(latest_ping.get("timestamp"))
                if latest_t > 0 and abs(latest_t - target_ts) < 7200:
                    p1 = latest_ping

            if not p1:
                continue

            if p1 and p2:
                t1, t2 = parse_utc_timestamp(p1["timestamp"]), parse_utc_timestamp(p2["timestamp"])
                ratio = (target_ts - t1) / (t2 - t1)
                lon = float(p1["longitude"]) + ratio * (float(p2["longitude"]) - float(p1["longitude"]))
                lat = float(p1["latitude"]) + ratio * (float(p2["latitude"]) - float(p1["latitude"]))
            else:
                lon, lat = float(p1["longitude"]), float(p1["latitude"])

            mx, my = trans.transform(lon, lat)
            py, px = src.index(mx, my)

            if not (0 <= px < width and 0 <= py < height):
                continue

            v_meta = fetch_vessel_metadata(mmsi)
            name = p1.get("vessel_name") or v_meta.get("vessel_name") or "Unknown"
            imo = v_meta.get("imo") or "N/A"

            # Check if ship is within valid data extent (Alpha channel > 0)
            # Sample alpha at pixel location
            try:
                # We need to sample from the source. To be efficient, we do a tiny read.
                # Since we are already inside 'with rio.open(tif_path) as src', we can use it.
                if src.count >= 4:
                    samp_win = Window(px, py, 1, 1)
                    alpha_samp = src.read(4, window=samp_win)
                    if alpha_samp[0, 0] == 0:
                        continue
            except:
                pass

            text = f"NAME: {name}\nMMSI: {mmsi}\nIMO: {imo}"
            bbox = dummy_draw.textbbox((0, 0), text, font=font)
            tw, th, padding = bbox[2] - bbox[0], bbox[3] - bbox[1], 10

            offsets = [(50, -th // 2), (50, 50), (0, 50), (-50 - tw, 50), (-50 - tw, -th // 2), (-50 - tw, -50 - th), (0, -50 - th), (50, -50 - th)]
            best_pos = (px + 50, py - th // 2)
            for ox, oy in offsets:
                tx, ty = px + ox, py + oy
                rect = [tx - padding, ty - padding, tx + tw + padding, ty + th + padding]
                if tx < 0 or ty < 0 or tx + tw > width or ty + th > height: continue
                if any(not (rect[2] < r[0] or rect[0] > r[2] or rect[3] < r[1] or rect[1] > r[3]) for r in placed_rects): continue
                best_pos = (tx, ty); placed_rects.append(rect); break
            else:
                best_pos = (px + 50, py - th // 2); placed_rects.append([best_pos[0]-padding, best_pos[1]-padding, best_pos[0]+tw+padding, best_pos[1]+th+padding])

            # Store annotation with Y-bounds for strip selection
            # Circle radius 15, padding 10
            min_y = min(py - 15, best_pos[1] - padding)
            max_y = max(py + 15, best_pos[1] + th + padding)
            
            ship_annotations.append({
                "px": px, "py": py, "text_pos": best_pos, "text": text,
                "tw": tw, "th": th, "padding": padding,
                "min_y": min_y, "max_y": max_y
            })

        if not ship_annotations:
            print("No vessels plotted on the image area.")
            return

        print(f"Plotting {len(ship_annotations)} vessels using strip-based rendering...")
        
        with rio.open(out_path, "w", **profile) as dst:
            strip_height = 2048
            for y_start in range(0, height, strip_height):
                y_end = min(y_start + strip_height, height)
                h = y_end - y_start
                win = Window(0, y_start, width, h)
                
                # Read strip
                if src.count >= 4:
                    strip_data = src.read([1, 2, 3, 4], window=win)
                else:
                    rgb = src.read([1, 2, 3], window=win)
                    alpha = np.ones((h, width), dtype=np.uint8) * 255
                    strip_data = np.concatenate([rgb, alpha[np.newaxis, ...]], axis=0)
                
                # Convert to PIL
                strip_img = Image.fromarray(np.transpose(strip_data, (1, 2, 0)), "RGBA")
                strip_draw = ImageDraw.Draw(strip_img, "RGBA")
                
                # Find ships that overlap this strip
                # A ship overlaps if its annotation bounding box overlaps [y_start, y_end]
                for ann in ship_annotations:
                    if ann["max_y"] < y_start or ann["min_y"] > y_end:
                        continue
                    
                    # Local coordinates
                    lpx, lpy = ann["px"], ann["py"] - y_start
                    ltx, lty = ann["text_pos"][0], ann["text_pos"][1] - y_start
                    tw, th, padding = ann["tw"], ann["th"], ann["padding"]
                    
                    # Circle
                    strip_draw.ellipse([lpx - 15, lpy - 15, lpx + 15, lpy + 15], outline=(255, 235, 59, 255), width=3)
                    # Line
                    strip_draw.line([lpx, lpy, ltx + (tw if lpx > ltx else 0), lty + th // 2], fill=(255, 235, 59, 180), width=2)
                    # Text box
                    strip_draw.rectangle([ltx - padding, lty - padding, ltx + tw + padding, lty + th + padding], fill=(0, 0, 0, 160), outline=(255, 235, 59, 200), width=1)
                    strip_draw.text((ltx, lty), ann["text"], font=font, fill=(255, 235, 59, 255))
                
                # Write strip
                dst.write(np.transpose(np.array(strip_img), (2, 0, 1)), window=win)
                print(f"  Processed strip {y_start} to {y_end}...", end="\r", flush=True)

    print(f"\nAIS Correlation complete: {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    target = sys.argv[1]
    if not os.path.exists(target):
        sys.exit(1)
    try:
        meta = get_metadata(target)
        data = fetch_ais_data(meta)
        if data:
            plot_on_image(target, data, meta["time"])
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
