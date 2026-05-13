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
Interpolates position at exact acquisition time from a 20m window.
"""

import os
import sys
import json
import datetime
import math
from typing import Dict, List, Any, Tuple, Optional

import requests
import rasterio as rio
from rasterio.warp import transform_bounds
from PIL import Image, ImageDraw, ImageFont
import numpy as np
from pyproj import Transformer
from shapely.geometry import Point

# --- CONFIGURATION ---
AIS_RECORDER_URL = os.getenv("AIS_RECORDER_URL")
AIS_MAX_TIME_MINUTES = int(os.getenv("AIS_MAX_TIME_MINUTES", "30"))

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
        with open(sidecar_path, "r") as f:
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
    with rio.open(tif_path) as src:
        bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        filename, is_s1 = os.path.basename(tif_path), "S1" in os.path.basename(tif_path)
        time_str = "2026-04-07T12:00:00Z"
        import re

        m = re.search(r"S1_(\d{8}T\d{6})" if is_s1 else r"-(\d{8}T\d{6}Z)", filename)
        if m:
            t = m.group(1)
            time_str = f"{t[:4]}-{t[4:6]}-{t[6:8]}T{t[9:11]}:{t[11:13]}:{t[13:15]}Z"
        return {"time": time_str, "bounds": bounds, "is_s1": is_s1}


def fetch_vessel_metadata(mmsi: int) -> Dict[str, Any]:
    if not AIS_RECORDER_URL:
        raise ValueError("AIS_RECORDER_URL not defined in environment.")
    try:
        resp = requests.get(f"{AIS_RECORDER_URL}/vessels?mmsi={mmsi}", timeout=10)
        if resp.ok:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
    except:
        pass
    return {}


def fetch_ais_data(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not AIS_RECORDER_URL:
        raise ValueError("AIS_RECORDER_URL not defined in environment.")
    base_dt = datetime.datetime.fromisoformat(meta["time"].replace("Z", "+00:00"))
    delta = datetime.timedelta(minutes=AIS_MAX_TIME_MINUTES)
    t_from = (base_dt - delta).isoformat()
    t_to = (base_dt + delta).isoformat()
    b = meta["bounds"]
    c_lon, c_lat = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
    radius_km = haversine(c_lat, c_lon, b[3], b[2]) + 2.0

    params = {
        "start_time": t_from,
        "end_time": t_to,
        "bbox": f"{b[0]},{b[1]},{b[2]},{b[3]}",
    }
    try:
        resp = requests.get(f"{AIS_RECORDER_URL}/positions", params=params, timeout=30)
        print(f"[DEBUG] Query URL: {resp.url}")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"AIS Query failed: {e}")
        return []


def plot_on_image(
    tif_path: str, ais_features: List[Dict[str, Any]], target_time_iso: str
):
    """Interpolates positions and plots MINIMALIST circles on image."""
    out_path = tif_path.replace(".tif", "_AIS.tif")
    target_ts = datetime.datetime.fromisoformat(
        target_time_iso.replace("Z", "+00:00")
    ).timestamp()

    with rio.open(tif_path) as src:
        # Read RGBA if available
        if src.count >= 4:
            img_data = src.read([1, 2, 3, 4])
        else:
            rgb = src.read([1, 2, 3])
            alpha = np.ones((src.height, src.width), dtype=np.uint8) * 255
            img_data = np.concatenate([rgb, alpha[np.newaxis, ...]], axis=0)

        if img_data.dtype != np.uint8:
            img_data = (img_data / np.max(img_data) * 255).astype(np.uint8)

        img = Image.fromarray(np.transpose(img_data, (1, 2, 0)), "RGBA")
        draw = ImageDraw.Draw(img, "RGBA")
        trans = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)

        tracks: Dict[int, List[Dict[str, Any]]] = {}
        for item in ais_features:
            mmsi = item["mmsi"]
            if mmsi not in tracks:
                tracks[mmsi] = []
            tracks[mmsi].append(item)

        placed_rects: List[List[float]] = []
        print(f"Analyzing {len(tracks)} vessel tracks for interpolation...")
        for mmsi, pings in tracks.items():
            pings.sort(key=lambda x: str(x.get("timestamp", "")))

            p1, p2 = None, None
            for i in range(len(pings) - 1):
                t1_str = pings[i].get("timestamp")
                t2_str = pings[i + 1].get("timestamp")
                if t1_str and t2_str:
                    t1 = datetime.datetime.fromisoformat(
                        t1_str.replace("Z", "+00:00")
                    ).timestamp()
                    t2 = datetime.datetime.fromisoformat(
                        t2_str.replace("Z", "+00:00")
                    ).timestamp()
                    if t1 <= target_ts <= t2:
                        p1, p2 = pings[i], pings[i + 1]
                        break

            if not p1:
                latest_t_str = pings[-1].get("timestamp")
                if latest_t_str:
                    latest_t = datetime.datetime.fromisoformat(
                        latest_t_str.replace("Z", "+00:00")
                    ).timestamp()
                    if abs(latest_t - target_ts) < 7200:
                        p1 = pings[-1]

            if not p1:
                continue

            if p1 and p2:
                p1_ts_str = p1.get("timestamp")
                p2_ts_str = p2.get("timestamp")
                if p1_ts_str and p2_ts_str:
                    t1 = datetime.datetime.fromisoformat(
                        p1_ts_str.replace("Z", "+00:00")
                    ).timestamp()
                    t2 = datetime.datetime.fromisoformat(
                        p2_ts_str.replace("Z", "+00:00")
                    ).timestamp()
                    ratio = (target_ts - t1) / (t2 - t1)
                    lon = float(p1["longitude"]) + ratio * (
                        float(p2["longitude"]) - float(p1["longitude"])
                    )
                    lat = float(p1["latitude"]) + ratio * (
                        float(p2["latitude"]) - float(p1["latitude"])
                    )
                else:
                    lon, lat = float(p1["longitude"]), float(p1["latitude"])
            else:
                lon, lat = float(p1["longitude"]), float(p1["latitude"])

            name = p1.get("vessel_name") or "Unknown"
            imo = "N/A"
            callsign = "N/A"

            mx, my = trans.transform(lon, lat)
            py, px = src.index(mx, my)
            if not (0 <= px < src.width and 0 <= py < src.height):
                continue

            try:
                v_meta = fetch_vessel_metadata(mmsi)
                if v_meta:
                    imo = v_meta.get("imo") or "N/A"
                    callsign = v_meta.get("callSign") or "N/A"
            except:
                pass

            # --- DRAW CIRCLE ONLY ---
            radius = 15
            draw.ellipse(
                [px - radius, py - radius, px + radius, py + radius],
                outline=(255, 235, 59, 255),
                width=3,
            )

            # --- DATA BLOCK ---
            text = f"NAME: {name}\nMMSI: {mmsi}\nIMO: {imo}\nCALL: {callsign}"
            font: Any
            try:
                font = ImageFont.truetype(
                    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 20
                )
            except:
                font = ImageFont.load_default()
            bbox = draw.textbbox((0, 0), text, font=font)
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
                if tx < 0 or ty < 0 or tx + tw > src.width or ty + th > src.height:
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

            draw.line(
                [
                    px,
                    py,
                    best_pos[0] + (tw if px > best_pos[0] else 0),
                    best_pos[1] + th // 2,
                ],
                fill=(255, 235, 59, 180),
                width=2,
            )
            draw.rectangle(
                [
                    best_pos[0] - padding,
                    best_pos[1] - padding,
                    best_pos[0] + tw + padding,
                    best_pos[1] + th + padding,
                ],
                fill=(0, 0, 0, 160),
                outline=(255, 235, 59, 200),
                width=1,
            )
            draw.text(best_pos, text, font=font, fill=(255, 235, 59, 255))

        final_data = np.transpose(np.array(img), (2, 0, 1))
        profile = src.profile.copy()
        profile.update(count=4, dtype=rio.uint8, compress="deflate", tiled=True)
        with rio.open(out_path, "w", **profile) as dst:
            dst.write(final_data)
    print(f"AIS Correlation complete: {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ais_correlator.py <path_to_satellite_tif>")
        sys.exit(1)
    target_tif = sys.argv[1]
    if not os.path.exists(target_tif):
        print(f"Error: File not found {target_tif}")
        sys.exit(1)
    try:
        metadata = get_metadata(target_tif)
        ais_data = fetch_ais_data(metadata)
        if not ais_data:
            print("No AIS data found for this time/area.")
        else:
            print(f"Found {len(ais_data)} raw reports. Plotting minimalist overlays...")
            plot_on_image(target_tif, ais_data, metadata["time"])
    except Exception as e:
        print(f"Error during AIS correlation: {e}")
        sys.exit(1)
