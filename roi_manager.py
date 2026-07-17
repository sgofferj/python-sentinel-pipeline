#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# roi_manager.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
ROI Manager for creating cropped visual products based on Region of Interest definitions.
Scans pipeline outputs and extracts crops for specified ROIs.
Supports both pipeline integration (new files only) and standalone (all files) modes.
"""

import argparse
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional

import pillow_heif  # type: ignore
import requests  # type: ignore
from atproto import Client, client_utils  # type: ignore
from osgeo import gdal  # type: ignore
from PIL import Image
from shapely.geometry import box, shape  # type: ignore
from shapely.ops import unary_union  # type: ignore

import constants as c
import functions as func
import metadata_engine as meta
import inventory_manager
import notifications as notify

# Register HEIF opener for Pillow
pillow_heif.register_heif_opener()

gdal.UseExceptions()


def load_roi_config() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Loads ROI definitions and global config from roi_config.json."""
    config_path = os.path.join(c.BASE_DIR, "roi_config.json")
    if not os.path.exists(config_path):
        return {}, []
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Support both old (list only) and new (dict with 'config' and 'rois') formats
            if isinstance(data, list):
                return {}, data
            return data.get("config", {}), data.get("rois", [])
    except Exception as e:
        print(f"Error loading ROI config: {e}", flush=True)
        return {}, []


def calculate_coverage(
    roi_bbox_str: str, layer_list: List[Dict[str, Any]], roi_poly: Optional[Any] = None
) -> float:
    """Calculates the percentage of ROI covered by the union of multiple product footprints."""
    try:
        if roi_poly is None:
            west, south, east, north = map(float, roi_bbox_str.split(","))
            roi_poly = box(west, south, east, north)

        product_polys = []
        for layer in layer_list:
            footprint_raw = layer.get("footprint")
            if not footprint_raw:
                # Fallback to bounds if footprint is missing
                bounds = layer.get("bounds")  # [[S, W], [N, E]]
                if bounds:
                    product_polys.append(
                        box(bounds[0][1], bounds[0][0], bounds[1][1], bounds[1][0])
                    )
            else:
                product_polys.append(shape(footprint_raw))

        if not product_polys:
            return 0.0

        combined_product_poly = unary_union(product_polys)

        if not roi_poly.intersects(combined_product_poly):
            return 0.0

        intersection_area = roi_poly.intersection(combined_product_poly).area
        roi_area = roi_poly.area

        if roi_area == 0:
            return 0.0

        return (intersection_area / roi_area) * 100
    except Exception as e:
        print(f"Error calculating coverage: {e}", flush=True)
        return 0.0


def crop_product(src_paths: List[str], dst_path: str, bbox_str: str) -> bool:
    """Crops one or more TIFF files to the specified WGS84 bounding box (mosaicing if needed)."""
    try:
        west, south, east, north = map(float, bbox_str.split(","))

        # Check source bands to avoid 5-band TIFFs (GDAL adds alpha to existing alpha)
        # We only take the RGB bands if it's a 4-band product, or 1 band if it's 2-band.
        # This ensures dstAlpha produces a clean, standard 4-band or 2-band output.
        src_bands = None
        ds = gdal.Open(src_paths[0])
        if ds:
            if ds.RasterCount == 4:
                src_bands = [1, 2, 3]
            elif ds.RasterCount == 2:
                src_bands = [1]
            ds = None

        # Use gdal.Warp for cropping and mosaicing
        warp_options = gdal.WarpOptions(
            format="GTiff",
            outputBounds=[west, south, east, north],
            outputBoundsSRS="EPSG:4326",
            srcBands=src_bands,
            creationOptions=["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=YES"],
            dstAlpha=True,
        )
        gdal.Warp(dst_path, src_paths, options=warp_options)
        return True
    except Exception as e:
        print(f"Error cropping {src_paths} to ROI: {e}", flush=True)
        return False


def create_social_image(src_path: str, base_dst_path: str) -> str:
    """
    Creates a non-georeferenced HEIC image from a TIFF for Bluesky.
    Iteratively scales and compresses to stay under Bluesky's 2MB limit.
    Returns the path to the created image.
    """
    try:
        with Image.open(src_path) as img:
            # Convert to RGB (remove alpha/extras if any)
            if img.mode != "RGB":
                rgb_img = img.convert("RGB")
            else:
                rgb_img = img

            max_dim = 4000
            quality = 80
            dst_path = base_dst_path + ".heic"

            # Iterative downscaling and compression
            while True:
                # Current attempt with current max_dim
                if rgb_img.width > max_dim or rgb_img.height > max_dim:
                    temp_img = rgb_img.copy()
                    temp_img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
                else:
                    temp_img = rgb_img

                # Save as HEIF (HEIC)
                temp_img.save(dst_path, "HEIF", quality=quality)
                size = os.path.getsize(dst_path)

                if size <= 2000000:
                    return dst_path

                # If still too big, try lower quality first
                if quality > 40:
                    quality -= 10
                else:
                    # Then try smaller dimensions
                    max_dim = int(max_dim * 0.8)
                    quality = 70  # Reset quality for smaller dim
                    if max_dim < 1000:
                        break

            return dst_path
    except Exception as e:
        print(f"Error creating social image (HEIC): {e}", flush=True)
        return ""


def create_full_image(src_path: str, base_dst_path: str) -> str:
    """
    Creates a full-size non-georeferenced JPEG from a TIFF.
    Returns the path to the created image.
    """
    try:
        with Image.open(src_path) as img:
            if img.mode != "RGB":
                rgb_img = img.convert("RGB")
            else:
                rgb_img = img

            dst_path = base_dst_path + "_full.jpg"
            # High quality JPEG, no downscaling
            rgb_img.save(dst_path, "JPEG", quality=90, optimize=True)
            return dst_path
    except Exception as e:
        print(f"Error creating full image: {e}", flush=True)
        return ""


PRODUCT_NAMES = {
    "TCI": "True Color",
    "TCI-GF": "True Color (Guided Filter)",
    "RATIO": "Radar Ratio (VV/VH)",
    "VV": "Radar VV",
    "VH": "Radar VH",
    "NIRFC": "False Color (NIR)",
    "NIRFC-GF": "False Color (Guided Filter)",
    "NDVI": "NDVI (Vegetation Index)",
    "NDRE": "NDRE (Plant Stress)",
    "NDBI": "NDBI (Urban/Built-up)",
    "NBR": "NBR (Burn Index)",
    "CAMO": "Camo Detection",
    "AP": "Atmospheric Penetration (SWIR)",
    "LIFE-MACHINE": "Life/Machine Separation",
    "RADAR-BURN": "Radar Burn Detection",
}


def get_human_name(product_type: str) -> str:
    """Converts a technical product type to a human-readable name."""
    # Strip sensor prefix (e.g. "S2-TCI-GF" → "TCI-GF")
    rest = product_type.split("-", 1)[1] if "-" in product_type else product_type
    parts = rest.split("-")
    # Try progressively shorter suffixes: "TCI-GF", then "GF"
    for i in range(len(parts)):
        candidate = "-".join(parts[i:]).upper()
        if candidate in PRODUCT_NAMES:
            return PRODUCT_NAMES[candidate]
    return rest


BASEMAP_CACHE: str = os.path.join(c.BASE_DIR, "temp", "basemap_cache")
BASEMAP_TILE_URL: str = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile"
)


def _mercator(lon: float, lat: float) -> Tuple[float, float]:
    """Convert lon/lat to EPSG:3857 meters."""
    x = lon * 20037508.34 / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * 20037508.34 / 180.0
    return x, y


def _lonlat_to_tile(lon: float, lat: float, zoom: int) -> Tuple[int, int]:
    """Get XYZ tile (x,y) for given lon/lat at zoom level."""
    lat_rad = math.radians(lat)
    n = 2.0**zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def _tile_epsg3857_extent(
    x: int, y: int, zoom: int
) -> Tuple[float, float, float, float]:
    """Return (x_min, y_min, x_max, y_max) in EPSG:3857 for tile (x,y) at zoom."""
    n = 2.0**zoom
    tile_m = 40075016.68 / n
    x_min = -20037508.34 + x * tile_m
    x_max = -20037508.34 + (x + 1) * tile_m
    y_max = 20037508.34 - y * tile_m
    y_min = 20037508.34 - (y + 1) * tile_m
    return x_min, y_min, x_max, y_max


def fetch_roi_basemap(roi_bbox_str: str, roi_name: str) -> Optional[str]:
    """
    Fetch and cache an ESRI satellite basemap covering the ROI.
    Returns the path to the cached PNG, or None on failure.
    """
    cache_dir = os.path.join(BASEMAP_CACHE, roi_name)
    cache_path = os.path.join(cache_dir, "basemap.png")
    if os.path.exists(cache_path):
        return cache_path

    west, south, east, north = map(float, roi_bbox_str.split(","))
    roi_w, roi_s = _mercator(west, south)
    roi_e, roi_n = _mercator(east, north)

    # Pick zoom so the ROI fits in at most ~6 tiles
    zoom = 12
    for z in range(14, 8, -1):
        x0, y0 = _lonlat_to_tile(west, north, z)
        x1, y1 = _lonlat_to_tile(east, south, z)
        if (x1 - x0 + 1) * (y1 - y0 + 1) <= 6:
            zoom = z
            break

    x0, y0 = _lonlat_to_tile(west, north, zoom)
    x1, y1 = _lonlat_to_tile(east, south, zoom)

    tile_m = 40075016.68 / (2.0**zoom)
    tile_px = 256

    cols = x1 - x0 + 1
    rows = y1 - y0 + 1
    mosaic = Image.new("RGB", (cols * tile_px, rows * tile_px))

    session = requests.Session()
    for dx in range(cols):
        for dy in range(rows):
            tx = x0 + dx
            ty = y0 + dy
            url = f"{BASEMAP_TILE_URL}/{zoom}/{ty}/{tx}.png"
            try:
                resp = session.get(url, timeout=10)
                if resp.status_code == 200:
                    tile_img = Image.open(BytesIO(resp.content)).convert("RGB")
                    mosaic.paste(tile_img, (dx * tile_px, dy * tile_px))
            except Exception:
                continue

    # Crop mosaic to exact ROI extent in EPSG:3857 pixel coordinates
    origin_x = -20037508.34 + x0 * tile_m
    origin_y = 20037508.34 - y0 * tile_m

    px = int((roi_w - origin_x) / tile_m * tile_px)
    py = int((origin_y - roi_n) / tile_m * tile_px)
    pw = int((roi_e - roi_w) / tile_m * tile_px)
    ph = int((roi_n - roi_s) / tile_m * tile_px)

    if pw > 0 and ph > 0:
        mosaic = mosaic.crop((px, py, px + pw, py + ph))

    os.makedirs(cache_dir, exist_ok=True)
    mosaic.save(cache_path)
    return cache_path


def _composite_thermal(basemap_path: str, thermal_path: str, output_path: str) -> str:
    """Overlay RGBA thermal crop on basemap with attribution text."""
    basemap = Image.open(basemap_path).convert("RGBA")
    thermal = Image.open(thermal_path).convert("RGBA")
    basemap = basemap.resize(thermal.size, Image.LANCZOS)
    comp = Image.alpha_composite(basemap, thermal)
    comp = comp.convert("RGB")

    w, h = comp.size
    bar_h = max(int(h * 0.025), 14)
    overlay = Image.new("RGBA", (w, bar_h), (0, 0, 0, 140))
    comp.paste(overlay, (0, h - bar_h), overlay)

    from PIL import ImageDraw

    draw = ImageDraw.Draw(comp)
    text = "Basemap (C) ESRI, made with material from Copernicus Sentinel"
    font_size = max(int(bar_h * 0.55), 8)
    try:
        from PIL import ImageFont

        font = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", font_size
        )
    except Exception:
        font = ImageFont.load_default()
    _, _, tw, th = draw.textbbox((0, 0), text, font=font)
    tx = (w - tw) // 2
    ty = h - bar_h + (bar_h - th) // 2
    draw.text((tx, ty), text, fill=(200, 200, 200), font=font)

    comp.save(output_path, "PNG")
    return output_path


def check_thermal_anomaly(
    ana_src_paths: List[str],
    roi_bbox_str: str,
    roi_name: str,
    base_acq_time: str,
    threshold_kelvin: float,
    apprise_url: str,
) -> bool:
    """Crop analtic S3 BT to ROI, check max BT against threshold, send Apprise alert."""
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tif")
    os.close(tmp_fd)
    try:
        if not crop_product(ana_src_paths, tmp_path, roi_bbox_str):
            return False
        ds = gdal.Open(tmp_path)
        if not ds:
            return False
        band = ds.GetRasterBand(1)
        stats = band.GetStatistics(True, True)
        ds = None
        max_temp = stats[1]
        if max_temp > threshold_kelvin:
            msg = (
                f"Thermal anomaly detected at {roi_name}\n"
                f"Max temperature: {max_temp:.1f}K ({max_temp - 273.15:.1f}C)\n"
                f"Threshold: {threshold_kelvin:.1f}K\n"
                f"Acquired: {base_acq_time}"
            )
            notify.send_notification(
                message=msg,
                title=f"Thermal Alert: {roi_name}",
                urls=apprise_url,
            )
            return True
        return False
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def post_to_bsky(
    config: Dict[str, Any],
    roi_name: str,
    product_type: str,
    acq_time: str,
    constellation: str,
    image_path: str,
) -> bool:
    """Posts the ROI image to Bluesky with functional hashtags."""
    username = config.get("roi_bsky_username")
    password = config.get("roi_bsky_pw")

    if not username or not password or "your-" in username:
        print("Bluesky credentials not configured correctly.", flush=True)
        return False

    try:
        client = Client()
        client.login(username, password)

        human_prod = get_human_name(product_type)

        # Build rich text with functional hashtags
        tb = client_utils.TextBuilder()
        tb.text(f"Updated {human_prod} image of {roi_name}\n")
        tb.text(f"Satellite: {constellation}\n")
        tb.text(f"Acquisition time: {acq_time}\n")
        tb.text("Made with material from Copernicus Sentinel\n\n")

        # Hashtag sequence: #OSINT, #satelliteimagery, #Sentinel, #<constellation>,
        # Copernicus, #<ROI-name>
        tb.tag("#OSINT", "OSINT")
        tb.text(" ")
        tb.tag("#satelliteimagery", "satelliteimagery")
        tb.text(" ")
        tb.tag("#Sentinel", "Sentinel")
        tb.text(" ")

        # constellation: remove spaces and hyphens
        const_tag = constellation.replace(" ", "").replace("-", "")
        tb.tag(f"#{const_tag}", const_tag)
        tb.text(" ")

        tb.tag("#Copernicus", "Copernicus")
        tb.text(" ")

        # ROI name: replace spaces with _
        roi_tag = roi_name.replace(" ", "_")
        tb.tag(f"#{roi_tag}", roi_tag)

        with open(image_path, "rb") as f:
            img_data = f.read()
            upload = client.upload_blob(img_data)

        from atproto_client.models.app.bsky.embed.images import (  # type: ignore
            Main,
            Image as BskyImage,
        )

        embed = Main(
            images=[
                BskyImage(
                    alt=f"{human_prod} image of {roi_name} ({acq_time})",
                    image=upload.blob,
                )
            ]
        )

        client.send_post(text=tb, embed=embed)
        print(f"Successfully posted {roi_name} to Bluesky.", flush=True)
        return True
    except Exception as e:
        print(f"Error posting to Bluesky: {e}", flush=True)
        return False


def run_roi_stage(process_all: bool = False) -> int:
    """
    Main entry point for the ROI stage.
    Groups inventory by acquisition time and product type to handle multi-tile orbits.
    Scans the inventory and processes ROI crops and social posts.
    """
    func.perf_logger.start_step("ROI Cropping Stage")

    config, rois = load_roi_config()
    if not rois:
        print("No ROI configurations found. Skipping.", flush=True)
        func.perf_logger.end_step()
        return 0

    inventory_path = os.path.join(c.DIRS["OUT"], "visual/inventory.json")
    if not os.path.exists(inventory_path):
        print("Inventory not found. Skipping ROI stage.", flush=True)
        func.perf_logger.end_step()
        return 0

    try:
        with open(inventory_path, "r", encoding="utf-8") as f:
            inventory = json.load(f)
    except Exception as e:
        print(f"Error reading inventory: {e}", flush=True)
        func.perf_logger.end_step()
        return 0

    all_layers = inventory.get("layers", [])
    if not all_layers:
        print("No products found in inventory.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Group all layers by (Date, Orbit, Direction, Product)
    # Filter out ROI crops themselves to avoid recursion
    grouped_layers: Dict[
        Tuple[str, Optional[str], Optional[str], str], List[Dict[str, Any]]
    ] = {}
    for layer in all_layers:
        p_type = layer.get("product", "")
        if p_type.startswith("ROI-"):
            continue

        acq_time = layer.get("acquisition_time", "Unknown")
        # Extract date part for broader grouping if orbit info is available
        # ISO: 2026-05-24T10:00:00Z -> 2026-05-24
        date_part = acq_time[:10] if len(acq_time) >= 10 else acq_time

        rel_orbit = layer.get("relative_orbit")
        orbit_dir = layer.get("orbit_direction")

        # If we don't have orbit info, we fallback to acq_time (original logic)
        # but if we have it, we use the date + orbit + dir which is rock solid.
        key = (date_part if rel_orbit else acq_time, rel_orbit, orbit_dir, p_type)
        if key not in grouped_layers:
            grouped_layers[key] = []
        grouped_layers[key].append(layer)

    # Identify groups to process
    groups_to_process = []
    run_start_dt = datetime.fromtimestamp(func.perf_logger.start_time, tz=timezone.utc)

    for key, layers in grouped_layers.items():
        if process_all:
            groups_to_process.append((key, layers))
        else:
            is_dirty = False
            for layer in layers:
                render_time_str = layer.get("render_time", "")
                if not render_time_str:
                    continue
                try:
                    render_dt = datetime.fromisoformat(
                        render_time_str.replace("Z", "+00:00")
                    )
                    if render_dt >= run_start_dt:
                        is_dirty = True
                        break
                except Exception:
                    continue
            if is_dirty:
                groups_to_process.append((key, layers))

    if not groups_to_process:
        print("No new products found for ROI processing.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Sort groups by acquisition time (oldest first) to ensure chronological processing
    # (i.e. oldest events are processed and posted first)
    groups_to_process.sort(
        key=lambda x: min(l.get("acquisition_time", "9999") for l in x[1])
    )

    print(
        f"Checking {len(groups_to_process)} product groups against {len(rois)} ROIs.",
        flush=True,
    )

    bsky_post_enabled = config.get("roi_bsky_post", False)
    bsky_roi_names = config.get("roi_bsky_names", [])

    crops_created = 0
    for (date_part, rel_orbit, orbit_dir, product_type), layers in groups_to_process:
        constellation = "Sentinel-2" if product_type.startswith("S2") else "Sentinel-1"

        # Use the earliest acquisition time in the group for the filename
        # to keep it consistent.
        base_acq_time = min(l.get("acquisition_time", "Unknown") for l in layers)
        iso_clean = base_acq_time.replace(":", "")

        # Aggregate metadata and paths for the group
        src_paths = [
            os.path.join(c.DIRS["OUT"], l["path"])
            for l in layers
            if os.path.exists(os.path.join(c.DIRS["OUT"], l["path"]))
        ]
        if not src_paths:
            continue

        # Cloud cover (average)
        cc_vals = [l["cloud_cover"] for l in layers if l.get("cloud_cover") is not None]
        avg_cloud_cover = sum(cc_vals) / len(cc_vals) if cc_vals else None

        # Resolution (max)
        resolution = max(l.get("resolution", 0) for l in layers)

        for roi in rois:
            roi_name_raw = roi.get("name", "ROI")
            # Resolve env vars but DON'T force lowercase
            roi_name = func.resolve_env_variable(roi_name_raw).replace("_", " ")

            roi_bbox = func.resolve_env_variable(roi.get("bbox", ""))
            roi_match_threshold = roi.get("bbox_match", 0)
            desired_products = roi.get("products", [])

            # Check if this product is desired by the ROI
            match_found = False
            for dp in desired_products:
                dp_up = dp.upper()
                pt_up = product_type.upper()
                # Direct match (e.g., "TCI" in "S2-TCI")
                if dp_up in pt_up:
                    match_found = True
                    break
                # Flexible match for RATIO variants (e.g., config RATIOVVVH vs inventory RATIO)
                dp_norm = dp_up.replace("RATIOVVVH", "RATIO")
                pt_norm = pt_up.replace("RATIOVVVH", "RATIO")
                if dp_norm in pt_norm:
                    match_found = True
                    break
                # Normalize hyphens/underscores for GF variants (e.g., "TCI-GF" vs "S2-TCI_GF")
                dp_norm2 = dp_up.replace("-", "_")
                pt_norm2 = pt_up.replace("-", "_")
                if dp_norm2 in pt_norm2:
                    match_found = True
                    break

            if not match_found:
                continue

            # Optimization: Check if any single tile in the group satisfies the ROI
            west, south, east, north = map(float, roi_bbox.split(","))
            roi_poly = box(west, south, east, north)

            best_src_paths = None
            effective_coverage = 0.0

            # Find best single tile coverage first
            max_single_coverage = 0.0
            best_single_layer = None
            for layer in layers:
                cov = calculate_coverage(roi_bbox, [layer], roi_poly=roi_poly)
                if cov > max_single_coverage:
                    max_single_coverage = cov
                    best_single_layer = layer

            if max_single_coverage >= roi_match_threshold and best_single_layer:
                best_src_paths = [
                    os.path.join(c.DIRS["OUT"], best_single_layer["path"])
                ]
                effective_coverage = max_single_coverage
            else:
                # If no single tile fits, check combined coverage
                combined_coverage = calculate_coverage(
                    roi_bbox, layers, roi_poly=roi_poly
                )
                if combined_coverage >= roi_match_threshold:
                    best_src_paths = src_paths
                    effective_coverage = combined_coverage

            if best_src_paths:
                # Use full product type (excluding sensor prefix)
                p_suffix = (
                    product_type.split("-", 1)[1]
                    if "-" in product_type
                    else product_type
                )

                dst_filename = f"{roi_name}_{p_suffix}_{iso_clean}.tif"
                dst_path = os.path.join(c.DIRS["VIS_ROI"], dst_filename)

                print(
                    f"ROI Match: {roi_name} ({effective_coverage:.1f}%) -> "
                    f"Cropping {dst_filename} ({len(best_src_paths)} tiles)",
                    flush=True,
                )
                if not crop_product(best_src_paths, dst_path, roi_bbox):
                    continue

                # Generate sidecar for the new crop
                sat_val = layers[0].get("satellite")
                meta.generate_sidecar(
                    dst_path,
                    f"ROI-{roi_name}-{p_suffix}",
                    product_type,
                    effective_res=resolution,
                    cloud_cover=avg_cloud_cover,
                    relative_orbit=rel_orbit,
                    orbit_direction=orbit_dir,
                    satellite=sat_val,
                )
                crops_created += 1

                # Thermal monitoring: check S3-BT analytic data for hot spots
                thermal_alert = False
                thermal_checked = False
                if product_type.startswith("S3-"):
                    thermal_enabled = roi.get("thermal_monitor", False)
                    if thermal_enabled:
                        thermal_checked = True
                        threshold = roi.get("thermal_threshold", 310.0)
                        ana_src = [
                            p.replace("/visual/", "/analytic/") for p in best_src_paths
                        ]
                        ana_src = [p for p in ana_src if os.path.exists(p)]
                        if ana_src:
                            thermal_alert = check_thermal_anomaly(
                                ana_src,
                                roi_bbox,
                                roi_name,
                                base_acq_time,
                                threshold,
                                roi.get("apprise_url", ""),
                            )

                # Delete all-transparent FIRE crops when no thermal anomaly
                if "FIRE" in product_type and thermal_checked and not thermal_alert:
                    os.remove(dst_path)
                    json_path = dst_path.replace(".tif", ".json")
                    if os.path.exists(json_path):
                        os.remove(json_path)
                    crops_created -= 1

                # Apprise and Bluesky posting
                apprise_url = roi.get("apprise_url", "")
                social_needed = bsky_post_enabled and roi_name_raw in bsky_roi_names

                # Skip image posts for S3 thermal products unless anomaly detected
                if product_type.startswith("S3-") and not thermal_alert:
                    apprise_url = ""
                    social_needed = False

                # Composite thermal crop on basemap for anomaly posts
                post_path = dst_path
                if thermal_alert and product_type.startswith("S3-"):
                    basemap_path = fetch_roi_basemap(roi_bbox, roi_name)
                    if basemap_path:
                        comp_path = dst_path.replace(".tif", "_comp.png")
                        post_path = _composite_thermal(
                            basemap_path, dst_path, comp_path
                        )

                social_base = os.path.join(
                    c.DIRS["VIS_ROI"],
                    f"{roi_name}_{p_suffix}_{iso_clean}_social",
                )

                # Apprise gets full size JPEG
                if apprise_url:
                    full_image = create_full_image(post_path, social_base)
                    if full_image:
                        human_prod = get_human_name(product_type)
                        msg = (
                            f"New {human_prod} image for ROI {roi_name}\n"
                            f"Acquired: {base_acq_time}\n"
                            f"Satellite: {constellation}"
                        )
                        notify.send_notification(
                            message=msg,
                            title=f"ROI Update: {roi_name}",
                            urls=apprise_url,
                            attachment=full_image,
                        )

                # Bluesky gets downscaled social image
                if social_needed:
                    image_path = create_social_image(post_path, social_base)
                    if image_path:
                        post_to_bsky(
                            config,
                            roi_name,
                            product_type,
                            base_acq_time,
                            constellation,
                            image_path,
                        )

    print(f"ROI stage complete. {crops_created} crops created/updated.", flush=True)
    if crops_created > 0:
        inventory_manager.rebuild_inventory()

    func.perf_logger.end_step()
    return crops_created


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone ROI Manager")
    parser.add_argument(
        "--all", action="store_true", help="Process all files in the inventory"
    )
    args = parser.parse_args()

    # Start performance logger for standalone run
    func.perf_logger.start_run()
    run_roi_stage(process_all=args.all)
    func.perf_logger.stop_run()
