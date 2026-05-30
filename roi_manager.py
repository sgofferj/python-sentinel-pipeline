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
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import pillow_heif  # type: ignore
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


def calculate_coverage(roi_bbox_str: str, layer_list: List[Dict[str, Any]]) -> float:
    """Calculates the percentage of ROI covered by the union of multiple product footprints."""
    try:
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
        # Use gdal.Warp for cropping and mosaicing
        warp_options = gdal.WarpOptions(
            format="GTiff",
            outputBounds=[west, south, east, north],
            outputBoundsSRS="EPSG:4326",
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
    "RATIO": "Radar Ratio (VV/VH)",
    "VV": "Radar VV",
    "VH": "Radar VH",
    "NIRFC": "False Color (NIR)",
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
    suffix = product_type.split("-")[-1].upper()
    return PRODUCT_NAMES.get(suffix, suffix)


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

            if not match_found:
                continue

            coverage = calculate_coverage(roi_bbox, layers)
            if coverage >= roi_match_threshold:
                # Use full product type (excluding sensor prefix)
                p_suffix = (
                    product_type.split("-", 1)[1]
                    if "-" in product_type
                    else product_type
                )

                dst_filename = f"{roi_name}_{p_suffix}_{iso_clean}.tif"
                dst_path = os.path.join(c.DIRS["VIS_ROI"], dst_filename)

                print(
                    f"ROI Match: {roi_name} ({coverage:.1f}%) -> "
                    f"Cropping {dst_filename} ({len(src_paths)} tiles)",
                    flush=True,
                )
                if not crop_product(src_paths, dst_path, roi_bbox):
                    continue

                # Generate sidecar for the new crop
                meta.generate_sidecar(
                    dst_path,
                    f"ROI-{roi_name}-{p_suffix}",
                    product_type,
                    effective_res=resolution,
                    cloud_cover=avg_cloud_cover,
                )
                crops_created += 1

                # Apprise and Bluesky posting
                apprise_url = roi.get("apprise_url", "")
                social_needed = bsky_post_enabled and roi_name_raw in bsky_roi_names

                social_base = os.path.join(
                    c.DIRS["VIS_ROI"],
                    f"{roi_name}_{p_suffix}_{iso_clean}_social",
                )

                # Apprise gets full size JPEG
                if apprise_url:
                    full_image = create_full_image(dst_path, social_base)
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
                    image_path = create_social_image(dst_path, social_base)
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
