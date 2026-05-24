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
from typing import Any, Dict, List, Tuple

import pillow_heif  # type: ignore
from atproto import Client  # type: ignore
from osgeo import gdal  # type: ignore
from PIL import Image
from shapely.geometry import box, shape  # type: ignore

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


def calculate_coverage(roi_bbox_str: str, sidecar_data: Dict[str, Any]) -> float:
    """Calculates the percentage of ROI covered by the product footprint."""
    try:
        west, south, east, north = map(float, roi_bbox_str.split(","))
        roi_poly = box(west, south, east, north)

        footprint_raw = sidecar_data.get("footprint")
        if not footprint_raw:
            # Fallback to bounds if footprint is missing
            bounds = sidecar_data.get("bounds")  # [[S, W], [N, E]]
            if bounds:
                product_poly = box(
                    bounds[0][1], bounds[0][0], bounds[1][1], bounds[1][0]
                )
            else:
                return 0.0
        else:
            product_poly = shape(footprint_raw)

        if not roi_poly.intersects(product_poly):
            return 0.0

        intersection_area = roi_poly.intersection(product_poly).area
        roi_area = roi_poly.area

        if roi_area == 0:
            return 0.0

        return (intersection_area / roi_area) * 100
    except Exception as e:
        print(f"Error calculating coverage: {e}", flush=True)
        return 0.0


def crop_product(src_path: str, dst_path: str, bbox_str: str) -> bool:
    """Crops a TIFF file to the specified WGS84 bounding box."""
    try:
        west, south, east, north = map(float, bbox_str.split(","))
        # Use gdal.Warp for cropping
        warp_options = gdal.WarpOptions(
            format="GTiff",
            outputBounds=[west, south, east, north],
            outputBoundsSRS="EPSG:4326",
            creationOptions=["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=YES"],
        )
        gdal.Warp(dst_path, src_path, options=warp_options)
        return True
    except Exception as e:
        print(f"Error cropping {src_path} to ROI: {e}", flush=True)
        return False


def create_social_image(src_path: str, base_dst_path: str) -> str:
    """
    Creates a non-georeferenced JPEG (or HEIC if >2MB) from a TIFF.
    Downscales to 4000px if needed.
    Returns the path to the created image.
    """
    try:
        # Load image with Pillow
        with Image.open(src_path) as img:
            # Downscale if needed
            max_size = 4000
            if img.width > max_size or img.height > max_size:
                img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

            # Convert to RGB (remove alpha/extras if any)
            if img.mode != "RGB":
                rgb_img = img.convert("RGB")
            else:
                rgb_img = img

            # Try JPEG first
            jpg_path = base_dst_path + ".jpg"
            rgb_img.save(jpg_path, "JPEG", quality=85, optimize=True)

            # Check size
            if os.path.getsize(jpg_path) <= 2 * 1024 * 1024:
                return jpg_path

            # If > 2MB, try HEIC
            heic_path = base_dst_path + ".heic"
            rgb_img.save(heic_path, "HEIF", quality=70)
            return heic_path
    except Exception as e:
        print(f"Error creating social image: {e}", flush=True)
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


def post_to_bsky(
    config: Dict[str, Any],
    roi_name: str,
    product_type: str,
    acq_time: str,
    constellation: str,
    image_path: str,
) -> bool:
    """Posts the ROI image to Bluesky."""
    username = config.get("roi_bsky_username")
    password = config.get("roi_bsky_pw")

    if not username or not password or "your-" in username:
        print("Bluesky credentials not configured correctly.", flush=True)
        return False

    try:
        client = Client()
        client.login(username, password)

        # Simple product name: S2-TCI -> TCI
        simple_prod = product_type.split("-")[-1]

        post_text = (
            f"Updated {simple_prod} image of {roi_name}\n"
            f"Satellite: {constellation}\n"
            f"Acquisition time: {acq_time}\n"
            f"Made with material from Copernicus Sentinel"
        )

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
                    alt=f"{simple_prod} image of {roi_name} ({acq_time})",
                    image=upload.blob,
                )
            ]
        )

        client.send_post(text=post_text, embed=embed)
        print(f"Successfully posted {roi_name} to Bluesky.", flush=True)
        return True
    except Exception as e:
        print(f"Error posting to Bluesky: {e}", flush=True)
        return False


def run_roi_stage(process_all: bool = False) -> int:
    """
    Main entry point for the ROI stage.
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

    # Filter for layers to process
    layers_to_check = []
    if process_all:
        layers_to_check = inventory.get("layers", [])
        print("Standalone Mode: Checking all inventory files.", flush=True)
    else:
        # Filter for "new" files rendered in this run
        run_start_dt = datetime.fromtimestamp(
            func.perf_logger.start_time, tz=timezone.utc
        )
        for layer in inventory.get("layers", []):
            render_time_str = layer.get("render_time", "")
            if not render_time_str:
                continue
            try:
                render_dt = datetime.fromisoformat(
                    render_time_str.replace("Z", "+00:00")
                )
                if render_dt >= run_start_dt:
                    layers_to_check.append(layer)
            except Exception:
                continue
        print(
            f"Pipeline Mode: Checking {len(layers_to_check)} new products.", flush=True
        )

    if not layers_to_check:
        print("No products found for ROI processing.", flush=True)
        func.perf_logger.end_step()
        return 0

    print(
        f"Checking {len(layers_to_check)} products against {len(rois)} ROIs.",
        flush=True,
    )

    bsky_post_enabled = config.get("roi_bsky_post", False)
    bsky_roi_names = config.get("roi_bsky_names", [])

    crops_created = 0
    for layer in layers_to_check:
        product_type = layer.get("product", "")
        # SKIP existing ROI crops to avoid recursive cropping
        if product_type.startswith("ROI-"):
            continue

        acq_time = layer.get("acquisition_time", "Unknown")
        tif_rel_path = layer.get("path", "")
        src_path = os.path.join(c.DIRS["OUT"], tif_rel_path)
        constellation = "Sentinel-2" if product_type.startswith("S2") else "Sentinel-1"
        resolution = layer.get("resolution")

        if not os.path.exists(src_path):
            continue

        iso_clean = acq_time.replace(":", "")

        for roi in rois:
            roi_name_raw = roi.get("name", "ROI")
            # Resolve env vars but DON'T force lowercase
            roi_name = func.resolve_env_variable(roi_name_raw)
            # Replace _ with space
            roi_name = roi_name.replace("_", " ")

            roi_bbox_raw = roi.get("bbox", "")
            roi_bbox = func.resolve_env_variable(roi_bbox_raw)
            roi_match_threshold = roi.get("bbox_match", 0)
            desired_products = roi.get("products", [])

            # Check if this product is desired by the ROI
            match_found = False
            for dp in desired_products:
                if dp.upper() in product_type.upper():
                    match_found = True
                    break

            if not match_found:
                continue

            coverage = calculate_coverage(roi_bbox, layer)
            if coverage >= roi_match_threshold:
                # Use full product type (excluding sensor prefix) for better clarity
                # e.g., S2-NDBI_CLEAN -> NDBI_CLEAN, FUSED-RADAR-BURN -> RADAR-BURN
                p_suffix = product_type.split("-", 1)[1] if "-" in product_type else product_type
                
                dst_filename = f"{roi_name}_{p_suffix}_{iso_clean}.tif"
                dst_path = os.path.join(c.DIRS["VIS_ROI"], dst_filename)

                if not os.path.exists(dst_path):
                    print(
                        f"ROI Match: {roi_name} ({coverage:.1f}%) -> Cropping {dst_filename}",
                        flush=True,
                    )
                    if not crop_product(src_path, dst_path, roi_bbox):
                        continue

                    # Generate sidecar for the new crop
                    # Label it as ROI-Name-Prod for clear identification
                    meta.generate_sidecar(
                        dst_path,
                        f"ROI-{roi_name}-{p_suffix}",
                        product_type,  # Keep original legend
                        effective_res=resolution,
                        cloud_cover=layer.get("cloud_cover"),
                    )
                    crops_created += 1

                # Apprise and Bluesky posting
                apprise_url = roi.get("apprise_url", "")
                social_needed = (
                    bsky_post_enabled and roi_name_raw in bsky_roi_names
                )
                
                social_base = os.path.join(
                    c.DIRS["VIS_ROI"],
                    f"{roi_name}_{p_suffix}_{iso_clean}_social",
                )

                # Apprise gets full size JPEG
                if apprise_url:
                    full_image = create_full_image(dst_path, social_base)
                    if full_image:
                        msg = (
                            f"New {p_suffix} image for ROI {roi_name}\n"
                            f"Acquired: {acq_time}\n"
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
                            acq_time,
                            constellation,
                            image_path,
                        )

    print(f"ROI stage complete. {crops_created} crops created.", flush=True)
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
