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
from typing import Any, Dict, List

from osgeo import gdal
from shapely.geometry import box, shape

import constants as c
import functions as func

gdal.UseExceptions()


def load_roi_config() -> List[Dict[str, Any]]:
    """Loads ROI definitions from roi_config.json in the project root."""
    config_path = os.path.join(c.BASE_DIR, "roi_config.json")
    if not os.path.exists(config_path):
        return []
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading ROI config: {e}", flush=True)
        return []


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


def run_roi_stage(process_all: bool = False) -> int:
    """
    Main entry point for the ROI stage.
    Scans the inventory and processes ROI crops.
    
    Args:
        process_all: If True, processes all files on disk. If False, only new files from this run.
    """
    func.perf_logger.start_step("ROI Cropping Stage")

    rois = load_roi_config()
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
        run_start_dt = datetime.fromtimestamp(func.perf_logger.start_time, tz=timezone.utc)
        for layer in inventory.get("layers", []):
            render_time_str = layer.get("render_time", "")
            if not render_time_str:
                continue
            try:
                render_dt = datetime.fromisoformat(render_time_str.replace("Z", "+00:00"))
                if render_dt >= run_start_dt:
                    layers_to_check.append(layer)
            except Exception:
                continue
        print(f"Pipeline Mode: Checking {len(layers_to_check)} new products.", flush=True)

    if not layers_to_check:
        print("No products found for ROI processing.", flush=True)
        func.perf_logger.end_step()
        return 0

    print(
        f"Checking {len(layers_to_check)} products against {len(rois)} ROIs.",
        flush=True,
    )

    crops_created = 0
    for layer in layers_to_check:
        product_type = layer.get("product", "")
        acq_time = layer.get("acquisition_time", "Unknown")
        tif_rel_path = layer.get("path", "")
        src_path = os.path.join(c.DIRS["OUT"], tif_rel_path)

        if not os.path.exists(src_path):
            continue

        # iso: 2026-05-10T12:00:00Z -> 2026-05-10T120000Z
        iso_clean = acq_time.replace(":", "")

        for roi in rois:
            roi_name = roi.get("name", "ROI")
            roi_bbox = roi.get("bbox", "")
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
                # Extract simple product name from "S2-TCI" -> "TCI"
                simple_prod = product_type.split("-")[-1]
                dst_filename = f"{roi_name}_{simple_prod}_{iso_clean}.tif"
                dst_path = os.path.join(c.DIRS["VIS_ROI"], dst_filename)

                if os.path.exists(dst_path):
                    continue

                print(
                    f"ROI Match: {roi_name} ({coverage:.1f}%) -> Cropping {dst_filename}",
                    flush=True,
                )
                if crop_product(src_path, dst_path, roi_bbox):
                    crops_created += 1

    print(f"ROI stage complete. {crops_created} crops created.", flush=True)
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
