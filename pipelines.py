#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pipelines.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Master pipeline orchestrator for Sentinel-1 and Sentinel-2.
Handles searching, downloading, and triggering individual sensor pipelines.
Refactored for sequential Search -> Download -> Process flow.
Now honors processed-file logging only after successful handling.
"""

import argparse
import os
import subprocess
import time
import zipfile
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from osgeo import gdal

import constants as c
import copernicus as cop
import functions as func
import functions_s1 as s1
import functions_s2 as s2
import functions_s3 as s3
import inventory_manager
from correlate import run_correlation
import overflight_predictor
import search
import cleanup
import notifications
import roi_manager

load_dotenv()

S1_BOX: Optional[str] = os.getenv("S1_BOX")
S2_BOX: Optional[str] = os.getenv("S2_BOX")
S3_BOX: Optional[str] = os.getenv("S3_BOX")

PIPELINES_ENV: str = os.getenv("PIPELINES", default="S1,S2")
PIPELINES_LIST: List[str] = PIPELINES_ENV.split(",")

S1_PRODUCTTYPE: str = os.getenv("S1_PRODUCTTYPE", default="GRD")
S1_PROCESSES: List[str] = os.getenv("S1_PROCESSES", default="VV,VH,RATIOVVVH").split(
    ","
)

S2_PRODUCTTYPE: str = os.getenv("S2_PRODUCTTYPE", default="L2A")
S2_PROCESSES: List[str] = os.getenv(
    "S2_PROCESSES", default="TCI,NIRFC,AP,NDVI,NDBI,NDRE,NBR,CAMO,NDBI_CLEAN"
).split(",")

S3_PRODUCTTYPE: str = os.getenv("S3_PRODUCTTYPE", default="SL_1_RBT___")
S3_PROCESSES: List[str] = os.getenv("S3_PROCESSES", default="BT,FIRE").split(",")

FUSION_PROCESSES: List[str] = os.getenv(
    "FUSION_PROCESSES", default="RADAR-BURN,LIFE-MACHINE,TARGET-PROBE-V2"
).split(",")

CLEANUP_AFTER_RUN: bool = os.getenv("CLEANUP_AFTER_RUN", "false").lower() == "true"
CLEANUP_DAYS: int = int(os.getenv("CLEANUP_DAYS", "30"))
CLEANUP_RAW: bool = os.getenv("CLEANUP_RAW", "true").lower() == "true"
CLEANUP_S1_DAYS: Optional[int] = int(v) if (v := os.getenv("CLEANUP_S1_DAYS")) else None
CLEANUP_S2_DAYS: Optional[int] = int(v) if (v := os.getenv("CLEANUP_S2_DAYS")) else None
CLEANUP_S3_DAYS: Optional[int] = int(v) if (v := os.getenv("CLEANUP_S3_DAYS")) else None
CLEANUP_FUSION_DAYS: Optional[int] = (
    int(v) if (v := os.getenv("CLEANUP_FUSION_DAYS")) else None
)
CLEANUP_S2_VERSIONS: Optional[int] = (
    int(v) if (v := os.getenv("CLEANUP_S2_VERSIONS")) else None
)
EXECUTE_AFTER_PIPELINE: Optional[str] = os.getenv("EXECUTE_AFTER_PIPELINE")

USERNAME: str = os.getenv("COPERNICUS_USERNAME", "")
PASSWORD: str = os.getenv("COPERNICUS_PASSWORD", "")
mycop: Any = cop.connect(USERNAME, PASSWORD)

s1_boxes: List[str] = func.get_boxes(S1_BOX)
s2_boxes: List[str] = func.get_boxes(S2_BOX)
s3_boxes: List[str] = func.get_boxes(S3_BOX)


def download_products(
    search_result: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Downloads and unzips satellite products. Returns list of products ready for processing."""
    ready_products: List[Dict[str, Any]] = []
    print("\nStarting downloads phase.", flush=True)
    for box_files in search_result.values():
        for feat in box_files:
            file_id: str = feat["id"]
            filename: str = feat["properties"]["title"]

            target_path = os.path.join(c.DIRS["DL"], filename)
            if os.path.exists(target_path):
                print(f"Already have {filename}, ready for processing.", flush=True)
                ready_products.append(feat)
            else:
                try:
                    mycop.refreshToken()
                    if mycop.download(file_id, filename, c.DIRS["DL"]):
                        time.sleep(1)
                        downloaded_zip = os.path.join(c.DIRS["DL"], f"{filename}.zip")
                        print(f"Unzipping {downloaded_zip}...", flush=True)
                        with zipfile.ZipFile(downloaded_zip, "r") as zip_ref:
                            zip_ref.extractall(c.DIRS["DL"])
                        os.remove(downloaded_zip)
                        ready_products.append(feat)
                except Exception as error:  # pylint: disable=broad-exception-caught
                    print(
                        f"Problem downloading/unzipping {filename}: {error}", flush=True
                    )

    print(
        f"Downloads phase complete. {len(ready_products)} products ready.", flush=True
    )
    return ready_products


def scan_local_products() -> Dict[str, List[Dict[str, Any]]]:
    """Scans the download directory for existing .SAFE or product folders."""
    print(f"\nScanning local directory {c.DIRS['DL']} for products...", flush=True)
    local_ready: Dict[str, List[Dict[str, Any]]] = {"s1": [], "s2": [], "s3": []}

    if not os.path.exists(c.DIRS["DL"]):
        return local_ready

    for item in os.listdir(c.DIRS["DL"]):
        item_path = os.path.join(c.DIRS["DL"], item)
        if not os.path.isdir(item_path):
            continue

        # Basic identification by name
        feat = {"properties": {"title": item}, "id": f"local_{item}"}
        if item.startswith("S1"):
            local_ready["s1"].append(feat)
        elif item.startswith("S2"):
            local_ready["s2"].append(feat)
        elif item.startswith("S3"):
            local_ready["s3"].append(feat)

    print(
        f"Found {len(local_ready['s1'])} S1, {len(local_ready['s2'])} S2, and {len(local_ready['s3'])} S3 local products.",
        flush=True,
    )
    return local_ready


def _shutdown_system() -> None:
    """Attempts to shutdown the system via user-accessible methods.

    Tries, in order:
    1. KDE Plasma D-Bus via qdbus (no root needed)
    2. systemctl poweroff (polkit-authorised for desktop users)
    3. shutdown -h now (last resort, requires root)
    """
    kde_session = os.environ.get("KDE_FULL_SESSION", "").lower() == "true"

    if kde_session:
        print(
            "  Detected KDE session, trying D-Bus logoutAndShutdown...",
            flush=True,
        )
        try:
            subprocess.run(
                [
                    "qdbus",
                    "org.kde.Shutdown",
                    "/Shutdown",
                    "org.kde.Shutdown.logoutAndShutdown",
                ],
                check=True,
            )
            return
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            print(
                f"  KDE D-Bus shutdown failed ({e}), trying next method...",
                flush=True,
            )

    print("  Trying systemctl poweroff...", flush=True)
    try:
        subprocess.run(["systemctl", "poweroff"], check=True)
        return
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        print(
            f"  systemctl poweroff failed ({e}), trying shutdown as last resort...",
            flush=True,
        )

    subprocess.run(["shutdown", "-h", "now"], check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sentinel Pipeline Orchestrator")
    parser.add_argument(
        "--downloaded",
        action="store_true",
        help="Skip search/download and process files already in temp/",
    )
    parser.add_argument(
        "--shutdown",
        action="store_true",
        help="Shutdown the computer after pipeline, post-run hook, and notifications complete",
    )
    args = parser.parse_args()

    func.perf_logger.start_run()
    print(f"Running pipelines {PIPELINES_LIST}...", flush=True)

    # Stale temp cleanup before any processing
    cleanup.cleanup_temp_files(dry_run=False)

    s1_ready = []
    s2_ready = []
    s3_ready = []

    if args.downloaded:
        print(">>> MODE: Processing local files only.", flush=True)
        local = scan_local_products()
        s1_ready = local["s1"]
        s2_ready = local["s2"]
        s3_ready = local["s3"]
    else:
        # 1. Search Phase
        s1_res = None
        s2_res = None
        s3_res = None

        if "S1" in PIPELINES_LIST:
            print("\n--- Sentinel 1 Search ---", flush=True)
            num_s1, s1_res = search.search_s1(s1_boxes)
            if num_s1 == 0:
                s1_res = None

        if "S2" in PIPELINES_LIST:
            print("\n--- Sentinel 2 Search ---", flush=True)
            num_s2, s2_res = search.search_s2(s2_boxes)
            if num_s2 == 0:
                s2_res = None

        if "S3" in PIPELINES_LIST:
            print("\n--- Sentinel 3 Search ---", flush=True)
            num_s3, s3_res = search.search_s3(s3_boxes)
            if num_s3 == 0:
                s3_res = None

        # 2. Download Phase
        if s1_res:
            print("\n--- Sentinel 1 Downloads ---", flush=True)
            s1_ready = download_products(s1_res)

        if s2_res:
            print("\n--- Sentinel 2 Downloads ---", flush=True)
            s2_ready = download_products(s2_res)

        if s3_res:
            print("\n--- Sentinel 3 Downloads ---", flush=True)
            s3_ready = download_products(s3_res)

    # 3. Process Phase
    processed_s1 = []
    if "S1" in PIPELINES_LIST and s1_ready:
        print("\n--- Sentinel 1 Processing ---", flush=True)
        for feat in s1_ready:
            filename = feat["properties"]["title"]
            manifest = os.path.join(c.DIRS["DL"], filename, "manifest.safe")
            if os.path.exists(manifest):
                try:
                    ds_obj = gdal.Open(manifest)
                    s1.run_pipeline(ds_obj, S1_PROCESSES, FUSION_PROCESSES)
                    ds_obj = None
                    processed_s1.append(feat)
                except Exception as e:
                    print(f"Error processing S1 product {filename}: {e}", flush=True)

        # Update log ONLY after successful processing and NOT in --downloaded mode
        if processed_s1 and not args.downloaded:
            search.update_last_run("s1", processed_s1)
        print("S1 Processing phase complete.", flush=True)

    processed_s2 = []
    if "S2" in PIPELINES_LIST and s2_ready:
        print("\n--- Sentinel 2 Processing ---", flush=True)
        for feat in s2_ready:
            filename = feat["properties"]["title"]
            # Check for L2A or L1C manifest
            manifest = os.path.join(
                c.DIRS["DL"], filename, f"MTD_MSI{S2_PRODUCTTYPE}.xml"
            )
            if not os.path.exists(manifest):
                # Fallback to other possible manifest name
                manifest = os.path.join(c.DIRS["DL"], filename, "MTD_MSIL2A.xml")

            if os.path.exists(manifest):
                try:
                    ds_obj = gdal.Open(manifest)
                    s2.run_pipeline(ds_obj, S2_PROCESSES, FUSION_PROCESSES)
                    ds_obj = None
                    processed_s2.append(feat)
                except Exception as e:
                    print(f"Error processing S2 product {filename}: {e}", flush=True)

        if processed_s2 and not args.downloaded:
            search.update_last_run("s2", processed_s2)
        print("S2 Processing phase complete.", flush=True)

    processed_s3 = []
    if "S3" in PIPELINES_LIST and s3_ready:
        print("\n--- Sentinel 3 Processing ---", flush=True)
        for feat in s3_ready:
            filename = feat["properties"]["title"]
            manifest = os.path.join(c.DIRS["DL"], filename, "xfdumanifest.xml")
            if os.path.exists(manifest):
                try:
                    cloud_cover = feat["properties"].get("cloudCover")
                    product_dir = os.path.join(c.DIRS["DL"], filename)
                    s3.run_pipeline(
                        product_dir,
                        S3_PROCESSES,
                        FUSION_PROCESSES,
                        cloud_cover=cloud_cover,
                    )
                    processed_s3.append(feat)
                except Exception as e:
                    print(f"Error processing S3 product {filename}: {e}", flush=True)

        if processed_s3 and not args.downloaded:
            search.update_last_run("s3", processed_s3)
        print("S3 Processing phase complete.", flush=True)

    # 4. Finalization (Fusion & Inventory)
    should_finalize = bool(processed_s1 or processed_s2 or processed_s3)
    fusion_count = 0
    roi_count = 0
    delta_count = 0
    if args.downloaded and (s1_ready or s2_ready):
        should_finalize = True

    if should_finalize:
        if "FUSION" in PIPELINES_LIST:
            print("\nChecking for S1/S2 overlaps for fusion...", flush=True)
            fusion_count = run_correlation(FUSION_PROCESSES)

        inventory_manager.rebuild_inventory()
        # ROI Stage (includes S1 DELTA via products[] e.g. Kronstadt with combining & notifications)
        roi_count = roi_manager.run_roi_stage()

        # Immediate cleanup of raw data if enabled
        if CLEANUP_RAW:
            print(
                "\nCleaning up ALL source .SAFE/.SEN3 directories...",
                flush=True,
            )
            cleanup.cleanup_all_source_data(dry_run=False)
    else:
        print("\nNothing new to finalize.", flush=True)

    overflight_predictor.predict_all()

    if CLEANUP_AFTER_RUN:
        print("\n--- Running Post-Pipeline Cleanup ---", flush=True)
        cleanup.run_cleanup(
            days=CLEANUP_DAYS,
            dry_run=False,
            s1_days=CLEANUP_S1_DAYS,
            s2_days=CLEANUP_S2_DAYS,
            s3_days=CLEANUP_S3_DAYS,
            fusion_days=CLEANUP_FUSION_DAYS,
            s2_versions=CLEANUP_S2_VERSIONS,
        )

    if EXECUTE_AFTER_PIPELINE:
        print(
            f"\n--- Running Post-Pipeline Hook: {EXECUTE_AFTER_PIPELINE} ---",
            flush=True,
        )
        try:
            subprocess.run(EXECUTE_AFTER_PIPELINE, shell=True, check=False)
        except Exception as e:
            print(f"Error executing post-pipeline hook: {e}", flush=True)

    func.perf_logger.stop_run()

    # --- Notifications ---
    total_duration = time.time() - func.perf_logger.start_time
    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)

    msg = f"Pipeline run complete in {minutes}m {seconds}s.\n"
    msg += f"Processed: {len(processed_s1)} S1, {len(processed_s2)} S2, {len(processed_s3)} S3"
    if fusion_count > 0:
        msg += f", {fusion_count} Fusion"
    if roi_count > 0:
        msg += f", {roi_count} ROI"
    msg += " products."

    if should_finalize:
        msg += "\nInventory updated."

    notifications.send_notification(msg)

    if args.shutdown:
        print("\n--- Shutdown requested, halting system ---", flush=True)
        _shutdown_system()
