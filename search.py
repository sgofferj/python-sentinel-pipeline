#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# search.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Satellite product search and logging module.
Handles OData queries for Sentinel-1, Sentinel-2, and Sentinel-3 and maintains local search logs.
"""

import json
import os
from typing import Any, Dict, List, Tuple, Optional

import constants as c
import copernicus as cop
import functions as func

# Connect to Copernicus CDSE
USERNAME: str = os.getenv("COPERNICUS_USERNAME", "")
PASSWORD: str = os.getenv("COPERNICUS_PASSWORD", "")
mycop: Any = cop.connect(USERNAME, PASSWORD)

USE_LOG: bool = os.getenv("USE_LOG", "true").lower() == "true"


def load_log(sat: str) -> Optional[Dict[str, Any]]:
    """Loads the last search log for a satellite."""
    log_path: str = os.path.join(c.DIRS["DL"], f"{sat}_last.json")
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def update_last_run(sat: str, processed_files: List[Dict[str, Any]]) -> None:
    """
    Updates the JSON log by APPENDING newly processed files to the existing list.
    Ensures that files are only added to the 'already handled' list after successful processing.
    """
    if not USE_LOG:
        return

    existing_log = load_log(sat) or {"time": "", "files": []}
    existing_files = existing_log.get("files", [])
    existing_ids = {f["id"] for f in existing_files if "id" in f}

    # Only add truly new files to the log
    new_entries = []
    for f in processed_files:
        if f.get("id") not in existing_ids:
            new_entries.append(f)

    combined_files = existing_files + new_entries

    log_data: Dict[str, Any] = {
        "time": func.this_moment(),
        "files": combined_files,
    }

    log_path: str = os.path.join(c.DIRS["DL"], f"{sat}_last.json")
    with open(log_path, "w", encoding="utf-8") as f_out:
        json.dump(log_data, f_out, indent=4)

    print(
        f"Updated {sat} log: added {len(new_entries)} new files (Total handled: {len(combined_files)}).",
        flush=True,
    )


def search_s1(boxes: List[str]) -> Tuple[int, Dict[str, List[Dict[str, Any]]]]:
    """Searches for Sentinel-1 products. Returns (num_files, results_per_box)."""
    num_files: int = 0
    search_result: Dict[str, List[Dict[str, Any]]] = {}
    seen_ids = set()

    product_type: str = os.getenv("S1_PRODUCTTYPE", "GRD")
    sensor_mode: str = os.getenv("S1_SENSORMODE", "IW")
    max_records: int = int(os.getenv("S1_MAXRECORDS", "5"))
    sort_param: str = os.getenv("S1_SORTPARAM", "startDate")
    sort_order: str = os.getenv("S1_SORTORDER", "descending")

    # Start date logic: fallback to yesterday, but honor search log if USE_LOG is true
    start_date: str = os.getenv("S1_STARTDATE", func.yesterday())
    last_ids: List[str] = []

    if USE_LOG:
        log = load_log("s1")
        if log:
            if not os.getenv("S1_STARTDATE"):
                # Apply a 24-hour look-back to account for cataloging delays
                log_time = log.get("time", start_date)
                start_date = func.offset_timestamp(log_time, hours=24)
            last_ids = [f["id"] for f in log.get("files", []) if "id" in f]

    print(
        f"Searching S1 ({product_type}/{sensor_mode}) from {start_date} (Watermark: {log.get('time') if 'log' in locals() and log else 'N/A'})...",
        flush=True,
    )

    for box in boxes:
        status, result = mycop.productSearch(
            "Sentinel1",
            productType=product_type,
            sensorMode=sensor_mode,
            startDate=start_date,
            box=box,
            maxRecords=max_records,
            sortParam=sort_param,
            sortOrder=sort_order,
        )
        if status == 200:
            box_files: List[Dict[str, Any]] = []
            for feat in result["features"]:
                if file_id := feat.get("id"):
                    if file_id in seen_ids:
                        continue
                    if USE_LOG and file_id in last_ids:
                        continue
                    box_files.append(feat)
                    seen_ids.add(file_id)
                    num_files += 1
            search_result[box] = box_files

    print(
        f"Search complete. Found {num_files} unique new products across {len(boxes)} areas.",
        flush=True,
    )
    return num_files, search_result


def search_s2(boxes: List[str]) -> Tuple[int, Dict[str, List[Dict[str, Any]]]]:
    """Searches for Sentinel-2 products. Returns (num_files, results_per_box)."""
    num_files: int = 0
    search_result: Dict[str, List[Dict[str, Any]]] = {}
    seen_ids = set()

    product_type: str = os.getenv("S2_PRODUCTTYPE", "S2MSI2A")
    cloud_cover: int = int(os.getenv("S2_CLOUDCOVER", "5"))
    max_records: int = int(os.getenv("S2_MAXRECORDS", "5"))
    sort_param: str = os.getenv("S2_SORTPARAM", "startDate")
    sort_order: str = os.getenv("S2_SORTORDER", "descending")

    start_date: str = os.getenv("S2_STARTDATE", func.yesterday())
    last_ids: List[str] = []

    if USE_LOG:
        log = load_log("s2")
        if log:
            if not os.getenv("S2_STARTDATE"):
                # Apply a 24-hour look-back to account for cataloging delays
                log_time = log.get("time", start_date)
                start_date = func.offset_timestamp(log_time, hours=24)
            last_ids = [f["id"] for f in log.get("files", []) if "id" in f]

    print(
        f"Searching S2 ({product_type}, Cloud < {cloud_cover}%) from {start_date} (Watermark: {log.get('time') if 'log' in locals() and log else 'N/A'})...",
        flush=True,
    )

    for box in boxes:
        status, result = mycop.productSearch(
            "Sentinel2",
            productType=product_type,
            startDate=start_date,
            box=box,
            cloudCover=cloud_cover,
            maxRecords=max_records,
            sortParam=sort_param,
            sortOrder=sort_order,
        )
        if status == 200:
            box_files: List[Dict[str, Any]] = []
            for feat in result["features"]:
                if file_id := feat.get("id"):
                    if file_id in seen_ids:
                        continue
                    if USE_LOG and file_id in last_ids:
                        continue
                    box_files.append(feat)
                    seen_ids.add(file_id)
                    num_files += 1
            search_result[box] = box_files

    print(
        f"S2 search complete. Found {num_files} unique new products across {len(boxes)} areas.",
        flush=True,
    )
    return num_files, search_result


def search_s3(boxes: List[str]) -> Tuple[int, Dict[str, List[Dict[str, Any]]]]:
    """Searches for Sentinel-3 products. Returns (num_files, results_per_box)."""
    num_files: int = 0
    search_result: Dict[str, List[Dict[str, Any]]] = {}
    seen_ids = set()

    product_type: str = os.getenv("S3_PRODUCTTYPE", "SL_1_RBT___")
    sensor_mode: str = os.getenv("S3_SENSORMODE", "NT")
    max_records: int = int(os.getenv("S3_MAXRECORDS", "5"))
    sort_param: str = os.getenv("S3_SORTPARAM", "startDate")
    sort_order: str = os.getenv("S3_SORTORDER", "descending")

    start_date: str = os.getenv("S3_STARTDATE", func.yesterday())
    last_ids: List[str] = []

    if USE_LOG:
        log = load_log("s3")
        if log:
            if not os.getenv("S3_STARTDATE"):
                log_time = log.get("time", start_date)
                start_date = func.offset_timestamp(log_time, hours=48)
            last_ids = [f["id"] for f in log.get("files", []) if "id" in f]

    # CDSE returns mixed product types — RBT is only ~15% of results, so fetch a
    # generous sample to reliably get max_records RBT products after filtering
    fetch_limit = max(100, max_records * 10)

    mode_filter = sensor_mode.upper()
    print(
        f"Searching S3 ({product_type}/{sensor_mode}) from {start_date} (fetch_limit={fetch_limit})...",
        flush=True,
    )

    for box in boxes:
        # S3 OData doesn't support productType/sensorMode as attribute filters
        # Filter by filename pattern instead
        status, result = mycop.productSearch(
            "Sentinel3",
            startDate=start_date,
            box=box,
            maxRecords=fetch_limit,
            sortParam=sort_param,
            sortOrder=sort_order,
        )
        if status == 200:
            box_files: List[Dict[str, Any]] = []
            for feat in result["features"]:
                title = feat.get("properties", {}).get("title", "")
                # Filter by product type in filename
                if product_type not in title:
                    continue
                # Filter by sensor mode: _NR_ = NRT, _NT_ = NTC
                if mode_filter == "NRT" and "_NR_" not in title:
                    continue
                if mode_filter == "NT" and "_NT_" not in title:
                    continue
                if file_id := feat.get("id"):
                    if file_id in seen_ids:
                        continue
                    if USE_LOG and file_id in last_ids:
                        continue
                    box_files.append(feat)
                    seen_ids.add(file_id)
                    num_files += 1
            # Trim to the most recent max_records products per box
            search_result[box] = box_files[:max_records]

    print(
        f"S3 search complete. Found {num_files} unique new products across {len(boxes)} areas.",
        flush=True,
    )
    return num_files, search_result
