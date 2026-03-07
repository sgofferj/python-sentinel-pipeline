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
Handles OData queries for Sentinel-1 and Sentinel-2 and maintains local search logs.
"""

import json
import os
from typing import Any, Dict, List, Tuple

import constants as c
import copernicus as cop
import functions as func

# Connect to Copernicus CDSE
USERNAME: str = os.getenv("COPERNICUS_USERNAME", "")
PASSWORD: str = os.getenv("COPERNICUS_PASSWORD", "")
mycop: Any = cop.connect(USERNAME, PASSWORD)


def writelog(sat: str, files: List[Dict[str, Any]]) -> None:
    """Writes search results to a local JSON log file."""
    log_data: Dict[str, Any] = {
        "time": func.this_moment(),
        "files": files,
    }
    log_path: str = os.path.join(c.DIRS["DL"], f"{sat}_last.json")
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=4)


def search_s1(boxes: List[str]) -> Tuple[int, Dict[str, List[Dict[str, str]]]]:
    """Searches for Sentinel-1 GRD products in specified bounding boxes."""
    num_files: int = 0
    search_result: Dict[str, List[Dict[str, str]]] = {}
    total_files: List[Dict[str, Any]] = []

    for box in boxes:
        status, result = mycop.productSearch(
            "Sentinel1",
            productType="GRD",
            sensorMode="IW",
            startDate=func.yesterday(),
            box=box,
        )
        if status == 200:
            box_files: List[Dict[str, str]] = []
            for feat in result["features"]:
                file_id: str = feat["id"]
                title: str = feat["properties"]["title"]
                box_files.append({file_id: title})
                total_files.append(feat)
                num_files += 1
            search_result[box] = box_files

    writelog("s1", total_files)
    return num_files, search_result


def search_s2(boxes: List[str]) -> Tuple[int, Dict[str, List[Dict[str, str]]]]:
    """Searches for Sentinel-2 L2A products in specified bounding boxes."""
    num_files: int = 0
    search_result: Dict[str, List[Dict[str, str]]] = {}
    total_files: List[Dict[str, Any]] = []

    for box in boxes:
        status, result = mycop.productSearch(
            "Sentinel2",
            productType="S2MSI2A",
            startDate=func.yesterday(),
            box=box,
            cloudCover=100,
        )
        if status == 200:
            box_files: List[Dict[str, str]] = []
            for feat in result["features"]:
                file_id: str = feat["id"]
                title: str = feat["properties"]["title"]
                box_files.append({file_id: title})
                total_files.append(feat)
                num_files += 1
            search_result[box] = box_files

    writelog("s2", total_files)
    return num_files, search_result
