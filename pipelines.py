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
"""

import os
import time
import zipfile
import re

from osgeo import gdal
from dotenv import load_dotenv

import constants as c
import functions as func
import functions_s1 as s1
import functions_s2 as s2
import copernicus as cop
from search import search_s1, search_s2
from correlate import run_correlation
import inventory_manager

load_dotenv()

S1_BOX = os.getenv("S1_BOX")
S2_BOX = os.getenv("S2_BOX")

PIPELINES_ENV = os.getenv("PIPELINES", default="S1,S2")
PIPELINES_LIST = PIPELINES_ENV.split(",")

S1_PRODUCTTYPE = os.getenv("S1_PRODUCTTYPE", default="GRD")
S1_PROCESSES = os.getenv("S1_PROCESSES", default="VV,VH,RATIOVVVH").split(",")

S2_PRODUCTTYPE = os.getenv("S2_PRODUCTTYPE", default="L2A")
S2_PROCESSES = os.getenv("S2_PROCESSES", default="TCI,NIRFC,AP,NDVI,NDBI,NDRE,NBR,CAMO,NDBI_CLEAN").split(",")

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)

s1_boxes = func.getBoxes(S1_BOX)
s2_boxes = func.getBoxes(S2_BOX)


def download_products(search_result):
    """Downloads and unzips satellite products."""
    print("\nStarting downloads.")
    for box in search_result:
        for files in search_result[box]:
            for fileid in files:
                filename = files[fileid]
                if os.path.exists(f"{c.DIRS['DL']}/{filename}"):
                    print(f"Already have {filename}, not downloading it again.")
                else:
                    print(f"Downloading {filename}...")
                    mycop.refreshToken()
                    mycop.download(fileid, filename, c.DIRS["DL"])
                    time.sleep(1)
                    downloaded_zip = f"{c.DIRS['DL']}/{filename}.zip"
                    print(f"Unzipping {downloaded_zip}...")
                    try:
                        with zipfile.ZipFile(downloaded_zip, "r") as zip_ref:
                            zip_ref.extractall(c.DIRS['DL'])
                        os.remove(downloaded_zip)
                    except Exception as error: # pylint: disable=broad-exception-caught
                        print(f"Problem unzipping: {error}")
    print("All files downloaded.")


def pipeline_s1():
    """Triggers S1 GRD processing."""
    numfiles, search_result = search_s1(s1_boxes)
    if numfiles > 0:
        download_products(search_result)
        print("\nStarting S1 pipeline...")
        for box in search_result:
            for files in search_result[box]:
                for fileid in files:
                    filename = files[fileid]
                    manifest = f"{c.DIRS['DL']}/{filename}/manifest.safe"
                    if os.path.exists(manifest):
                        ds_obj = gdal.Open(manifest)
                        s1.run_pipeline(ds_obj, S1_PROCESSES)
                        ds_obj = None
        print("S1 Pipeline complete.")


def pipeline_s2():
    """Triggers S2 Optical processing."""
    numfiles, search_result = search_s2(s2_boxes)
    if numfiles > 0:
        download_products(search_result)
        print("\nStarting S2 pipeline...")
        for box in search_result:
            for files in search_result[box]:
                for fileid in files:
                    filename = files[fileid]
                    manifest = f"{c.DIRS['DL']}/{filename}/MTD_MSI{S2_PRODUCTTYPE}.xml"
                    if os.path.exists(manifest):
                        ds_obj = gdal.Open(manifest)
                        s2.run_pipeline(ds_obj, S2_PROCESSES)
                        ds_obj = None
        print("S2 Pipeline complete.")


if __name__ == "__main__":
    func.perf_logger.start_run()
    print(f"Running pipelines {PIPELINES_LIST}...")
    if "S1" in PIPELINES_LIST:
        print("Sentinel 1")
        pipeline_s1()
    if "S2" in PIPELINES_LIST:
        print("Sentinel 2")
        pipeline_s2()

    print("\nChecking for S1/S2 overlaps for fusion...")
    run_correlation()
    inventory_manager.rebuild_inventory()
    func.perf_logger.stop_run()
