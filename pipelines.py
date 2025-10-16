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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import constants as c
import functions as func
import functions_s1 as s1
import functions_s2 as s2

import copernicus as cop
from osgeo import gdal
from search import search_s1, search_s2
from dotenv import load_dotenv
import zipfile
import os
import time

load_dotenv()

S1_BOX = os.getenv("S1_BOX")
S2_BOX = os.getenv("S2_BOX")

PIPELINES = os.getenv("PIPELINES", default="S1,S2")
PIPELINES = PIPELINES.split(",")

S1_PRODUCTTYPE = os.getenv("S1_PRODUCTTYPE", default="GRD")
S1_PROCESSES = os.getenv("S1_PROCESSES", default="VV,VH,RATIOVVVH")
S1_PROCESSES = S1_PROCESSES.split(",")

S2_PRODUCTTYPE = os.getenv("S2_PRODUCTTYPE", default="L2A")
S2_PROCESSES = os.getenv("S2_PROCESSES", default="TCI,NIRFC,AP,NDVI")
S2_PROCESSES = S2_PROCESSES.split(",")

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)

s1_boxes = func.getBoxes(S1_BOX)
s2_boxes = func.getBoxes(S2_BOX)


def downloadProducts(searchresult):
    print("\nStarting downloads.")
    for box in searchresult:
        for files in searchresult[box]:
            for fileid in files:
                filename = files[fileid]
                if os.path.exists(f"{c.DIRS["DL"]}/{filename}"):
                    print(f"Already have {filename}, not downloading it again.")
                else:
                    print(f"Downloading {filename}...")
                    mycop.refreshToken()
                    mycop.download(fileid, filename, c.DIRS["DL"])
                    time.sleep(1)  # Sleep a sec to let the filesystem settle
                    downloadedZip = f"{c.DIRS["DL"]}/{filename}.zip"
                    print(f"Unzipping {downloadedZip}...")
                    try:
                        zip_ref = zipfile.ZipFile(downloadedZip, "r")
                    except:
                        print("Problem unzipping:", error)
                    else:
                        try:
                            zip_ref.extractall(c.DIRS["DL"])
                        except Exception as error:
                            print("Problem unzipping:", error)
                        os.remove(downloadedZip)
    print("All files downloaded.")


def pipeline_S1():
    numfiles, searchresult = search_s1(s1_boxes)
    if numfiles > 0:
        downloadProducts(searchresult)
        print("\nStarting pipeline...")
        for box in searchresult:
            print(box)
            for files in searchresult[box]:
                for fileid in files:
                    filename = files[fileid]
                    manifest = f"{c.DIRS["DL"]}/{filename}/manifest.safe"
                    if os.path.exists(manifest):
                        print(manifest)
                        ds = gdal.Open(manifest)
                        s1.runPipeline(ds, S1_PROCESSES)
                        ds = None
        print("Pipeline complete.")


def pipeline_S2():
    numfiles, searchresult = search_s2(s2_boxes)
    if numfiles > 0:
        downloadProducts(searchresult)
        print("\nStarting pipeline...")
        for box in searchresult:
            print(box)
            for files in searchresult[box]:
                for fileid in files:
                    filename = files[fileid]
                    manifest = f"{c.DIRS["DL"]}/{filename}/MTD_MSI{S2_PRODUCTTYPE}.xml"
                    if os.path.exists(manifest):
                        ds = gdal.Open(manifest)
                        s2.runPipeline(ds, S2_PROCESSES)
                        ds = None
        print("Pipeline complete.")


if __name__ == "__main__":
    print(f"Running pipelines {PIPELINES}...")
    if "S1" in PIPELINES:
        print("Sentinel 1")
        pipeline_S1()
    if "S2" in PIPELINES:
        print("Sentinel 2")
        pipeline_S2()
