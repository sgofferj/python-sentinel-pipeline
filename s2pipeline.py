#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# s2pipeline.py from https://github.com/sgofferj/python-sentinel-pipeline/copernicus
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
import copernicus as cop
from osgeo import gdal
from search import S2_search
from dotenv import load_dotenv
import zipfile
import os
import time

load_dotenv()

S2_BOX = os.getenv("S2_BOX")
S2_PRODUCTTYPE = os.getenv("S2_PRODUCTTYPE", default="L2A")
S2_CLIP = os.getenv("S2_CLIP", default=True)

boxes = func.getBoxes(S2_BOX)
searchresult = S2_search(boxes)

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)


if len(searchresult) > 0:
    print("\nStarting download.")
    for box in searchresult:
        for files in searchresult[box]:
            for fileid in files:
                filename = files[fileid]
                print(f"Downloading {filename}...")
                mycop.refreshToken()
                mycop.download(fileid, filename, c.DLDIR)
                time.sleep(1)  # Sleep a sec to let the filesystem settle
                downloadedZip = f"{c.DLDIR}/{filename}.zip"
                print(f"Unzipping {downloadedZip}...")
                try:
                    zip_ref = zipfile.ZipFile(downloadedZip, "r")
                except:
                    print("Problem unzipping:", error)
                else:
                    try:
                        zip_ref.extractall(c.DLDIR)
                    except Exception as error:
                        print("Problem unzipping:", error)
                    os.remove(downloadedZip)
    print("All files downloaded.")

    print("\nStarting pipeline...")
    for box in searchresult:
        print(box)
        for files in searchresult[box]:
            for fileid in files:
                filename = files[fileid]
                manifest = f"{c.DLDIR}/{filename}/MTD_MSI{S2_PRODUCTTYPE}.xml"
                if os.path.exists(manifest):
                    ds = gdal.Open(manifest)
                    productURI = gdal.Info(ds, format="json")["metadata"][""][
                        "PRODUCT_URI"
                    ]
                    utm = func.getS2utm(productURI)
                    time = func.getS2time(productURI) + "Z"
                    boxname = box.replace(",", "_")
                    name = f"{c.OUTDIR}/{boxname}-{utm}-{time}"
                    func.S2_TCI(ds, name, box=box)
                    func.S2_NIRFC(ds, name, box=box)
                    func.S2_AP(ds, name, box=box)
                    # func.S2_NDVI(ds, name)
                    ds = None
    print("Pipeline complete.")
