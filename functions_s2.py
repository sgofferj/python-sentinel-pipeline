#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions_s2.py from https://github.com/sgofferj/python-sentinel-pipeline
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
from osgeo import gdal
import rasterio as rio
import numpy as np
import re
import os
import shutil

gdal.UseExceptions()


# ----- Sentinel 2 helper functions ---------------------------------
def get_utm(name):
    """Gets the UTM grid from a Sentinel 2 dataset name"""
    result = re.search(r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name)
    utm = result.groups()[0]
    return utm


def get_time(name):
    """Gets the production time from a Sentinel 2 dataset name"""
    result = re.search(r"S2._......_(\d+T\d+)_\w\d+_\w\d+_.*_\d+T\d+.SAFE", name)
    time = result.groups()[0]
    return time


def reproject(name):
    gdal.Warp(f"{name}-3857.tif", f"{name}.tif", dstSRS="EPSG:3857", dstAlpha=True)


# ----- Sentinel 2 pipeline functions -------------------------------
def calc_TCI(ds, name):
    """Creates Sentinel 2 true color image (TCI)"""
    name = f"{name}-TCI"
    print(name)
    if not func.outputExists(name):
        sds = rio.open(ds.GetSubDatasets()[c.DS_TCI][0])
        profile = sds.profile
        bands = sds.read([c.BAND_RED, c.BAND_GRN, c.BAND_BLU])

        bands = np.array(
            [func.normalizeminmax(bands[i, :, :]) for i in range(bands.shape[0])]
        )
        func.writeTiffRGB(bands, profile, name)
        reproject(name)
    else:
        print(f"{name} exists, not calculating it again.")


def calc_NIRFC(ds, name):
    """Creates Sentinel 2 false color NIR image"""
    name = f"{name}-NIRFC"
    print(name)
    if not func.outputExists(name):
        sds = rio.open(ds.GetSubDatasets()[c.DS_10m][0])
        profile = sds.profile
        bands = sds.read([c.BAND_NIR, c.BAND_GRN, c.BAND_BLU])
        bands = np.array(
            [func.normalize(bands[i, :, :]) for i in range(bands.shape[0])]
        )
        func.writeTiffRGB(bands, profile, name)
        reproject(name)
    else:
        print(f"{name} exists, not calculating it again.")


def calc_AP(ds, name):
    """Creates Sentinel 2 atmospheric penetration image"""
    name = f"{name}-AP"
    print(name)
    if not func.outputExists(name):
        sds = rio.open(ds.GetSubDatasets()[c.DS_20m][0])
        profile = sds.profile
        bands = sds.read([c.BAND_2190, c.BAND_1610, c.BAND_856])
        bands = np.array(
            [func.normalize(bands[i, :, :]) for i in range(bands.shape[0])]
        )
        func.writeTiffRGB(bands, profile, name)
        reproject(name)
    else:
        print(f"{name} exists, not calculating it again.")


def calc_NDVI(ds, name):
    """Creates Sentinel 2 NDVI"""
    name = f"{name}-NDVI"
    print(name)
    if not func.outputExists(name):
        sds = rio.open(ds.GetSubDatasets()[c.DS_10m][0])
        profile = sds.profile

        bands = sds.read([c.BAND_NIR, c.BAND_RED])

        mask = np.ones((sds.height, sds.width), dtype=np.uint8)
        mask[sds.read(1) == 0] = 0

        ndvi = np.zeros(bands[0].shape, dtype=rio.float32)
        ndvi = (bands[0].astype(float) - bands[1].astype(float)) / (bands[0] + bands[1])

        profile.update(driver="GTIFF", dtype=rio.float32, count=1, compress="deflate")
        with rio.open("/tmp/ndvi.tif", "w", **profile) as dds:
            dds.write(ndvi, indexes=1)
            dds.close()

        gdal.DEMProcessing(
            f"{name}.tif",
            "/tmp/ndvi.tif",
            "color-relief",
            colorFilename="data/ndvi2.txt",
        )
        os.remove("/tmp/ndvi.tif")
        func.writeMask(name, profile)
        reproject(name)
    else:
        print(f"{name} exists, not calculating it again.")


def pipeline(ds, name, processes):
    """The Sentinel 2 pipeline itself"""
    if "TCI" in processes:
        calc_TCI(ds, name)
    if "NIRFC" in processes:
        calc_NIRFC(ds, name)
    if "AP" in processes:
        calc_AP(ds, name)
    if "NDVI" in processes:
        calc_NDVI(ds, name)


def runPipeline(ds, processes):
    """Runs the Sentinel 2 pipeline"""
    productURI = gdal.Info(ds, format="json")["metadata"][""]["PRODUCT_URI"]
    utm = get_utm(productURI)
    time = get_time(productURI) + "Z"

    name = f"{c.OUTDIR}/{utm}-{time}"
    pipeline(ds, name, processes)
