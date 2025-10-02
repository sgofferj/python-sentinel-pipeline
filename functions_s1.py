#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions_s1.py from https://github.com/sgofferj/python-sentinel-pipeline
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


# ----- Sentinel 1 helper functions ---------------------------------
def get_data(name):
    """Gets the start and end time from a Sentinel 1 dataset name"""
    result = re.search(r".*S1._.*_.*_.*_(\d+T\d+_\d+T\d+)_.*", name)
    times = result.groups()[0]
    return times


def get_uri(name):
    """Gets the product URI from a Sentinel 1 dataset name"""
    result = re.search(r".*(S1._.*_.*_.*_\d+T\d+_\d+T\d+_.+_.+\.SAFE).*", name)
    uri = result.groups()[0]
    return uri


def get_percentiles(ds):
    dataset = gdal.Open(ds)
    array = dataset.ReadAsArray()
    min = np.percentile(array, c.S1_PCT_MIN)
    max = np.percentile(array, c.S1_PCT_MAX)
    del array
    del dataset
    return min, max


def get_minmax(ds):
    dataset = gdal.Open(ds)
    array = dataset.ReadAsArray()
    min = np.min(array)
    max = np.max(array)
    del array
    del dataset
    return min, max


# ----- Sentinel 1 pipeline functions -------------------------------


def prepare(ds):
    dsvv = f"SENTINEL1_CALIB:UNCALIB:{ds}/manifest.safe:IW_VV:AMPLITUDE"
    dsvh = f"SENTINEL1_CALIB:UNCALIB:{ds}/manifest.safe:IW_VH:AMPLITUDE"
    print("Extracting bands and reprojecting...")
    print("VV")
    gdal.Warp("/tmp/vv.tif", dsvv, dstSRS="EPSG:3857")
    print("VH")
    gdal.Warp("/tmp/vh.tif", dsvh, dstSRS="EPSG:3857")
    print("Scaling...")
    print("VV")
    min, max = get_percentiles("/tmp/vv.tif")
    gdal.Translate(
        "/tmp/vvn.tif",
        "/tmp/vv.tif",
        outputType=gdal.gdalconst.GDT_Byte,
        scaleParams=[[min, max, 0, 255]],
    )
    print("VH")
    min, max = get_percentiles("/tmp/vh.tif")
    gdal.Translate(
        "/tmp/vhn.tif",
        "/tmp/vh.tif",
        outputType=gdal.gdalconst.GDT_Byte,
        scaleParams=[[min, max, 0, 255]],
    )


def cleanup():
    os.remove("/tmp/vv.tif")
    os.remove("/tmp/vh.tif")
    os.remove("/tmp/vvn.tif")
    os.remove("/tmp/vhn.tif")


def calc_VV(name):
    """Creates Sentinel 1 VV image"""
    name = f"{name}-VV"
    print(name)
    if not func.outputExists(name):
        shutil.copyfile("/tmp/vvn.tif", f"{name}.tif")
        with rio.open(f"{name}.tif") as sds:
            profile = sds.profile
            sds.close()
        func.writeMask(name, profile)

    else:
        print(f"{name} exists, not calculating it again.")


def calc_VH(name):
    """Creates Sentinel 1 VH image"""
    name = f"{name}-VH"
    print(name)
    if not func.outputExists(name):
        shutil.copyfile("/tmp/vhn.tif", f"{name}.tif")
        with rio.open(f"{name}.tif") as sds:
            profile = sds.profile
            sds.close()
        func.writeMask(name, profile)

    else:
        print(f"{name} exists, not calculating it again.")


def calc_ratiovvvh(name):
    """Creates Sentinel 1 ratio image (VV/VH)"""
    name = f"{name}-RATIOVVVH"
    print(name)
    if not func.outputExists(name):
        with rio.open("/tmp/vvn.tif") as sds:
            profile = sds.profile
            vv = sds.read(1)
            sds.close()
        with rio.open("/tmp/vhn.tif") as sds:
            vh = sds.read(1)
            sds.close()
        del sds

        print("Calculating ratio VV/VH...")
        ratio = np.zeros(vv.shape, dtype=rio.float32)
        ratio = (vv / (vh + (vh == 0))).astype(rio.float32)

        print("Writing ratio temp file")
        profile.update(dtype=rio.float32, compress="deflate")
        with rio.open("/tmp/ratio.tif", "w", **profile) as dds:
            dds.write(ratio, indexes=1)
            dds.close()
        del ratio

        print("Scaling ratio band...")
        min, max = get_percentiles("/tmp/ratio.tif")
        gdal.Translate(
            "/tmp/ration.tif",
            "/tmp/ratio.tif",
            outputType=gdal.gdalconst.GDT_Byte,
            scaleParams=[[min, max, 0, 255]],
        )
        os.remove("/tmp/ratio.tif")

        print("Merging bands...")
        dataset = rio.open("/tmp/ration.tif")
        ra = dataset.read(1)
        del dataset
        bands = np.array([vv, vh, ra])
        del vv
        del vh
        del ra

        print("Writing image...")
        profile.update(driver="GTIFF", dtype=rio.uint8, count=3, compress="deflate")
        func.writeTiffRGB(bands, profile, name)
        del bands
        func.writeMask(name, profile)
        os.remove("/tmp/ration.tif")
    else:
        print(f"{name} exists, not calculating it again.")


def calc_ratiovhvv(name):
    """Creates Sentinel 1 ratio image (VH/VV)"""
    name = f"{name}-RATIOVHVV"
    print(name)
    if not func.outputExists(name):
        with rio.open("/tmp/vv.tif") as sds:
            profile = sds.profile
            vv = sds.read(1)
            sds.close()
        with rio.open("/tmp/vh.tif") as sds:
            vh = sds.read(1)
            sds.close()
        del sds

        print("Calculating ratio VH/VV...")
        ratio = np.zeros(vv.shape, dtype=rio.float32)
        ratio = (vh / (vv + (vv == 0))).astype(rio.float32)

        print("Writing ratio temp file")
        profile.update(dtype=rio.float32, compress="deflate")
        with rio.open("/tmp/ratio.tif", "w", **profile) as dds:
            dds.write(ratio, indexes=1)
            dds.close()
        del ratio

        print("Scaling ratio band...")
        min, max = get_percentiles("/tmp/ratio.tif")
        gdal.Translate(
            "/tmp/ration.tif",
            "/tmp/ratio.tif",
            outputType=gdal.gdalconst.GDT_Byte,
            scaleParams=[[min, max, 0, 255]],
        )
        os.remove("/tmp/ratio.tif")

        print("Merging bands...")
        dataset = rio.open("/tmp/ration.tif")
        ra = dataset.read(1)
        del dataset
        bands = np.array([vv, vh, ra])
        del vv
        del vh
        del ra

        print("Writing image...")
        profile.update(driver="GTIFF", dtype=rio.uint8, count=3, compress="deflate")
        func.writeTiffRGB(bands, profile, name)
        del bands
        func.writeMask(name, profile)
        os.remove("/tmp/ration.tif")
    else:
        print(f"{name} exists, not calculating it again.")


def calc_product(name):
    """Creates Sentinel 1 product image"""
    name = f"{name}-PRODUCT"
    print(name)
    if not func.outputExists(name):
        with rio.open("/tmp/vv.tif") as sds:
            profile = sds.profile
            vv = sds.read(1)
            sds.close()
        with rio.open("/tmp/vh.tif") as sds:
            vh = sds.read(1)
            sds.close()
        del sds

        print("Calculating product...")
        product = np.zeros(vv.shape, dtype=rio.float32)
        product = (vv * vh).astype(rio.float32)

        print("Writing product temp file")
        profile.update(dtype=rio.float32, compress="deflate")
        with rio.open("/tmp/product.tif", "w", **profile) as dds:
            dds.write(product, indexes=1)
            dds.close()
        del product

        print("Scaling product band...")
        min, max = get_percentiles("/tmp/product.tif")
        gdal.Translate(
            "/tmp/productn.tif",
            "/tmp/product.tif",
            outputType=gdal.gdalconst.GDT_Byte,
            scaleParams=[[min, max, 0, 255]],
        )
        os.remove("/tmp/product.tif")

        print("Merging bands...")
        dataset = rio.open("/tmp/productn.tif")
        pr = dataset.read(1)
        del dataset
        bands = np.array([vv, vh, pr])
        del vv
        del vh
        del pr

        print("Writing image...")
        profile.update(driver="GTIFF", dtype=rio.uint8, count=3, compress="deflate")
        func.writeTiffRGB(bands, profile, name)
        del bands
        func.writeMask(name, profile)
        os.remove("/tmp/productn.tif")
    else:
        print(f"{name} exists, not calculating it again.")


def calc_difference(name):
    """Creates Sentinel 1 difference image"""
    name = f"{name}-DIFFERENCE"
    print(name)
    if not func.outputExists(name):
        with rio.open("/tmp/vv.tif") as sds:
            profile = sds.profile
            vv = sds.read(1)
            sds.close()
        with rio.open("/tmp/vh.tif") as sds:
            vh = sds.read(1)
            sds.close()
        del sds

        print("Calculating difference...")
        difference = np.zeros(vv.shape, dtype=rio.float32)
        difference = (abs(vv - vh)).astype(rio.float32)

        print("Writing difference temp file")
        profile.update(dtype=rio.float32, compress="deflate")
        with rio.open("/tmp/difference.tif", "w", **profile) as dds:
            dds.write(difference, indexes=1)
            dds.close()
        del difference

        print("Scaling difference band...")
        min, max = get_percentiles("/tmp/difference.tif")
        gdal.Translate(
            "/tmp/differencen.tif",
            "/tmp/difference.tif",
            outputType=gdal.gdalconst.GDT_Byte,
            scaleParams=[[min, max, 0, 255]],
        )
        os.remove("/tmp/difference.tif")

        print("Merging bands...")
        dataset = rio.open("/tmp/differencen.tif")
        diff = dataset.read(1)
        del dataset
        bands = np.array([vv, vh, diff])
        del vv
        del vh
        del diff

        print("Writing image...")
        profile.update(driver="GTIFF", dtype=rio.uint8, count=3, compress="deflate")
        func.writeTiffRGB(bands, profile, name)
        del bands
        func.writeMask(name, profile)
        os.remove("/tmp/differencen.tif")
    else:
        print(f"{name} exists, not calculating it again.")


def pipeline(name, uri, processes):
    """The Sentinel 1 pipeline itself"""
    prepare(uri)
    if "VV" in processes:
        calc_VV(f"{c.DIRS["S1_VV"]}/{name}")
    if "VH" in processes:
        calc_VH(f"{c.DIRS["S1_VH"]}/{name}")
    if "RATIOVVVH" in processes:
        calc_ratiovvvh(f"{c.DIRS["S1_RATIOVVVH"]}/{name}")
    if "RATIOVHVV" in processes:
        calc_ratiovhvv(f"{c.DIRS["S1_RATIOVHVV"]}/{name}")
    if "PRODUCT" in processes:
        calc_product(f"{c.DIRS["S1_PRODUCTVVVH"]}/{name}")
    if "DIFFERENCE" in processes:
        calc_difference(f"{c.DIRS["S1_DIFFVVVH"]}/{name}")
    cleanup()


def runPipeline(ds, processes):
    """Runs the Sentinel 1 pipeline"""
    desc = gdal.Info(ds, format="json")["description"]
    productUri = f"{c.DIRS["DL"]}/{get_uri(desc)}"
    times = get_data(desc)
    name = f"S1_{times}"

    pipeline(name, productUri, processes)
