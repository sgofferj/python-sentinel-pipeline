#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions.py from https://github.com/sgofferj/python-sentinel-pipeline/copernicus
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
from osgeo import gdal
import rasterio as rio
from rasterio.windows import from_bounds, Window
from rasterio.warp import transform_bounds
from rasterio.warp import reproject
import numpy as np
import re
from datetime import datetime, timedelta
import json

gdal.UseExceptions()


def getBoxes(boxes):
    try:
        result = json.loads(boxes)
    except:
        result = [boxes]
    return result


def yesterday(frmt="%Y-%m-%d", string=True):
    yesterday = datetime.now() - timedelta(1)
    if string:
        return yesterday.strftime(frmt)
    return yesterday


def normalize(ds):
    """Normalize an array percentiles"""
    dmin, dmax = np.percentile(ds, (c.PCT_MIN, c.PCT_MAX))
    dsn = (ds.astype(float) - dmin) / (dmax - dmin)
    dsn = np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)
    return dsn


def normalizeminmax(ds):
    """Normalize an array percentiles"""
    dmin, dmax = np.min(ds), np.max(ds)
    dsn = (ds.astype(float) - dmin) / (dmax - dmin)
    dsn = np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)
    return dsn


def scaleOnes(ds):
    """Scale an array from -1 - 1 to 0-255"""
    dmin, dmax = -1, 1
    dsn = (ds.astype(float) - dmin) / (dmax - dmin)
    dsn = np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)
    return dsn


def getS2utm(name):
    """Gets the UTM grid from a Sentinel 2 dataset name"""
    result = re.search(r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name)
    utm = result.groups()[0]
    return utm


def getS2time(name):
    """Gets the production time from a Sentinel 2 dataset name"""
    result = re.search(r"S2._......_(\d+T\d+)_\w\d+_\w\d+_.*_\d+T\d+.SAFE", name)
    time = result.groups()[0]
    return time


def get_window(dst_crs, dst_transform, width, height, box):
    # Parse correctly: west, south, east, north (lon_min, lat_min, lon_max, lat_max)
    west, south, east, north = map(float, box.split(","))

    # Reproject bbox to dataset CRS (lon/lat order preserved)
    left, bottom, right, top = transform_bounds(
        rio.CRS.from_epsg(4326),
        dst_crs,
        west,
        south,
        east,
        north,
        densify_pts=21,  # helps with curvy reprojection edges
    )

    # Build window from bounds
    win = from_bounds(left, bottom, right, top, transform=dst_transform)

    # Round to integer pixel offsets/sizes
    win = win.round_offsets().round_lengths()

    # Clip to dataset extent
    full = Window(0, 0, width, height)
    win = win.intersection(full)

    return win


def writeTiffRGB(ds, profile, name):
    profile.update(
        photometric="RGB",
        count=3,
        dtype=rio.uint8,
        compress="DEFLATE",
        driver="GTiff",
    )
    with rio.open(f"{name}.tif", "w", **profile) as dds:
        dds.write(ds)
        dds.close()
    with rio.open(f"{name}.tif", "r+", **profile) as dds:
        mask = np.ones((dds.height, dds.width), dtype=np.uint8)
        mask[dds.read(1) == 0] = 0
        dds.write_mask(mask)


def S2_TCI(ds, name, box=None):
    """Creates Sentinel 2 true color image (TCI)"""
    name = f"{name}-TCI"
    print(name)
    sds = rio.open(ds.GetSubDatasets()[c.DS_TCI][0])
    profile = sds.profile
    if box:
        win = get_window(sds.crs, sds.transform, sds.width, sds.height, box)
        bands = sds.read(
            [c.BAND_RED, c.BAND_GRN, c.BAND_BLU],
            window=win,
            boundless=False,  # set True if you want reads outside the edge padded
            masked=True,
        )
        profile.update(width=win.width, height=win.height)
    else:
        bands = sds.read([c.BAND_RED, c.BAND_GRN, c.BAND_BLU])

    bands = np.array([normalizeminmax(bands[i, :, :]) for i in range(bands.shape[0])])
    writeTiffRGB(bands, profile, name)


def S2_NIRFC(ds, name, box=None):
    """Creates Sentinel 2 false color NIR image"""
    name = f"{name}-NIRFC"
    print(name)
    sds = rio.open(ds.GetSubDatasets()[c.DS_10m][0])
    profile = sds.profile
    if box:
        win = get_window(sds.crs, sds.transform, sds.width, sds.height, box)
        bands = sds.read(
            [c.BAND_NIR, c.BAND_GRN, c.BAND_BLU],
            window=win,
            boundless=False,  # set True if you want reads outside the edge padded
            masked=True,
        )
        profile.update(width=win.width, height=win.height)
    else:
        bands = sds.read([c.BAND_NIR, c.BAND_GRN, c.BAND_BLU])
    bands = np.array([normalize(bands[i, :, :]) for i in range(bands.shape[0])])
    writeTiffRGB(bands, profile, name)


def S2_AP(ds, name, box=None):
    """Creates Sentinel 2 atmospheric penetration image"""
    name = f"{name}-AP"
    print(name)
    sds = rio.open(ds.GetSubDatasets()[c.DS_20m][0])
    profile = sds.profile
    if box:
        win = get_window(sds.crs, sds.transform, sds.width, sds.height, box)
        bands = sds.read(
            [c.BAND_2190, c.BAND_1610, c.BAND_856],
            window=win,
            boundless=False,  # set True if you want reads outside the edge padded
            masked=True,
        )
        profile.update(width=win.width, height=win.height)
    else:
        bands = sds.read([c.BAND_2190, c.BAND_1610, c.BAND_856])
    bands = np.array([normalize(bands[i, :, :]) for i in range(bands.shape[0])])
    writeTiffRGB(bands, profile, name)


def S2_NDVI(ds, name):
    """Creates Sentinel 2 NDVI"""
    name = f"{name}-NDVI"
    print(name)
    sds = rio.open(ds.GetSubDatasets()[c.DS_10m][0])
    profile = sds.profile
    bands = sds.read([c.BAND_NIR, c.BAND_RED])
    ndvi = np.zeros(bands[0].shape, dtype=rio.float32)

    ndvi = scaleOnes(
        (bands[0].astype(float) - bands[1].astype(float)) / (bands[0] + bands[1])
    )

    profile.update(driver="GTIFF", dtype=rio.uint8, count=1, compress="lzw")

    with rio.open("ndvitest2.tif", "w", **profile) as dst:
        dst.write(ndvi, indexes=1)
        dst.write_colormap(1, c.COLORMAP_NDVI)


def S1_ratio(ds, name):
    """Creates Sentinel 2 NDVI"""
    name = f"{name}-ratio"
    print(name)

    vv = rio.open(ds.GetSubDatasets()[c.DS_VV][0])
    vh = rio.open(ds.GetSubDatasets()[c.DS_VH][0])
    profile = vv.profile

    vva = vv.read(1)
    vva_norm = np.array([normalize(vva[:, :])])
    vha = vh.read(1)
    vha_norm = np.array([normalize(vha[:, :])])

    ratio = np.zeros(vva.shape, dtype=rio.uint16)
    ratio = (vva / (vha + (vha == 0))).astype(rio.uint16)

    dds = np.array((ratio, vva, vha), dtype=rio.uint16)

    writeTiffRGB(dds, profile, name)
