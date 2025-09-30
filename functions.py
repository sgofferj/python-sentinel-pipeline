#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions.py from https://github.com/sgofferj/python-sentinel-pipeline
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
import numpy as np
from datetime import datetime, timedelta, timezone
import json
import os

gdal.UseExceptions()


# ----- General helper functions ------------------------------------


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "off", "0"):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


def getBoxes(boxes):
    try:
        result = json.loads(boxes)
    except:
        result = [boxes]
    return result


def this_moment():
    result = datetime.now(timezone.utc)
    return result.strftime("%Y-%m-%dT%H:%M:%SZ")


def yesterday(frmt="%Y-%m-%d", string=True):
    yesterday = datetime.now() - timedelta(1)
    if string:
        return yesterday.strftime(frmt)
    return yesterday


def normalize(ds):
    """Normalize an array percentiles"""
    dmin, dmax = np.percentile(ds, (c.S2_PCT_MIN, c.S2_PCT_MAX))
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


def get_window(dst_crs, dst_transform, width, height, box):
    west, south, east, north = map(float, box.split(","))
    left, bottom, right, top = transform_bounds(
        rio.CRS.from_epsg(4326),
        dst_crs,
        west,
        south,
        east,
        north,
        densify_pts=21,  # helps with curvy reprojection edges
    )
    win = from_bounds(left, bottom, right, top, transform=dst_transform)
    win = win.round_offsets().round_lengths()
    full = Window(0, 0, width, height)
    win = win.intersection(full)
    return win


def outputExists(name) -> bool:
    if os.path.exists(f"{name}.tif"):
        return True
    else:
        return False


def writeTiffRGB(ds, profile, name):
    profile.update(
        photometric="RGB",
        count=3,
        dtype=rio.uint8,
        compress="deflate",
        driver="GTiff",
    )
    with rio.open(f"{name}.tif", "w", **profile) as dds:
        dds.write(ds)
        dds.close()


def writeMask(name, profile):
    profile.update(compress="deflate")
    with rio.open(f"{name}.tif", "r+", **profile) as dds:
        mask = np.ones((dds.height, dds.width), dtype=np.uint8)
        mask[dds.read(1) == 0] = 0
        dds.write_mask(mask)
        dds.close()
        del mask
