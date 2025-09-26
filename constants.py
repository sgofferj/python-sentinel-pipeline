#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# constants.py from https://github.com/sgofferj/python-sentinel-pipeline/copernicus
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

import os

# ----- Directories -------------------------------------------------
DLDIR = "temp"
OUTDIR = "data"

if not os.path.exists(DLDIR):
    os.makedirs(DLDIR)

if not os.path.exists(OUTDIR):
    os.makedirs(OUTDIR)

# ----- Percentile min/max for scaling ------------------------------
PCT_MIN = 0.5
PCT_MAX = 99.5

# ----- Sentinel 2 subdatasets --------------------------------------
DS_10m = 0  # 10m resolution bands
DS_20m = 1  # 20m resolution bands
DS_60m = 2  # 60m resolution bands
DS_TCI = 3  # True color image

# ----- Sentinel 2 10m resolution bands -----------------------------
BAND_RED = 1  # Red band (665nm)
BAND_GRN = 2  # Green band (560nm)
BAND_BLU = 3  # Blue band (490nm)
BAND_NIR = 4  # NIR band (842nm)

# ----- Sentinel 2 20m resolution bands -----------------------------
BAND_705 = 1  # 705nm band
BAND_740 = 2  # 740nm band
BAND_783 = 3  # 783nm band
BAND_856 = 4  # 856nm band
BAND_1610 = 5  # 1610nm band
BAND_2190 = 6  # 2190nm band

# ----- Sentinel 2 60m resolution bands -----------------------------
BAND_443 = 1  # 443nm band
BAND_945 = 2  # 945nm band
BAND_1375 = 3  # 1375nm band

# ----- Sentinel 2 True color image bands ---------------------------
BAND_RED = 1  # Red band (665nm)
BAND_GRN = 2  # Green band (560nm)
BAND_BLU = 3  # Blue band (490nm)

# ----- Sentinel 1 subdatasets --------------------------------------
DS_VV = 1
DS_VH = 2

# ----- Colormaps ---------------------------------------------------
COLORMAP_NDVI = {
    102: (191, 191, 191, 255),
    114: (173, 173, 173, 255),
    127: (255, 255, 224, 255),
    130: (255, 249, 204, 255),
    133: (237, 232, 181, 255),
    137: (222, 217, 156, 255),
    140: (204, 199, 130, 255),
    143: (189, 184, 107, 255),
    146: (176, 194, 97, 255),
    149: (163, 204, 89, 255),
    153: (145, 191, 82, 255),
    159: (128, 179, 71, 255),
    165: (112, 163, 64, 255),
    172: (97, 150, 54, 255),
    178: (79, 138, 46, 255),
    184: (64, 125, 36, 255),
    191: (48, 110, 28, 255),
    197: (33, 97, 18, 255),
    204: (15, 84, 10, 255),
}
