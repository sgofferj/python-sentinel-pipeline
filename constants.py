#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# constants.py from https://github.com/sgofferj/python-sentinel-pipeline
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
DIRS = {
    "DL": "temp",
    "OUT": "output",
    "S1": "output/s1",
    "S2": "output/s2",
    "S1_VV": "output/s1/vv",
    "S1_VH": "output/s1/vh",
    "S1_RATIOVVVH": "output/s1/ratiovvvh",
    "S1_RATIOVHVV": "output/s1/ratiovhvv",
    "S1_PRODUCTVVVH": "output/s1/productvvvh",
    "S1_DIFFVVVH": "output/s1/diffvvvh",
    "S2_TCI": "output/s2/tci",
    "S2_NIRFC": "output/s2/nirfc",
    "S2_AP": "output/s2/ap",
    "S2_NDVI": "output/s2/ndvi",
}

for dir in DIRS:
    if not os.path.exists(DIRS[dir]):
        os.makedirs(DIRS[dir])

# ----- Percentile min/max for scaling ------------------------------
S1_PCT_MIN = 2
S1_PCT_MAX = 98

S2_PCT_MIN = 2
S2_PCT_MAX = 98

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
