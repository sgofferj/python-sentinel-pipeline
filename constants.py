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

# ----- Base Directory ----------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ----- Directories -------------------------------------------------
DIRS = {
    "DL": os.path.join(BASE_DIR, "temp"),
    "OUT": os.path.join(BASE_DIR, "output"),
    "S1": os.path.join(BASE_DIR, "output/s1"),
    "S2": os.path.join(BASE_DIR, "output/s2"),
    "S1_VV": os.path.join(BASE_DIR, "output/s1/vv"),
    "S1_VH": os.path.join(BASE_DIR, "output/s1/vh"),
    "S1_RATIOVVVH": os.path.join(BASE_DIR, "output/s1/ratiovvvh"),
    "S1_NDPI": os.path.join(BASE_DIR, "output/s1/ndpi"),
    "S2_TCI": os.path.join(BASE_DIR, "output/s2/tci"),
    "S2_NIRFC": os.path.join(BASE_DIR, "output/s2/nirfc"),
    "S2_AP": os.path.join(BASE_DIR, "output/s2/ap"),
    "S2_NDVI": os.path.join(BASE_DIR, "output/s2/ndvi"),
    "S2_NDBI": os.path.join(BASE_DIR, "output/s2/ndbi"),
    "S2_NDRE": os.path.join(BASE_DIR, "output/s2/ndre"),
    "S2_NBR": os.path.join(BASE_DIR, "output/s2/nbr"),
    "S2_CAMO": os.path.join(BASE_DIR, "output/s2/camo"),
    "S1S2_FUSED": os.path.join(BASE_DIR, "output/fused"),
}

for dir in DIRS:
    if not os.path.exists(DIRS[dir]):
        print(f"{DIRS[dir]} does not exist - creating...")
        os.makedirs(DIRS[dir])

# ----- Percentile min/max for scaling ------------------------------
S1_PCT_MIN = 2
S1_PCT_MAX = 98

# ----- Absolute dB scaling for consistent S1 contrast --------------
S1_dB_MIN = -30.0
S1_dB_MAX = 0.0

# ----- Absolute scaling for derived S1 products --------------------
S1_RATIO_MIN = 0.1
S1_RATIO_MAX = 3.0
S1_NDPI_MIN = 0.0
S1_NDPI_MAX = 1.0

S2_PCT_MIN = 2
S2_PCT_MAX = 98

# ----- Absolute Reflectance scaling for consistent S2 contrast -----
# L2A reflectance is scaled by 10,000.
# 0 to 10000 is the full range; we use Gamma to handle contrast.
S2_REF_MIN = 0
S2_REF_MAX = 10000

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
