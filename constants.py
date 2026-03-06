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

import os
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DIRS = {
    "DL": os.path.join(BASE_DIR, "temp"),
    "TMP": "/tmp",
    "OUT": os.path.join(BASE_DIR, "output"),
    
    # --- VISUAL (8-bit RGBA for Leaflet) ---
    "VIS_S1_VV": os.path.join(BASE_DIR, "output/visual/s1/vv"),
    "VIS_S1_VH": os.path.join(BASE_DIR, "output/visual/s1/vh"),
    "VIS_S1_RATIO": os.path.join(BASE_DIR, "output/visual/s1/ratio"),
    
    "VIS_S2_TCI": os.path.join(BASE_DIR, "output/visual/s2/tci"),
    "VIS_S2_NIRFC": os.path.join(BASE_DIR, "output/visual/s2/nirfc"),
    "VIS_S2_NDVI": os.path.join(BASE_DIR, "output/visual/s2/ndvi"),
    "VIS_S2_NDRE": os.path.join(BASE_DIR, "output/visual/s2/ndre"),
    "VIS_S2_NDBI": os.path.join(BASE_DIR, "output/visual/s2/ndbi"),
    "VIS_S2_NDBI_CLEAN": os.path.join(BASE_DIR, "output/visual/s2/ndbi_clean"),
    "VIS_S2_NBR": os.path.join(BASE_DIR, "output/visual/s2/nbr"),
    "VIS_S2_CAMO": os.path.join(BASE_DIR, "output/visual/s2/camo"),
    "VIS_S2_AP": os.path.join(BASE_DIR, "output/visual/s2/ap"),
    
    "VIS_FUSED": os.path.join(BASE_DIR, "output/visual/fused"),
    
    # --- ANALYTIC (Float32 for Change Detection) ---
    "ANA_S1_VV": os.path.join(BASE_DIR, "output/analytic/s1/vv"),
    "ANA_S1_VH": os.path.join(BASE_DIR, "output/analytic/s1/vh"),
    
    "ANA_S2_NDVI": os.path.join(BASE_DIR, "output/analytic/s2/ndvi"),
    "ANA_S2_NDRE": os.path.join(BASE_DIR, "output/analytic/s2/ndre"),
    "ANA_S2_NDBI": os.path.join(BASE_DIR, "output/analytic/s2/ndbi"),
    "ANA_S2_NBR": os.path.join(BASE_DIR, "output/analytic/s2/nbr"),

    # --- System ---
    "S1S2_LEGENDS": os.path.join(BASE_DIR, "output/legends"),
    "S1S2_LOGS": os.path.join(BASE_DIR, "output/logs"),
}

for dir in DIRS:
    if not os.path.exists(DIRS[dir]):
        print(f"{DIRS[dir]} does not exist - creating...")
        os.makedirs(DIRS[dir], exist_ok=True)

# ----- Parallelism -------------------------------------------------
# Default to 2 workers for 16GB systems, overridable via env
WORKERS = int(os.getenv("PIPELINE_WORKERS", 2))
# Macro-block size for GPU saturation (4096^2 = 16M pixels)
BLOCK_SIZE = 4096

# ----- Sentinel 2 Band Mapping ---------------------------
# Source: Sentinel-2 L2A Product Specification
# 10m Subdataset
BAND_BLU = 1 # B02 (490nm)
BAND_GRN = 2 # B03 (560nm)
BAND_RED = 3 # B04 (665nm)
BAND_NIR = 4 # B08 (842nm)

# 20m Subdataset
BAND_RE1 = 1 # B05 (705nm)
BAND_SW1 = 5 # B11 (1610nm)
BAND_SW2 = 6 # B12 (2190nm)

# ----- Sentinel 1 subdatasets --------------------------------------
DS_VV = 1
DS_VH = 2

# ----- Global Rendering Constraints --------------------------------
S1_dB_MIN = -30.0
S1_dB_MAX = 0.0

S1_RATIO_MIN = 0.5
S1_RATIO_MAX = 5.0

S2_REF_MIN = 0
S2_REF_MAX = 8000

# ----- NDVI Integrated Palette (from ndvi2.txt) ---------------------
NDVI_PALETTE = {
    "values": np.array([-1.0, -0.2, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]),
    "r": np.array([0, 165, 204, 255, 255, 255, 255, 217, 163, 105, 61, 38, 0]),
    "g": np.array([0, 0, 153, 255, 235, 190, 145, 230, 204, 179, 145, 115, 68]),
    "b": np.array([0, 38, 0, 204, 175, 115, 55, 115, 89, 64, 43, 26, 0])
}
