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

"""
Global constants and configuration for the Sentinel pipeline.
"""

import os
from typing import Dict

import numpy as np
from dotenv import load_dotenv

load_dotenv()

BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
TARGET_DIR: str = os.getenv("TARGET_DIR", BASE_DIR)
OUT_BASE: str = os.path.join(TARGET_DIR, "output")

DIRS: Dict[str, str] = {
    "DL": os.path.join(BASE_DIR, "temp"),
    "TMP": "/tmp",
    "OUT": OUT_BASE,
    # --- VISUAL (8-bit RGBA for Leaflet) ---
    "VIS_S1_VV": os.path.join(OUT_BASE, "visual/s1/vv"),
    "VIS_S1_VH": os.path.join(OUT_BASE, "visual/s1/vh"),
    "VIS_S1_RATIO": os.path.join(OUT_BASE, "visual/s1/ratio"),
    "VIS_S2_TCI": os.path.join(OUT_BASE, "visual/s2/tci"),
    "VIS_S2_NIRFC": os.path.join(OUT_BASE, "visual/s2/nirfc"),
    "VIS_S2_NDVI": os.path.join(OUT_BASE, "visual/s2/ndvi"),
    "VIS_S2_NDRE": os.path.join(OUT_BASE, "visual/s2/ndre"),
    "VIS_S2_NDBI": os.path.join(OUT_BASE, "visual/s2/ndbi"),
    "VIS_S2_NDBI_CLEAN": os.path.join(OUT_BASE, "visual/s2/ndbi_clean"),
    "VIS_S2_NBR": os.path.join(OUT_BASE, "visual/s2/nbr"),
    "VIS_S2_CAMO": os.path.join(OUT_BASE, "visual/s2/camo"),
    "VIS_S2_AP": os.path.join(OUT_BASE, "visual/s2/ap"),
    "VIS_FUSED": os.path.join(OUT_BASE, "visual/fused"),
    # --- ANALYTIC (Float32 for Change Detection) ---
    "ANA_S1_VV": os.path.join(OUT_BASE, "analytic/s1/vv"),
    "ANA_S1_VH": os.path.join(OUT_BASE, "analytic/s1/vh"),
    "ANA_S2_NDVI": os.path.join(OUT_BASE, "analytic/s2/ndvi"),
    "ANA_S2_NDRE": os.path.join(OUT_BASE, "analytic/s2/ndre"),
    "ANA_S2_NDBI": os.path.join(OUT_BASE, "analytic/s2/ndbi"),
    "ANA_S2_NBR": os.path.join(OUT_BASE, "analytic/s2/nbr"),
    # --- System ---
    "S1S2_LEGENDS": os.path.join(OUT_BASE, "legends"),
    "S1S2_LOGS": os.path.join(OUT_BASE, "logs"),
}

# Create missing directories
for directory in DIRS.values():
    if not os.path.exists(directory):
        print(f"{directory} does not exist - creating...", flush=True)
        os.makedirs(directory, exist_ok=True)

# ----- Parallelism -------------------------------------------------
# Default to 2 workers for 16GB systems, overridable via env
WORKERS: int = int(os.getenv("PIPELINE_WORKERS", "2"))
# Macro-block size for GPU saturation (2048^2 = 4M pixels)
BLOCK_SIZE: int = 2048

# ----- Sentinel 2 Band Mapping ---------------------------
# Source: Sentinel-2 L2A Product Specification (via GDAL SENTINEL2 Driver)
# 10m Subdataset
BAND_RED: int = 1  # B04 (665nm)
BAND_GRN: int = 2  # B03 (560nm)
BAND_BLU: int = 3  # B02 (490nm)
BAND_NIR: int = 4  # B08 (842nm)

# 20m Subdataset
BAND_RE1: int = 1  # B05 (705nm)
BAND_SW1: int = 5  # B11 (1610nm)
BAND_SW2: int = 6  # B12 (2190nm)

# ----- Sentinel 1 subdatasets --------------------------------------
DS_VV: int = 1
DS_VH: int = 2

# ----- Global Rendering Constraints --------------------------------
S1_DB_MIN: float = -30.0
S1_DB_MAX: float = 0.0

S1_RATIO_MIN: float = 0.5
S1_RATIO_MAX: float = 5.0

S2_REF_MIN: int = 1000
S2_REF_MAX: int = 4000

# Multi-temporal Normalization percentiles
S2_PCT_MIN: int = 2
S2_PCT_MAX: int = 98

# ----- NDVI Integrated Palette (from ndvi2.txt) ---------------------
NDVI_PALETTE: Dict[str, np.ndarray] = {
    "values": np.array(
        [-1.0, -0.2, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    ),
    "r": np.array([0, 165, 204, 255, 255, 255, 255, 217, 163, 105, 61, 38, 0]),
    "g": np.array([0, 0, 153, 255, 235, 190, 145, 230, 204, 179, 145, 115, 68]),
    "b": np.array([0, 38, 0, 204, 175, 115, 55, 115, 89, 64, 43, 26, 0]),
}
