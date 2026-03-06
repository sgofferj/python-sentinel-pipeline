#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# metadata_engine.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

import os
import json
import rasterio as rio
from rasterio.warp import transform_bounds
import re
from datetime import datetime

def generate_sidecar(tif_path, product_type, legend_id):
    """
    Generates a .json sidecar for a Visual TIF.
    Contains acquisition time, bounds (Leaflet format), and product metadata.
    """
    if not os.path.exists(tif_path):
        return

    sidecar_path = tif_path.replace(".tif", ".json")
    
    with rio.open(tif_path) as src:
        # Get bounds in EPSG:4326 for Leaflet
        bounds = transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
        leaflet_bounds = [[bounds[1], bounds[0]], [bounds[3], bounds[2]]] # [[lat, lon], [lat, lon]]
        
        # Extract Acquisition Time from filename
        # S1 Pattern: S1_20260213T045812_...
        # S2 Pattern: T35VLJ-20260213T100131Z-...
        filename = os.path.basename(tif_path)
        timestamp = "Unknown"
        
        s1_match = re.search(r"S1_(\d{8}T\d{6})", filename)
        s2_match = re.search(r"-(\d{8}T\d{6}Z)", filename)
        
        if s1_match:
            raw_t = s1_match.group(1)
            timestamp = f"{raw_t[:4]}-{raw_t[4:6]}-{raw_t[6:8]}T{raw_t[9:11]}:{raw_t[11:13]}:{raw_t[13:15]}Z"
        elif s2_match:
            raw_t = s2_match.group(1)
            timestamp = f"{raw_t[:4]}-{raw_t[4:6]}-{raw_t[6:8]}T{raw_t[9:11]}:{raw_t[11:13]}:{raw_t[13:15]}Z"

        metadata = {
            "product": product_type,
            "acquisition_time": timestamp,
            "render_time": datetime.now().isoformat() + "Z",
            "bounds": leaflet_bounds,
            "legend_id": legend_id,
            "crs": "EPSG:3857"
        }

        with open(sidecar_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
    
    print(f"Sidecar generated: {os.path.basename(sidecar_path)}")

if __name__ == "__main__":
    # Test logic
    pass
