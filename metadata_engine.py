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

"""
Metadata engine for generating sidecar JSON files for visual products.
"""

import json
import os
import re
from datetime import datetime
from typing import List, Optional

import rasterio as rio
from rasterio.warp import transform_bounds


def generate_sidecar(tif_path: str, product_type: str, legend_id: str) -> None:
    """
    Generates a .json sidecar for a Visual TIF.
    Contains acquisition time, bounds (Leaflet format), and product metadata.
    """
    if not os.path.exists(tif_path):
        return

    sidecar_path: str = tif_path.replace(".tif", ".json")

    with rio.open(tif_path) as src:
        # Get bounds in EPSG:4326 for Leaflet
        bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        leaflet_bounds: List[List[float]] = [
            [bounds[1], bounds[0]],
            [bounds[3], bounds[2]],
        ]  # [[lat, lon], [lat, lon]]

        # Extract Acquisition Time from filename
        filename: str = os.path.basename(tif_path)
        timestamp: str = "Unknown"

        s1_match: Optional[re.Match] = re.search(r"S1_(\d{8}T\d{6})", filename)
        s2_match: Optional[re.Match] = re.search(r"-(\d{8}T\d{6}Z)", filename)

        if s1_match:
            raw_t: str = s1_match.group(1)
            timestamp = (
                f"{raw_t[:4]}-{raw_t[4:6]}-{raw_t[6:8]}T"
                f"{raw_t[9:11]}:{raw_t[11:13]}:{raw_t[13:15]}Z"
            )
        elif s2_match:
            raw_t_s2: str = s2_match.group(1)
            timestamp = (
                f"{raw_t_s2[:4]}-{raw_t_s2[4:6]}-{raw_t_s2[6:8]}T"
                f"{raw_t_s2[9:11]}:{raw_t_s2[11:13]}:{raw_t_s2[13:15]}Z"
            )

        metadata = {
            "product": product_type,
            "acquisition_time": timestamp,
            "render_time": datetime.now().isoformat() + "Z",
            "bounds": leaflet_bounds,
            "legend_id": legend_id,
            "crs": "EPSG:3857",
        }

        with open(sidecar_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    print(f"Sidecar generated: {os.path.basename(sidecar_path)}", flush=True)


if __name__ == "__main__":
    pass
