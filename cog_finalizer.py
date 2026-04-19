#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cog_finalizer.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Finalizer module for converting tiled GeoTIFFs to Cloud Optimized GeoTIFF (COG).
"""

import os
import subprocess
from typing import List

import constants as c
import functions as func


def convert_to_cog(path: str) -> None:
    """
    Converts a TIF to a Cloud Optimized GeoTIFF (COG).
    Uses smart copy to preserve existing overviews.
    """
    if not os.path.exists(path):
        return

    func.perf_logger.start_step(f"COG Conversion: {os.path.basename(path)}")
    tmp_path: str = path + ".tmp.tif"

    # Use configurable WORKERS for multi-threaded compression
    # Check if we are in a parallel worker with restricted threads
    num_threads = os.getenv("GDAL_NUM_THREADS", str(c.WORKERS))

    cmd: List[str] = [
        "gdal_translate",
        "-of",
        "COG",
        "-co",
        "BIGTIFF=YES",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "LEVEL=6",
        "--config",
        "GDAL_NUM_THREADS",
        num_threads,
        path,
        tmp_path,
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True)
        # Replace original with COG
        os.replace(tmp_path, path)
        print(f"Converted to COG: {os.path.basename(path)}", flush=True)
    except subprocess.CalledProcessError as e:
        print(
            f"Error converting {path} to COG: {e.stderr.decode()}",
            flush=True,
        )
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    func.perf_logger.end_step()


if __name__ == "__main__":
    pass
