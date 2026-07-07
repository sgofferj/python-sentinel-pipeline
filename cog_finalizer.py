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

import rasterio as rio

import constants as c
import functions as func


COG_TAG = "COG_FINALIZED"


def is_cog(path: str) -> bool:
    """Check if a GeoTIFF was already finalized as a COG by our pipeline."""
    try:
        with rio.open(path) as src:
            if src.tags().get(COG_TAG) == "YES":
                return True
            # Also match files with COG-default 512x512 blocks + overviews
            if src.profile.get("tiled") and len(src.overviews(1)) > 0:
                block = src.block_shapes[0]
                if block[0] >= 512 or block[1] >= 512:
                    return True
    except Exception:
        pass
    return False


def has_overviews(path: str) -> bool:
    """Check if a GeoTIFF has internal overviews."""
    try:
        with rio.open(path) as src:
            return len(src.overviews(1)) > 0
    except Exception:
        return False


def convert_to_cog(path: str) -> None:
    """
    Converts a TIF to a Cloud Optimized GeoTIFF (COG).
    Skips if the file is already a COG.
    Uses smart copy to preserve existing overviews.
    """
    if not os.path.exists(path):
        return

    if is_cog(path):
        return

    func.perf_logger.start_step(f"COG Conversion: {os.path.basename(path)}")
    tmp_path: str = path + ".tmp.tif"

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
        "-mo",
        f"{COG_TAG}=YES",
        "--config",
        "GDAL_NUM_THREADS",
        num_threads,
        path,
        tmp_path,
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=1800)
        os.replace(tmp_path, path)
        print(f"Converted to COG: {os.path.basename(path)}", flush=True)
    except subprocess.CalledProcessError as e:
        print(
            f"Error converting {path} to COG: {e.stderr.decode()}",
            flush=True,
        )
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception as e:
        print(f"Error converting {path} to COG: {e}", flush=True)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    func.perf_logger.end_step()


def ensure_overviews(path: str) -> None:
    """Add overviews if the GeoTIFF does not already have them."""
    if not os.path.exists(path):
        return

    if has_overviews(path):
        return

    print(
        f"Building overviews for {os.path.basename(path)} (External Process)...",
        flush=True,
    )
    num_threads = os.getenv("GDAL_NUM_THREADS", str(c.WORKERS))
    try:
        subprocess.run(
            [
                "gdaladdo",
                "-r",
                "average",
                "--config",
                "GDAL_NUM_THREADS",
                num_threads,
                path,
                "2",
                "4",
                "8",
                "16",
                "32",
            ],
            check=True,
            capture_output=True,
            timeout=1800,
        )
        print(
            f"Overview build complete: {os.path.basename(path)}", flush=True
        )
    except Exception as e:
        print(
            f"Warning: gdaladdo failed for {path}: {e}", flush=True,
        )


if __name__ == "__main__":
    pass
