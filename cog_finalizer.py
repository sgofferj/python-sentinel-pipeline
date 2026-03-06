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

import os
import subprocess
import functions as func
import constants as c

def convert_to_cog(path):
    """
    Converts a TIF to a Cloud Optimized GeoTIFF (COG).
    Uses smart copy to preserve existing overviews.
    """
    if not os.path.exists(path):
        return

    func.perf_logger.start_step(f"COG Conversion: {os.path.basename(path)}")
    tmp_path = path + ".tmp.tif"
    
    # Use configurable WORKERS for multi-threaded compression
    cmd = [
        "gdal_translate",
        "-of", "COG",
        "-co", "COPY_SRC_OVERVIEWS=YES",
        "-co", "COMPRESS=DEFLATE",
        "-co", "LEVEL=6", 
        "--config", "GDAL_NUM_THREADS", str(c.WORKERS),
        path,
        tmp_path
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        # Replace original with COG
        os.replace(tmp_path, path)
        print(f"Converted to COG: {os.path.basename(path)}")
    except subprocess.CalledProcessError as e:
        print(f"Error converting {path} to COG: {e.stderr.decode()}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    
    func.perf_logger.end_step()

if __name__ == "__main__":
    pass
