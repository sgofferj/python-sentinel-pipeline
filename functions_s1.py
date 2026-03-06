#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions_s1.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Sentinel-1 GRD processing module.
Handles calibration, warping, and single-pass rendering of SAR products.
"""

import gc
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import rasterio as rio
from rasterio.enums import ColorInterp
from osgeo import gdal

import constants as c
import functions as func
import denoise
import legends
import metadata_engine as meta
import cog_finalizer as cog
from s1_calibrator import S1Calibrator

gdal.UseExceptions()


def build_overviews_gdal(path):
    """Uses gdaladdo for memory-efficient overview building."""
    func.perf_logger.start_step(f"S1 Overviews: {os.path.basename(path)}")
    try:
        subprocess.run(
            [
                "gdaladdo", "-r", "average",
                "--config", "GDAL_NUM_THREADS", str(c.WORKERS),
                path, "2", "4", "8", "16", "32"
            ],
            check=True,
            capture_output=True
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Warning: gdaladdo failed for {path}: {e}")
    func.perf_logger.end_step()


def prepare(ds_obj):
    """Calibrates, denoises, and reprojects S1 data to Float32 Sigma0 + Alpha."""
    # S1Calibrator expects a path string, not a GDAL Dataset object
    safe_path = os.path.dirname(ds_obj.GetDescription())
    cal = S1Calibrator(safe_path)
    print("Calibrating and denoising bands (with alpha mask)...")

    func.perf_logger.start_step("S1 Prepare (VV Calibration)")
    cal.calibrate("VV", "/tmp/vv_raw.tif", block_size=1024, build_ov=False, workers=c.WORKERS)
    func.perf_logger.end_step()

    func.perf_logger.start_step("S1 Prepare (VH Calibration)")
    cal.calibrate("VH", "/tmp/vh_raw.tif", block_size=1024, build_ov=False, workers=c.WORKERS)
    func.perf_logger.end_step()

    func.perf_logger.start_step("S1 Warp (EPSG:3857)")
    warp_options = gdal.WarpOptions(
        dstSRS="EPSG:3857",
        multithread=True,
        warpMemoryLimit=512,
        warpOptions=[f"NUM_THREADS={c.WORKERS}"],
    )
    gdal.Warp("/tmp/vv.tif", "/tmp/vv_raw.tif", options=warp_options)
    os.remove("/tmp/vv_raw.tif")
    gdal.Warp("/tmp/vh.tif", "/tmp/vh_raw.tif", options=warp_options)
    os.remove("/tmp/vh_raw.tif")
    func.perf_logger.end_step()


def cleanup():
    """Removes intermediate temporary files."""
    for f in ["/tmp/vv_raw.tif", "/tmp/vh_raw.tif", "/tmp/vv.tif", "/tmp/vh.tif"]:
        if os.path.exists(f):
            os.remove(f)


def _render_internal(visual_paths, analytic_paths):
    """Macro-block threaded renderer for maximum GPU saturation."""
    func.perf_logger.start_step("S1 Single-Pass Render")
    print(f"Starting Overdrive S1 Render (Block: {c.BLOCK_SIZE})...")

    db_min = c.S1_dB_MIN
    db_range = c.S1_dB_MAX - c.S1_dB_MIN
    ratio_min = c.S1_RATIO_MIN
    ratio_range = c.S1_RATIO_MAX - c.S1_RATIO_MIN

    with rio.open("/tmp/vv.tif") as vv_src, rio.open("/tmp/vh.tif") as vh_src:
        v_prof = vv_src.profile.copy()
        v_prof.update(
            photometric="RGB", count=4, dtype=rio.uint8, nodata=None,
            compress="DEFLATE", tiled=True, blockxsize=256, blockysize=256, num_threads=2
        )

        a_prof = vv_src.profile.copy()
        a_prof.update(
            count=1, dtype=rio.float32, nodata=0,
            compress="DEFLATE", tiled=True, blockxsize=256, blockysize=256, num_threads=2
        )

        v_handles = {
            p: rio.open(path + ".tif", "w", **v_prof)
            for p, path in visual_paths.items() if not func.outputExists(path)
        }
        a_handles = {
            p: rio.open(path + ".tif", "w", **a_prof)
            for p, path in analytic_paths.items() if not func.outputExists(path)
        }

        for h in v_handles.values():
            h.colorinterp = [ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha]

        def db_scale(arr):
            m = arr > 0
            db_vals = np.full_like(arr, db_min, dtype=np.float32)
            db_vals[m] = 10 * np.log10(arr[m])
            return np.clip((db_vals - db_min) / db_range * 255, 0, 255).astype(np.uint8)

        def process_block(window):
            vv_s0 = vv_src.read(1, window=window)
            vh_s0 = vh_src.read(1, window=window)
            alpha = vv_src.read(2, window=window).astype(np.uint8)

            # GPU-Accelerated Denoising
            vv_denoised = denoise.refined_lee_filter(vv_s0, size=5)
            vh_denoised = denoise.improved_lee_filter(vh_s0, size=3)

            # Analytic Writes
            if "VV" in a_handles:
                a_handles["VV"].write(vv_denoised, 1, window=window)
            if "VH" in a_handles:
                a_handles["VH"].write(vh_denoised, 1, window=window)

            s_vv = db_scale(vv_denoised)
            s_vh = db_scale(vh_denoised)

            if "VV" in v_handles:
                v_handles["VV"].write(np.stack([s_vv, s_vv, s_vv, alpha], axis=0), window=window)
            if "VH" in v_handles:
                v_handles["VH"].write(np.stack([s_vh, s_vh, s_vh, alpha], axis=0), window=window)
            if "RATIO" in v_handles:
                ratio_raw = vv_s0 / (vh_s0 + 1e-9)
                ratio_denoised = denoise.gamma_map_filter(ratio_raw, size=5, looks=1)
                s_r = np.clip((ratio_denoised - ratio_min) / ratio_range * 255, 0, 255).astype(np.uint8)
                v_handles["RATIO"].write(np.stack([s_vv, s_vh, s_r, alpha], axis=0), window=window)

        # Macro-block logic
        windows = []
        for r in range(0, vv_src.height, c.BLOCK_SIZE):
            for col in range(0, vv_src.width, c.BLOCK_SIZE):
                windows.append(rio.windows.Window(
                    col, r,
                    min(c.BLOCK_SIZE, vv_src.width - col),
                    min(c.BLOCK_SIZE, vv_src.height - r)
                ))

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.map(process_block, windows)

        # Only build overviews and finalizers for VISUAL paths
        vis_output_paths = []
        for h in v_handles.values():
            vis_output_paths.append(h.name)
            h.close()
        
        # Just close analytics
        for h in a_handles.values():
            h.close()

        func.perf_logger.end_step()
        
        # Finalize display layers
        for path in vis_output_paths:
            build_overviews_gdal(path)
            p_type = path.split('/')[-2].upper()
            meta.generate_sidecar(path, f"S1-{p_type}", f"S1-{p_type}")
            cog.convert_to_cog(path)
            
        legends.save_all_legends(c.DIRS['S1S2_LEGENDS'])
        gc.collect()


def run_pipeline(ds_obj, processes):
    """Entry point for S1 pipeline."""
    desc = gdal.Info(ds_obj, format="json")["description"]
    times = re.search(r".*S1._.*_.*_.*_(\d+T\d+_\d+T\d+)_.*", desc).groups()[0]
    name = f"S1_{times}"

    prepare(ds_obj)
    v_paths = {}
    a_paths = {}
    if "VV" in processes:
        v_paths["VV"] = f"{c.DIRS['VIS_S1_VV']}/{name}"
        a_paths["VV"] = f"{c.DIRS['ANA_S1_VV']}/{name}"
    if "VH" in processes:
        v_paths["VH"] = f"{c.DIRS['VIS_S1_VH']}/{name}"
        a_paths["VH"] = f"{c.DIRS['ANA_S1_VH']}/{name}"
    if "RATIOVVVH" in processes:
        v_paths["RATIO"] = f"{c.DIRS['VIS_S1_RATIO']}/{name}"
    _render_internal(v_paths, a_paths)
    cleanup()
