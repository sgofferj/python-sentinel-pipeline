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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import constants as c
import functions as func
import denoise
from s1_calibrator import S1Calibrator
from osgeo import gdal
import rasterio as rio
from rasterio.enums import ColorInterp
import numpy as np
import re
import os
import gc
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

gdal.UseExceptions()


# ----- Sentinel 1 helper functions ---------------------------------
def get_data(name):
    """Gets the start and end time from a Sentinel 1 dataset name"""
    result = re.search(r".*S1._.*_.*_.*_(\d+T\d+_\d+T\d+)_.*", name)
    times = result.groups()[0]
    return times


def get_uri(name):
    """Gets the product URI from a Sentinel 1 dataset name"""
    result = re.search(r".*(S1._.*_.*_.*_\d+T\d+_\d+T\d+_.+_.+\.SAFE).*", name)
    uri = result.groups()[0]
    return uri


def prepare(ds):
    """Calibrates, denoises, and reprojects S1 data to Float32 Sigma0 + Alpha."""
    cal = S1Calibrator(ds)
    print("Calibrating and denoising bands (with alpha mask)...")
    print("VV")
    cal.calibrate("VV", "/tmp/vv_raw.tif", block_size=1024)
    gc.collect()
    print("VH")
    cal.calibrate("VH", "/tmp/vh_raw.tif", block_size=1024)
    gc.collect()

    print("Reprojecting to EPSG:3857 using optimized gdal.Warp...")

    cores = max(1, multiprocessing.cpu_count() // 2)

    warp_options = gdal.WarpOptions(
        dstSRS="EPSG:3857",
        multithread=True,
        warpMemoryLimit=512,
        warpOptions=[f"NUM_THREADS={cores}"],
    )

    print("VV")
    gdal.Warp("/tmp/vv.tif", "/tmp/vv_raw.tif", options=warp_options)
    os.remove("/tmp/vv_raw.tif")
    gc.collect()

    print("VH")
    gdal.Warp("/tmp/vh.tif", "/tmp/vh_raw.tif", options=warp_options)
    os.remove("/tmp/vh_raw.tif")
    gc.collect()


def cleanup():
    for f in ["/tmp/vv_raw.tif", "/tmp/vh_raw.tif", "/tmp/vv.tif", "/tmp/vh.tif"]:
        if os.path.exists(f):
            os.remove(f)


def _build_ov_task(path):
    """Helper for parallel overview building."""
    print(f"Building overviews for {os.path.basename(path)}...")
    with rio.open(path, "r+") as ds:
        ds.build_overviews([2, 4, 8, 16, 32], rio.enums.Resampling.average)
    return path


def _render_internal(product_paths):
    """Core single-pass rendering logic with parallel overview building."""
    print("Starting optimized single-pass render...")
    cores = max(1, multiprocessing.cpu_count() // 2)

    # Pre-scale parameters
    db_min = c.S1_dB_MIN
    db_range = c.S1_dB_MAX - c.S1_dB_MIN
    ratio_min = c.S1_RATIO_MIN
    ratio_range = c.S1_RATIO_MAX - c.S1_RATIO_MIN
    ndpi_min = c.S1_NDPI_MIN
    ndpi_range = c.S1_NDPI_MAX - c.S1_NDPI_MIN

    with rio.open("/tmp/vv.tif") as vv_src, rio.open("/tmp/vh.tif") as vh_src:
        profile = vv_src.profile.copy()
        profile.update(
            photometric="RGB",
            count=4,
            dtype=rio.uint8,
            nodata=None,
            compress="DEFLATE",
            tiled=True,
            blockxsize=256,
            blockysize=256,
            num_threads=cores,
        )

        dst_handles = {}
        try:
            for p, path in product_paths.items():
                if not func.outputExists(path):
                    h = rio.open(path + ".tif", "w", **profile)
                    h.colorinterp = [
                        ColorInterp.red,
                        ColorInterp.green,
                        ColorInterp.blue,
                        ColorInterp.alpha,
                    ]
                    dst_handles[p] = h

            if not dst_handles:
                print("All requested products already exist.")
                return

            print(f"Rendering {list(dst_handles.keys())}...")
            for _, window in vv_src.block_windows(1):
                vv_s0 = vv_src.read(1, window=window)
                vh_s0 = vh_src.read(1, window=window)
                alpha = vv_src.read(2, window=window).astype(np.uint8)

                # Standalone Denoising (Improved point target preservation for OSINT)
                vv_denoised = denoise.refined_lee_filter(vv_s0, size=5)
                vh_denoised = denoise.improved_lee_filter(vh_s0, size=3)

                # Shared dB scaling
                v_m = vv_denoised > 0
                v_db = np.full_like(vv_denoised, db_min, dtype=np.float32)
                v_db[v_m] = 10 * np.log10(vv_denoised[v_m])
                s_vv = np.clip((v_db - db_min) / db_range * 255, 0, 255).astype(
                    np.uint8
                )

                h_m = vh_denoised > 0
                h_db = np.full_like(vh_denoised, db_min, dtype=np.float32)
                h_db[h_m] = 10 * np.log10(vh_denoised[h_m])
                s_vh = np.clip((h_db - db_min) / db_range * 255, 0, 255).astype(
                    np.uint8
                )

                if "VV" in dst_handles:
                    dst_handles["VV"].write(
                        np.stack([s_vv, s_vv, s_vv, alpha], axis=0), window=window
                    )
                if "VH" in dst_handles:
                    dst_handles["VH"].write(
                        np.stack([s_vh, s_vh, s_vh, alpha], axis=0), window=window
                    )

                if "RATIOVVVH" in dst_handles:
                    # Use Gamma Map for the noisy Ratio product
                    ratio_raw = vv_s0 / (vh_s0 + 1e-9)
                    ratio_denoised = denoise.gamma_map_filter(ratio_raw, size=5, looks=1)
                    s_r = np.clip((ratio_denoised - ratio_min) / ratio_range * 255, 0, 255).astype(
                        np.uint8
                    )
                    dst_handles["RATIOVVVH"].write(
                        np.stack([s_vv, s_vh, s_r, alpha], axis=0), window=window
                    )

                if "NDPI" in dst_handles:
                    # NDPI = (VV - VH) / (VV + VH)
                    denom = vv_s0 + vh_s0
                    r = np.zeros_like(vv_s0)
                    m = denom != 0
                    r[m] = (vv_s0[m] - vh_s0[m]) / denom[m]
                    # Also apply Gamma Map logic to NDPI for consistency
                    ndpi_denoised = denoise.gamma_map_filter(r, size=5, looks=1)
                    s_r = np.clip((ndpi_denoised - ndpi_min) / ndpi_range * 255, 0, 255).astype(
                        np.uint8
                    )
                    dst_handles["NDPI"].write(
                        np.stack([s_vv, s_vh, s_r, alpha], axis=0), window=window
                    )

        finally:
            output_paths = []
            for h in dst_handles.values():
                output_paths.append(h.name)
                h.close()

            if output_paths:
                print(f"Building overviews in parallel (throttled to 2 workers)...")
                with ProcessPoolExecutor(max_workers=2) as executor:
                    list(executor.map(_build_ov_task, output_paths))

            gc.collect()


def pipeline(name, uri, processes):
    """The Sentinel 1 pipeline itself"""
    prepare(uri)

    product_paths = {}
    if "VV" in processes:
        product_paths["VV"] = f"{c.DIRS['S1_VV']}/{name}"
    if "VH" in processes:
        product_paths["VH"] = f"{c.DIRS['S1_VH']}/{name}"
    if "RATIOVVVH" in processes:
        product_paths["RATIOVVVH"] = f"{c.DIRS['S1_RATIOVVVH']}/{name}"
    if "NDPI" in processes:
        product_paths["NDPI"] = f"{c.DIRS['S1_NDPI']}/{name}"

    _render_internal(product_paths)
    cleanup()


def runPipeline(ds, processes):
    """Runs the Sentinel 1 pipeline"""
    desc = gdal.Info(ds, format="json")["description"]
    productUri = os.path.join(c.DIRS["DL"], get_uri(desc))
    times = get_data(desc)
    name = f"S1_{times}"

    # Smart Skip: If all requested products exist, don't even open the manifest
    product_paths = {}
    if "VV" in processes: product_paths["VV"] = os.path.join(c.DIRS['S1_VV'], name)
    if "VH" in processes: product_paths["VH"] = os.path.join(c.DIRS['S1_VH'], name)
    if "RATIOVVVH" in processes: product_paths["RATIOVVVH"] = os.path.join(c.DIRS['S1_RATIOVVVH'], name)
    if "NDPI" in processes: product_paths["NDPI"] = os.path.join(c.DIRS['S1_NDPI'], name)
    
    all_exist = all(func.outputExists(p) for p in product_paths.values())
    if all_exist:
        print(f"  All S1 products for {name} already exist. Skipping heavy processing.")
        return

    # If we need to process, but already have the intermediate reprojected tifs in /tmp
    # we can potentially skip prepare, but usually /tmp is cleared. 
    # Let's check if the base bands (VV/VH) exist in output to avoid re-calibrating.
    prepare(productUri)
    _render_internal(product_paths)
    cleanup()
