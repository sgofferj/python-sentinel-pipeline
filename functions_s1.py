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
import queue
import re
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import numpy as np
import rasterio as rio
from osgeo import gdal
from rasterio.enums import ColorInterp

import cog_finalizer as cog
import constants as c
import denoise
import functions as func
import legends
import metadata_engine as meta
from s1_calibrator import S1Calibrator
import gpu_warp

# --- CUDA Acceleration ---
try:
    import cupy as cp

    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False

gdal.UseExceptions()


def build_overviews_gdal(path: str) -> None:
    """Uses gdaladdo for memory-efficient overview building."""
    func.perf_logger.start_step(f"S1 Overviews: {os.path.basename(path)}")
    print(
        f"Building overviews for {os.path.basename(path)} (External Process)...",
        flush=True,
    )
    # Check if we are in a parallel worker with restricted threads
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
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Warning: gdaladdo failed for {path}: {e}", flush=True)
    func.perf_logger.end_step()


def prepare(ds_obj: gdal.Dataset) -> None:
    """Calibrates, denoises, and reprojects S1 data to Float32 Sigma0 + Alpha."""
    safe_path: str = os.path.dirname(ds_obj.GetDescription())
    cal = S1Calibrator(safe_path)
    print("Calibrating and denoising bands (with alpha mask)...", flush=True)

    # 1. VV Calibration
    func.perf_logger.start_step("S1 Prepare (VV Calibration)", use_gpu=True)
    cal.calibrate("VV", "/tmp/vv_raw.tif", block_size=1024, build_ov=False, workers=c.WORKERS)
    func.perf_logger.end_step()

    # 2. VH Calibration
    func.perf_logger.start_step("S1 Prepare (VH Calibration)", use_gpu=True)
    cal.calibrate("VH", "/tmp/vh_raw.tif", block_size=1024, build_ov=False, workers=c.WORKERS)
    func.perf_logger.end_step()

    # 3. Warping
    func.perf_logger.start_step("S1 Warp (EPSG:3857)")
    print("Reprojecting to EPSG:3857...", flush=True)

    if HAS_CUDA and os.getenv("ENABLE_GPU_WARP", "false").lower() in ("true", "1"):
        print("Using CUDA Acceleration for S1 Warp...", flush=True)
        # Warp VV and VH independently for maximum stability
        gpu_warp.reproject_with_cuda("/tmp/vv_raw.tif", "/tmp/vv.tif", dst_crs="EPSG:3857", resolution=10, dst_alpha=True)
        gpu_warp.reproject_with_cuda("/tmp/vh_raw.tif", "/tmp/vh.tif", dst_crs="EPSG:3857", resolution=10, dst_alpha=True)
    else:
        # Standard CPU Path
        warp_options = gdal.WarpOptions(
            dstSRS="EPSG:3857", xRes=10, yRes=10,
            multithread=True, warpMemoryLimit=2048,
            warpOptions=[f"NUM_THREADS={c.WORKERS}"],
            creationOptions=["TILED=YES", "COMPRESS=DEFLATE", "BLOCKXSIZE=256", "BLOCKYSIZE=256", "BIGTIFF=YES"],
            dstAlpha=True, srcNodata=0,
        )
        gdal.Warp("/tmp/vv.tif", "/tmp/vv_raw.tif", options=warp_options)
        gdal.Warp("/tmp/vh.tif", "/tmp/vh_raw.tif", options=warp_options)
        
    # Cleanup raw calibrated bands
    for f in ["/tmp/vv_raw.tif", "/tmp/vh_raw.tif"]:
        if os.path.exists(f): os.remove(f)
        
    func.perf_logger.end_step()


def cleanup() -> None:
    """Removes intermediate temporary files."""
    for f in ["/tmp/vv.tif", "/tmp/vh.tif"]:
        if os.path.exists(f): os.remove(f)


def _render_internal(visual_paths: Dict[str, str], analytic_paths: Dict[str, str]) -> None:
    """Macro-block threaded renderer for maximum GPU saturation using Double Buffering."""
    func.perf_logger.start_step("S1 Single-Pass Render", use_gpu=True)
    print(f"Starting Prefetch S1 Render (Block: {c.BLOCK_SIZE})...", flush=True)

    db_min: float = c.S1_DB_MIN
    db_range: float = c.S1_DB_MAX - c.S1_DB_MIN
    ratio_min: float = c.S1_RATIO_MIN
    ratio_range: float = c.S1_RATIO_MAX - c.S1_RATIO_MIN

    with rio.open("/tmp/vv.tif") as vv_src, rio.open("/tmp/vh.tif") as vh_src:
        print(f"Source Dimensions: {vv_src.width}x{vv_src.height}", flush=True)
        
        v_prof = vv_src.profile.copy()
        v_prof.update(
            photometric="RGB", count=4, dtype=rio.uint8, nodata=None,
            compress="DEFLATE", tiled=True, blockxsize=256, blockysize=256, num_threads=2,
            BIGTIFF="YES",
        )

        a_prof = vv_src.profile.copy()
        a_prof.update(
            count=1, dtype=rio.float32, nodata=0,
            compress="DEFLATE", tiled=True, blockxsize=256, blockysize=256, num_threads=2,
            BIGTIFF="YES",
        )

        v_handles = {p: rio.open(path + ".tif", "w", **v_prof) for p, path in visual_paths.items() if not func.output_exists(path)}
        a_handles = {p: rio.open(path + ".tif", "w", **a_prof) for p, path in analytic_paths.items() if not func.output_exists(path)}

        # Explicitly set Alpha interpretation for visual products
        for h in v_handles.values():
            h.colorinterp = [ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha]

        read_queue: queue.Queue = queue.Queue(maxsize=2)
        write_queue: queue.Queue = queue.Queue(maxsize=2)

        def reader_thread() -> None:
            try:
                if vv_src.height == 0 or vv_src.width == 0:
                    print("Error: Source file has 0 dimensions.", flush=True)
                    read_queue.put(None); return
                    
                for r in range(0, vv_src.height, c.BLOCK_SIZE):
                    for col in range(0, vv_src.width, c.BLOCK_SIZE):
                        window = rio.windows.Window(col, r, min(c.BLOCK_SIZE, vv_src.width - col), min(c.BLOCK_SIZE, vv_src.height - r))
                        vv_data = vv_src.read(1, window=window)
                        vh_data = vh_src.read(1, window=window)
                        # Read geometric alpha from warped Band 2
                        alpha = vv_src.read(2, window=window).astype(np.uint8)
                        read_queue.put((window, vv_data, vh_data, alpha), timeout=120)
                read_queue.put(None, timeout=120)
            except Exception as e:
                print(f"\nCRITICAL: S1 Reader thread failed: {e}", flush=True)
                read_queue.put(None)

        def writer_thread() -> None:
            try:
                while True:
                    item = write_queue.get(timeout=120)
                    if item is None:
                        write_queue.task_done(); break
                    window, res = item
                    for p, h in v_handles.items():
                        if f"{p}_VIS" in res: h.write(res[f"{p}_VIS"], window=window)
                    for p, h in a_handles.items():
                        if f"{p}_ANA" in res: h.write(res[f"{p}_ANA"], 1, window=window)
                    write_queue.task_done()
            except Exception as e:
                print(f"\nCRITICAL: S1 Writer thread failed: {e}", flush=True)

        t_read = threading.Thread(target=reader_thread, daemon=True)
        t_write = threading.Thread(target=writer_thread, daemon=True)
        t_read.start(); t_write.start()

        def db_scale(arr: np.ndarray) -> np.ndarray:
            m = arr > 0
            db_vals = np.full_like(arr, db_min, dtype=np.float32)
            db_vals[m] = 10 * np.log10(arr[m])
            return np.clip((db_vals - db_min) / db_range * 255, 0, 255).astype(np.uint8)

        try:
            while True:
                try:
                    item = read_queue.get(timeout=120)
                except queue.Empty:
                    print("\nCRITICAL: S1 Reader timed out (Deadlock?).", flush=True); break
                    
                if item is None:
                    write_queue.put(None, timeout=120); read_queue.task_done(); break

                try:
                    window, vv_raw, vh_raw, alpha = item
                    results = {}

                    # Processing
                    vv_denoised = denoise.refined_lee_filter(vv_raw, size=5)
                    vh_denoised = denoise.improved_lee_filter(vh_raw, size=3)
                    results["VV_ANA"] = vv_denoised; results["VH_ANA"] = vh_denoised

                    s_vv, s_vh = db_scale(vv_denoised), db_scale(vh_denoised)
                    m_norm = alpha.astype(float) / 255.0
                    def apply_mask(img): return (img.astype(float) * m_norm).astype(np.uint8)

                    if "VV" in v_handles: results["VV_VIS"] = np.stack([apply_mask(s_vv)]*3 + [alpha], axis=0)
                    if "VH" in v_handles: results["VH_VIS"] = np.stack([apply_mask(s_vh)]*3 + [alpha], axis=0)
                    if "RATIO" in v_handles:
                        ratio_denoised = denoise.gamma_map_filter(vv_raw / (vh_raw + 1e-9), size=5, looks=1)
                        s_r = np.clip((ratio_denoised - ratio_min) / ratio_range * 255, 0, 255).astype(np.uint8)
                        results["RATIO_VIS"] = np.stack([apply_mask(s_vv), apply_mask(s_vh), apply_mask(s_r), alpha], axis=0)

                    write_queue.put((window, results), timeout=120)
                except Exception as e:
                    print(f"\nCRITICAL: S1 processing loop failed: {e}", flush=True)
                    break
                    
                read_queue.task_done()
        finally:
            write_queue.put(None, timeout=5)
            t_read.join(); t_write.join()
            vis_output_paths: List[str] = [h.name for h in v_handles.values()]
            for h in list(v_handles.values()) + list(a_handles.values()): h.close()

        func.perf_logger.end_step()

        if vis_output_paths:
            # Memory Safety: We use max 2 parallel finalizers if not overriden.
            # Each finalizer will use GDAL_NUM_THREADS=1 to avoid OOM spikes.
            max_finalizers = int(os.getenv("MAX_PARALLEL_FINALIZERS", "2"))
            print(f"Finalizing {len(vis_output_paths)} S1 products (Parallel: {max_finalizers})...", flush=True)

            def finalize_product(path):
                # Inside parallel task, we force GDAL to single-thread per process
                # to stay within memory budget
                os.environ["GDAL_NUM_THREADS"] = "1"
                cog.convert_to_cog(path)
                p_type = path.split("/")[-2].upper()
                meta.generate_sidecar(path, f"S1-{p_type}", f"S1-{p_type}", effective_res=15.0)

            with ThreadPoolExecutor(max_workers=min(len(vis_output_paths), max_finalizers)) as executor:
                executor.map(finalize_product, vis_output_paths)

        legends.save_all_legends(c.DIRS["S1S2_LEGENDS"])
        gc.collect()


def run_pipeline(ds_obj: gdal.Dataset, processes: List[str], fusion_processes: List[str] = []) -> None:
    """Entry point for S1 pipeline."""
    desc = gdal.Info(ds_obj, format="json")["description"]
    times_match = re.search(r".*S1._.*_.*_.*_(\d+T\d+_\d+T\d+)_.*", desc)
    if not times_match: return
    name = f"S1_{times_match.groups()[0]}"

    prepare(ds_obj)
    v_paths: Dict[str, str] = {}
    a_paths: Dict[str, str] = {}

    # Fusion dependencies for S1:
    # All current fusion products (RADAR-BURN, LIFE-MACHINE, TARGET-PROBE-V2) need VH
    needs_vh_for_fusion = any(p in fusion_processes for p in ["RADAR-BURN", "LIFE-MACHINE", "TARGET-PROBE-V2"])

    if "VV" in processes or "RATIOVVVH" in processes:
        v_paths["VV"] = f"{c.DIRS['VIS_S1_VV']}/{name}"
        a_paths["VV"] = f"{c.DIRS['ANA_S1_VV']}/{name}"
        if "VV" not in processes:
            del v_paths["VV"]

    if "VH" in processes or "RATIOVVVH" in processes or needs_vh_for_fusion:
        v_paths["VH"] = f"{c.DIRS['VIS_S1_VH']}/{name}"
        a_paths["VH"] = f"{c.DIRS['ANA_S1_VH']}/{name}"
        if "VH" not in processes:
            del v_paths["VH"]

    if "RATIOVVVH" in processes:
        v_paths["RATIO"] = f"{c.DIRS['VIS_S1_RATIO']}/{name}"
    _render_internal(v_paths, a_paths)
    cleanup()
