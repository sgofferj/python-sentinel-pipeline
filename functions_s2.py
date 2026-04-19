#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions_s2.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Sentinel-2 Optical processing module.
Handles warping, multispectral index calculation, and single-pass rendering.
Uses Fixed Reflectance Scaling (Reflectance 0.0-0.3 -> 0-255) for tile consistency.
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
import functions as func
import legends
import metadata_engine as meta

# --- CUDA Acceleration ---
try:
    import cupy as cp

    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False

gdal.UseExceptions()


def build_overviews_gdal(path: str) -> None:
    """Uses gdaladdo for memory-efficient overview building."""
    func.perf_logger.start_step(f"S2 Overviews: {os.path.basename(path)}")
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


def get_utm(name: str) -> Optional[str]:
    """Gets the UTM grid from a Sentinel 2 dataset name"""
    result: Optional[re.Match] = re.search(
        r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name
    )
    return result.groups()[0] if result else None


def get_time(name: str) -> Optional[str]:
    """Gets the production time from a Sentinel 2 dataset name"""
    result: Optional[re.Match] = re.search(
        r"S2._......_(\d+T\d+)_\w\d+_\w\d+_.*_\d+T\d+.SAFE", name
    )
    return result.groups()[0] if result else None


def prepare(ds_obj: gdal.Dataset) -> None:
    """Reprojects required Sentinel-2 bands to EPSG:3857 at 10m resolution."""
    func.perf_logger.start_step("S2 Warp (EPSG:3857)")
    print("Reprojecting required S2 bands to EPSG:3857 (10m aligned)...", flush=True)

    sub10m: str = ds_obj.GetSubDatasets()[0][0]
    sub20m: str = ds_obj.GetSubDatasets()[1][0]

    warp_options = {
        "dstSRS": "EPSG:3857",
        "xRes": 10,
        "yRes": 10,
        "multithread": True,
        "warpMemoryLimit": 256,
        "warpOptions": [f"NUM_THREADS={c.WORKERS}"],
        "creationOptions": [
            "TILED=YES",
            "BLOCKXSIZE=256",
            "BLOCKYSIZE=256",
            "COMPRESS=DEFLATE",
            "BIGTIFF=YES",
        ],
        "resampleAlg": gdal.GRA_Bilinear,
    }

    gdal.Warp("/tmp/s2_10m.tif", sub10m, **warp_options)
    master_info = gdal.Info("/tmp/s2_10m.tif", format="json")
    bounds = master_info["cornerCoordinates"]
    out_bounds: List[float] = [
        bounds["lowerLeft"][0],
        bounds["lowerLeft"][1],
        bounds["upperRight"][0],
        bounds["upperRight"][1],
    ]
    gdal.Warp("/tmp/s2_20m.tif", sub20m, outputBounds=out_bounds, **warp_options)

    gc.collect()
    func.perf_logger.end_step()


def cleanup() -> None:
    """Removes intermediate temporary files."""
    for f in ["/tmp/s2_10m.tif", "/tmp/s2_20m.tif"]:
        if os.path.exists(f):
            os.remove(f)


def _apply_rdylgn(
    data: np.ndarray, vmin: float = -0.2, vmax: float = 0.5
) -> List[np.ndarray]:
    """Applies a hardcoded RdYlGn-like colormap to normalized data."""
    nodes: List[float] = [vmin, (vmin + vmax) / 2, vmax]
    rs: List[int] = [165, 255, 0]
    gs: List[int] = [0, 255, 104]
    bs: List[int] = [38, 191, 55]
    flat = data.flatten()
    r = np.interp(flat, nodes, rs).astype(np.uint8).reshape(data.shape)
    g = np.interp(flat, nodes, gs).astype(np.uint8).reshape(data.shape)
    b = np.interp(flat, nodes, bs).astype(np.uint8).reshape(data.shape)
    return [r, g, b]


def _apply_urban_heat(data: np.ndarray) -> List[np.ndarray]:
    """Applies a custom Urban Heat Map colormap to NDBI data (-1 to 1)."""
    nodes: List[float] = [-0.6, -0.2, 0.05, 0.3]
    rs: List[int] = [20, 60, 255, 255]
    gs: List[int] = [20, 60, 255, 0]
    bs: List[int] = [40, 60, 0, 0]
    flat = data.flatten()
    r = np.interp(flat, nodes, rs).astype(np.uint8).reshape(data.shape)
    g = np.interp(flat, nodes, gs).astype(np.uint8).reshape(data.shape)
    b = np.interp(flat, nodes, bs).astype(np.uint8).reshape(data.shape)
    return [r, g, b]


def _apply_osint_ramp(
    data: np.ndarray, vmin: float = -0.6, vmax: float = 0.2
) -> List[np.ndarray]:
    """Safety Green -> Electric Cyan -> Magma Red"""
    nodes: List[float] = [vmin, -0.2, -0.05, 0.05, vmax]
    rs: List[int] = [20, 0, 0, 255, 255]
    gs: List[int] = [20, 200, 255, 255, 0]
    bs: List[int] = [60, 0, 255, 0, 0]
    flat = data.flatten()
    r = np.interp(flat, nodes, rs).astype(np.uint8).reshape(data.shape)
    g = np.interp(flat, nodes, gs).astype(np.uint8).reshape(data.shape)
    b = np.interp(flat, nodes, bs).astype(np.uint8).reshape(data.shape)
    return [r, g, b]


def _render_internal(
    visual_paths: Dict[str, str],
    analytic_paths: Dict[str, str],
    skip_overviews: bool = False,
) -> None:
    """Macro-block threaded renderer for S2 indices using Double Buffering and GPU Concurrency."""
    func.perf_logger.start_step("S2 Single-Pass Render", use_gpu=True)
    print(f"Starting Overdrive S2 Render (Block: {c.BLOCK_SIZE})...", flush=True)
    ref_min: int = c.S2_REF_MIN
    ref_max: int = c.S2_REF_MAX
    cv, cr, cg, cb = (
        c.NDVI_PALETTE["values"],
        c.NDVI_PALETTE["r"],
        c.NDVI_PALETTE["g"],
        c.NDVI_PALETTE["b"],
    )

    with rio.open("/tmp/s2_10m.tif") as src10, rio.open("/tmp/s2_20m.tif") as src20:
        v_prof = src10.profile.copy()
        v_prof.update(
            photometric="RGB",
            count=4,
            dtype=rio.uint8,
            nodata=None,
            compress="DEFLATE",
            tiled=True,
            blockxsize=256,
            blockysize=256,
            num_threads=2,
            BIGTIFF="YES",
        )

        a_prof = src10.profile.copy()
        a_prof.update(
            count=1,
            dtype=rio.float32,
            nodata=0,
            compress="DEFLATE",
            tiled=True,
            blockxsize=256,
            blockysize=256,
            num_threads=2,
            BIGTIFF="YES",
        )

        v_handles = {
            p: rio.open(path + ".tif", "w", **v_prof)
            for p, path in visual_paths.items()
            if not func.output_exists(path)
        }
        a_handles = {
            p: rio.open(path + ".tif", "w", **a_prof)
            for p, path in analytic_paths.items()
            if not func.output_exists(path)
        }

        # Explicitly set color interpretation for visual products
        for h in v_handles.values():
            h.colorinterp = [
                ColorInterp.red,
                ColorInterp.green,
                ColorInterp.blue,
                ColorInterp.alpha,
            ]

        read_queue: queue.Queue = queue.Queue(maxsize=2)
        write_queue: queue.Queue = queue.Queue(maxsize=2)

        def reader_thread() -> None:
            try:
                for r in range(0, src10.height, c.BLOCK_SIZE):
                    for col in range(0, src10.width, c.BLOCK_SIZE):
                        window = rio.windows.Window(
                            col,
                            r,
                            min(c.BLOCK_SIZE, src10.width - col),
                            min(c.BLOCK_SIZE, src10.height - r),
                        )
                        bands = {
                            "b02": src10.read(c.BAND_BLU, window=window),
                            "b03": src10.read(c.BAND_GRN, window=window),
                            "b04": src10.read(c.BAND_RED, window=window),
                            "b08": src10.read(c.BAND_NIR, window=window),
                            "b05": src20.read(
                                c.BAND_RE1,
                                window=window,
                                out_shape=(window.height, window.width),
                            ),
                            "b11": src20.read(
                                c.BAND_SW1,
                                window=window,
                                out_shape=(window.height, window.width),
                            ),
                            "b12": src20.read(
                                c.BAND_SW2,
                                window=window,
                                out_shape=(window.height, window.width),
                            ),
                        }
                        read_queue.put((window, bands), timeout=120)
                read_queue.put(None, timeout=120)
            except Exception as e:
                print(f"\nCRITICAL: S2 Reader thread failed: {e}", flush=True)
                read_queue.put(None)

        def writer_thread() -> None:
            try:
                while True:
                    item = write_queue.get(timeout=120)
                    if item is None:
                        write_queue.task_done()
                        break
                    window, results = item
                    for p, h in v_handles.items():
                        if f"{p}_VIS" in results:
                            h.write(results[f"{p}_VIS"], window=window)
                    for p, h in a_handles.items():
                        if f"{p}_ANA" in results:
                            h.write(results[f"{p}_ANA"], 1, window=window)
                    write_queue.task_done()
            except Exception as e:
                print(f"\nCRITICAL: S2 Writer thread failed: {e}", flush=True)

        t_read = threading.Thread(target=reader_thread, daemon=True)
        t_write = threading.Thread(target=writer_thread, daemon=True)
        t_read.start()
        t_write.start()

        def scale(
            band: np.ndarray,
            mn: float = ref_min,
            mx: float = ref_max,
            gamma: float = 2.2,
        ) -> np.ndarray:
            """
            Fixed-Threshold Reflectance Scaling.
            Maps Reflectance 0.0 (DN 1000) to 0.3 (DN 4000) to 0-255 with Gamma 2.2.
            This ensures tile consistency.
            """
            res = (band.astype(np.float32) - mn) / (mx - mn)
            res = np.clip(res, 0, 1)
            if gamma != 1.0:
                res = np.power(res, 1 / gamma)
            return (res * 255).astype(np.uint8)

        while True:
            try:
                item = read_queue.get(timeout=120)
            except queue.Empty:
                print("\nCRITICAL: S2 Reader timed out (Deadlock?).", flush=True)
                break

            if item is None:
                write_queue.put(None, timeout=120)
                read_queue.task_done()
                break

            try:
                window, bands = item
                results = {}

                # Calculate Alpha with a small threshold to avoid 1px dark borders from interpolation
                # Only pixels where sum of RGB > 0 (or some small value)
                # Using b02 as baseline
                alpha = np.where(bands["b02"] > 1, 255, 0).astype(np.uint8)

                # Consistent Scaling across all tiles (Reflectance 0.0 to 0.3)
                # mn=1000 is 0.0 reflectance (baseline offset)
                # mx=4000 is 0.3 reflectance (BOA_QUANT=10000)
                s_b02 = scale(bands["b02"], mn=1000, mx=4000, gamma=2.2)
                s_b03 = scale(bands["b03"], mn=1000, mx=4000, gamma=2.2)
                s_b04 = scale(bands["b04"], mn=1000, mx=4000, gamma=2.2)

                # NIR and SWIR can have higher reflectance, use 0.0 to 0.5 range
                s_b08 = scale(bands["b08"], mn=1000, mx=6000, gamma=1.8)
                s_b11 = scale(bands["b11"], mn=1000, mx=5000, gamma=1.8)
                s_b12 = scale(bands["b12"], mn=1000, mx=5000, gamma=1.8)

                if "TCI" in v_handles:
                    results["TCI_VIS"] = np.stack([s_b04, s_b03, s_b02, alpha], axis=0)
                if "NIRFC" in v_handles:
                    results["NIRFC_VIS"] = np.stack(
                        [s_b08, s_b04, s_b03, alpha], axis=0
                    )
                if "AP" in v_handles:
                    results["AP_VIS"] = np.stack([s_b12, s_b11, s_b08, alpha], axis=0)

                # --- GPU CONCURRENT KERNELS ---
                if HAS_CUDA:
                    m_pool = cp.get_default_memory_pool()
                    g_b04 = cp.array(bands["b04"], dtype=cp.float32)
                    g_b05 = cp.array(bands["b05"], dtype=cp.float32)
                    g_b08 = cp.array(bands["b08"], dtype=cp.float32)
                    g_b11 = cp.array(bands["b11"], dtype=cp.float32)
                    g_b12 = cp.array(bands["b12"], dtype=cp.float32)
                    g_mask = cp.array(alpha, dtype=cp.uint8)

                    def gpu_math(ba, bb):
                        # Use raw DNs for index math to avoid scale artifacts
                        # Indices are naturally normalized -1 to 1
                        denom = (ba - 1000) + (bb - 1000)
                        idx = cp.full_like(ba, -1.0, dtype=cp.float32)
                        valid = (denom != 0) & (g_mask > 0)
                        idx[valid] = ((ba[valid] - 1000) - (bb[valid] - 1000)) / denom[
                            valid
                        ]
                        return idx

                    s1 = cp.cuda.Stream()
                    s2 = cp.cuda.Stream()
                    s3 = cp.cuda.Stream()
                    s4 = cp.cuda.Stream()

                    with s1:
                        ndvi_g = gpu_math(g_b08, g_b04)
                    with s2:
                        ndre_g = gpu_math(g_b08, g_b05)
                    with s3:
                        ndbi_g = gpu_math(g_b11, g_b08)
                    with s4:
                        nbr_g = gpu_math(g_b08, g_b12)

                    cp.cuda.Device(0).synchronize()
                    ndvi_raw = cp.asnumpy(ndvi_g)
                    ndre_raw = cp.asnumpy(ndre_g)
                    ndbi_raw = cp.asnumpy(ndbi_g)
                    nbr_raw = cp.asnumpy(nbr_g)

                    del (
                        g_b04,
                        g_b05,
                        g_b08,
                        g_b11,
                        g_b12,
                        g_mask,
                        ndvi_g,
                        ndre_g,
                        ndbi_g,
                        nbr_g,
                    )
                    m_pool.free_all_blocks()
                else:
                    # CPU path fallback
                    def cpu_math(ba, bb):
                        ba_f = ba.astype(float) - 1000
                        bb_f = bb.astype(float) - 1000
                        denom = ba_f + bb_f
                        res = np.full_like(ba_f, -1.0)
                        m = (denom != 0) & (alpha > 0)
                        res[m] = (ba_f[m] - bb_f[m]) / denom[m]
                        return res

                    ndvi_raw = cpu_math(bands["b08"], bands["b04"])
                    ndre_raw = cpu_math(bands["b08"], bands["b05"])
                    ndbi_raw = cpu_math(bands["b11"], bands["b08"])
                    nbr_raw = cpu_math(bands["b08"], bands["b12"])

                results["NDVI_ANA"] = ndvi_raw
                results["NDRE_ANA"] = ndre_raw
                results["NDBI_ANA"] = ndbi_raw
                results["NBR_ANA"] = nbr_raw

                if "NDVI" in v_handles:
                    flat_ndvi = ndvi_raw.flatten()
                    r_m = (
                        np.interp(flat_ndvi, cv, cr)
                        .astype(np.uint8)
                        .reshape(bands["b08"].shape)
                    )
                    g_m = (
                        np.interp(flat_ndvi, cv, cg)
                        .astype(np.uint8)
                        .reshape(bands["b08"].shape)
                    )
                    b_m = (
                        np.interp(flat_ndvi, cv, cb)
                        .astype(np.uint8)
                        .reshape(bands["b08"].shape)
                    )
                    results["NDVI_VIS"] = np.stack([r_m, g_m, b_m, alpha], axis=0)

                if "NDRE" in v_handles:
                    res_ndre = _apply_rdylgn(ndre_raw)
                    results["NDRE_VIS"] = np.stack(
                        [res_ndre[0], res_ndre[1], res_ndre[2], alpha], axis=0
                    )

                if "NDBI" in v_handles:
                    res_ndbi = _apply_urban_heat(ndbi_raw)
                    results["NDBI_VIS"] = np.stack(
                        [res_ndbi[0], res_ndbi[1], res_ndbi[2], alpha], axis=0
                    )

                if "NDBI_CLEAN" in v_handles:
                    ndbi_clean = ndbi_raw - (ndre_raw * 0.4)
                    res_ndbi_c = _apply_osint_ramp(ndbi_clean)
                    results["NDBI_CLEAN_VIS"] = np.stack(
                        [res_ndbi_c[0], res_ndbi_c[1], res_ndbi_c[2], alpha], axis=0
                    )

                if "NBR" in v_handles:
                    res_nbr = _apply_rdylgn(nbr_raw, vmin=-0.2, vmax=0.5)
                    results["NBR_VIS"] = np.stack(
                        [res_nbr[0], res_nbr[1], res_nbr[2], alpha], axis=0
                    )

                if "CAMO" in v_handles:

                    def scale_nd(val):
                        return np.clip((val + 1) / 2 * 255, 0, 255).astype(np.uint8)

                    results["CAMO_VIS"] = np.stack(
                        [scale_nd(ndvi_raw), scale_nd(ndre_raw), s_b03, alpha], axis=0
                    )

                write_queue.put((window, results), timeout=120)
            except Exception as e:
                print(f"\nCRITICAL: S2 processing loop failed: {e}", flush=True)
                break

            read_queue.task_done()

        t_read.join()
        t_write.join()
        vis_output_paths: List[str] = [h.name for h in v_handles.values()]
        for h in list(v_handles.values()) + list(a_handles.values()):
            h.close()

        func.perf_logger.end_step()

        if vis_output_paths and not skip_overviews:
            # Memory Safety: We use max 2 parallel finalizers if not overriden.
            # Each finalizer will use GDAL_NUM_THREADS=1 to avoid OOM spikes.
            max_finalizers = int(os.getenv("MAX_PARALLEL_FINALIZERS", "2"))
            print(f"Finalizing {len(vis_output_paths)} products (Parallel: {max_finalizers})...", flush=True)
            
            def finalize_product(path):
                # Inside parallel task, we force GDAL to single-thread per process
                # to stay within memory budget
                os.environ["GDAL_NUM_THREADS"] = "1"
                build_overviews_gdal(path)
                p_type = path.split("/")[-2].upper()
                eff_res = 20.0 if p_type in ["AP", "NDBI", "NDBI_CLEAN", "NDRE", "NBR", "CAMO"] else 10.0
                meta.generate_sidecar(path, f"S2-{p_type}", f"S2-{p_type}", effective_res=eff_res)
                cog.convert_to_cog(path)

            with ThreadPoolExecutor(max_workers=min(len(vis_output_paths), max_finalizers)) as executor:
                executor.map(finalize_product, vis_output_paths)

        legends.save_all_legends(c.DIRS["S1S2_LEGENDS"])
        gc.collect()


def run_pipeline(ds_obj: gdal.Dataset, processes: List[str], fusion_processes: List[str] = []) -> None:
    """Entry point for S2 pipeline."""
    product_uri = gdal.Info(ds_obj, format="json")["metadata"][""]["PRODUCT_URI"]
    utm = get_utm(product_uri)
    time_str: str = str(get_time(product_uri)) + "Z"
    name = f"{utm}-{time_str}"

    v_paths: Dict[str, str] = {}
    a_paths: Dict[str, str] = {}
    # Map dependencies: visual_product -> [required_analytic_indices]
    s2_deps = {
        "NDVI": ["NDVI"],
        "NDRE": ["NDRE"],
        "NDBI": ["NDBI"],
        "NDBI_CLEAN": ["NDBI", "NDRE"],
        "CAMO": ["NDVI", "NDRE"],
        "NBR": ["NBR"],
    }
    
    # Track which analytics we actually need to produce
    needed_analytics = set()
    for p in processes:
        if p in s2_deps:
            needed_analytics.update(s2_deps[p])
            
    # Add fusion dependencies for S2:
    if "TARGET-PROBE-V2" in fusion_processes:
        needed_analytics.update(["NDBI", "NDRE"])

    for p in [
        "TCI",
        "NIRFC",
        "AP",
        "NDVI",
        "NDBI",
        "NDRE",
        "NBR",
        "CAMO",
        "NDBI_CLEAN",
    ]:
        if p in processes:
            v_paths[p] = f"{c.DIRS[f'VIS_S2_{p}']}/{name}-{p}"
        
        # Always produce analytic if it's in the 'needed' set or if explicitly requested
        if p in needed_analytics or p in processes:
            if f"ANA_S2_{p}" in c.DIRS:
                a_paths[p] = f"{c.DIRS[f'ANA_S2_{p}']}/{name}-{p}"
    prepare(ds_obj)
    _render_internal(v_paths, a_paths)
    cleanup()
