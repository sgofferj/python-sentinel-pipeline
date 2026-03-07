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
"""

import gc
import os
import re
import subprocess
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

gdal.UseExceptions()


def build_overviews_gdal(path: str) -> None:
    """Uses gdaladdo for memory-efficient overview building."""
    func.perf_logger.start_step(f"S2 Overviews: {os.path.basename(path)}")
    print(f"Building overviews for {os.path.basename(path)} (External Process)...", flush=True)
    try:
        subprocess.run(
            [
                "gdaladdo",
                "-r",
                "average",
                "--config",
                "GDAL_NUM_THREADS",
                str(c.WORKERS),
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


def _apply_rdylgn(data: np.ndarray, vmin: float = -0.2, vmax: float = 0.5) -> List[np.ndarray]:
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


def _apply_osint_ramp(data: np.ndarray, vmin: float = -0.6, vmax: float = 0.2) -> List[np.ndarray]:
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
    visual_paths: Dict[str, str], analytic_paths: Dict[str, str], skip_overviews: bool = False
) -> None:
    """Macro-block threaded renderer for S2 indices."""
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

        def scale(band: np.ndarray, mn: float = ref_min, mx: float = ref_max, gamma: float = 2.2) -> np.ndarray:
            res = (band.astype(np.float32) - mn) / (mx - mn)
            res = np.clip(res, 0, 1)
            if gamma != 1.0:
                res = np.power(res, 1 / gamma)
            return (res * 255).astype(np.uint8)

        def process_block(window: rio.windows.Window) -> None:
            b02 = src10.read(c.BAND_BLU, window=window)
            b03 = src10.read(c.BAND_GRN, window=window)
            b04 = src10.read(c.BAND_RED, window=window)
            b08 = src10.read(c.BAND_NIR, window=window)
            b05 = src20.read(c.BAND_RE1, window=window, out_shape=b02.shape)
            b11 = src20.read(c.BAND_SW1, window=window, out_shape=b02.shape)
            b12 = src20.read(c.BAND_SW2, window=window, out_shape=b02.shape)
            alpha = np.where(b02 > 0, 255, 0).astype(np.uint8)

            s_b02 = scale(b02)
            s_b03 = scale(b03)
            s_b04 = scale(b04)
            s_b08 = scale(b08)
            s_b11 = scale(b11, mn=ref_min, mx=4000, gamma=1.5)
            s_b12 = scale(b12, mn=ref_min, mx=4000, gamma=1.5)

            if "TCI" in v_handles:
                v_handles["TCI"].write(np.stack([s_b04, s_b03, s_b02, alpha], axis=0), window=window)
            if "NIRFC" in v_handles:
                v_handles["NIRFC"].write(
                    np.stack([s_b08, s_b04, s_b03, alpha], axis=0), window=window
                )
            if "AP" in v_handles:
                v_handles["AP"].write(np.stack([s_b12, s_b11, s_b08, alpha], axis=0), window=window)

            # GPU Accelerated Indexing
            ndvi_raw = func.gpu_calc_idx(b08, b04, alpha)
            ndre_raw = func.gpu_calc_idx(b08, b05, alpha)
            ndbi_raw = func.gpu_calc_idx(b11, b08, alpha)
            nbr_raw = func.gpu_calc_idx(b08, b12, alpha)

            if "NDVI" in a_handles:
                a_handles["NDVI"].write(ndvi_raw, 1, window=window)
            if "NDRE" in a_handles:
                a_handles["NDRE"].write(ndre_raw, 1, window=window)
            if "NDBI" in a_handles:
                a_handles["NDBI"].write(ndbi_raw, 1, window=window)
            if "NBR" in a_handles:
                a_handles["NBR"].write(nbr_raw, 1, window=window)

            if "NDVI" in v_handles:
                flat_ndvi = ndvi_raw.flatten()
                r_m = np.interp(flat_ndvi, cv, cr).astype(np.uint8).reshape(b08.shape)
                g_m = np.interp(flat_ndvi, cv, cg).astype(np.uint8).reshape(b08.shape)
                b_m = np.interp(flat_ndvi, cv, cb).astype(np.uint8).reshape(b08.shape)
                v_handles["NDVI"].write(np.stack([r_m, g_m, b_m, alpha], axis=0), window=window)

            if "NDRE" in v_handles:
                res_ndre = _apply_rdylgn(ndre_raw)
                v_handles["NDRE"].write(
                    np.stack([res_ndre[0], res_ndre[1], res_ndre[2], alpha], axis=0), window=window
                )

            if "NDBI" in v_handles:
                res_ndbi = _apply_urban_heat(ndbi_raw)
                v_handles["NDBI"].write(
                    np.stack([res_ndbi[0], res_ndbi[1], res_ndbi[2], alpha], axis=0), window=window
                )

            if "NDBI_CLEAN" in v_handles:
                ndbi_clean = ndbi_raw - (ndre_raw * 0.4)
                res_ndbi_c = _apply_osint_ramp(ndbi_clean)
                v_handles["NDBI_CLEAN"].write(
                    np.stack([res_ndbi_c[0], res_ndbi_c[1], res_ndbi_c[2], alpha], axis=0),
                    window=window,
                )

            if "NBR" in v_handles:
                res_nbr = _apply_rdylgn(nbr_raw, vmin=-0.2, vmax=0.5)
                v_handles["NBR"].write(
                    np.stack([res_nbr[0], res_nbr[1], res_nbr[2], alpha], axis=0), window=window
                )

            if "CAMO" in v_handles:

                def scale_nd(val: np.ndarray) -> np.ndarray:
                    return np.clip((val + 1) / 2 * 255, 0, 255).astype(np.uint8)

                v_handles["CAMO"].write(
                    np.stack([scale_nd(ndvi_raw), scale_nd(ndre_raw), s_b03, alpha], axis=0),
                    window=window,
                )

        # Macro-block Logic
        windows: List[rio.windows.Window] = []
        for r in range(0, src10.height, c.BLOCK_SIZE):
            for col in range(0, src10.width, c.BLOCK_SIZE):
                windows.append(
                    rio.windows.Window(
                        col,
                        r,
                        min(c.BLOCK_SIZE, src10.width - col),
                        min(c.BLOCK_SIZE, src10.height - r),
                    )
                )

        if v_handles or a_handles:
            print(
                f"Rendering Visual: {list(v_handles.keys())} | "
                f"Analytic: {list(a_handles.keys())}",
                flush=True,
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.map(process_block, windows)

        # Only build finalizers for VISUAL paths
        vis_output_paths: List[str] = []
        for h in v_handles.values():
            vis_output_paths.append(h.name)
            h.close()

        # Analytic files: close handles, no overviews
        for h in a_handles.values():
            h.close()

        func.perf_logger.end_step()
        if vis_output_paths and not skip_overviews:
            for path in vis_output_paths:
                build_overviews_gdal(path)
                p_type = path.split("/")[-2].upper()
                meta.generate_sidecar(path, f"S2-{p_type}", f"S2-{p_type}")
                cog.convert_to_cog(path)
        legends.save_all_legends(c.DIRS["S1S2_LEGENDS"])
        gc.collect()


def run_pipeline(ds_obj: gdal.Dataset, processes: List[str]) -> None:
    """Entry point for S2 pipeline."""
    product_uri = gdal.Info(ds_obj, format="json")["metadata"][""]["PRODUCT_URI"]
    utm = get_utm(product_uri)
    time_str: str = str(get_time(product_uri)) + "Z"
    name = f"{utm}-{time_str}"

    v_paths: Dict[str, str] = {}
    a_paths: Dict[str, str] = {}
    for p in ["TCI", "NIRFC", "AP", "NDVI", "NDBI", "NDRE", "NBR", "CAMO", "NDBI_CLEAN"]:
        if p in processes:
            v_paths[p] = f"{c.DIRS[f'VIS_S2_{p}']}/{name}-{p}"
            if f"ANA_S2_{p}" in c.DIRS:
                a_paths[p] = f"{c.DIRS[f'ANA_S2_{p}']}/{name}-{p}"
    prepare(ds_obj)
    _render_internal(v_paths, a_paths)
    cleanup()
