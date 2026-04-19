#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# gpu_warp.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
CUDA-accelerated GeoTIFF warping module.
Handles coordinate remapping and bilinear interpolation on the GPU.
Refined alpha logic to remove black border stripes using value-based masking.
"""

import os
import threading
from typing import Tuple, Any

import cupy as cp
import numpy as np
import rasterio as rio
from cupyx.scipy.ndimage import map_coordinates
from osgeo import gdal, osr

import functions as func

# --- CUDA Autodetection ---
try:
    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False

# Global lock to prevent multiple concurrent GPU warps (VRAM management)
gpu_lock = threading.Lock()


def _create_remap_grid(
    src_path: str, dst_crs: str, resolution: int
) -> Tuple[str, Any, int, int]:
    """Uses GDAL to calculate the target geometry and coordinate remap grid."""
    grid_path = src_path + ".grid.tif"

    with rio.open(src_path) as src:
        # Create target SRS
        dst_srs = osr.SpatialReference()
        dst_srs.SetFromUserInput(dst_crs)

        # Calculate target dimensions and transform
        vrt = gdal.AutoCreateWarpedVRT(
            gdal.Open(src_path), None, dst_srs.ExportToWkt(), gdal.GRA_Bilinear
        )
        dst_w = vrt.RasterXSize
        dst_h = vrt.RasterYSize
        # Force exact resolution
        geo_t = list(vrt.GetGeoTransform())
        geo_t[1] = resolution
        geo_t[5] = -resolution
        dst_transform = rio.transform.from_gdal(*geo_t)

        # Create coordinate grid TIFF (Band 1: X, Band 2: Y in source pixel space)
        # We use standard gdal.Warp to project a coordinate grid
        # This is the "secret sauce" for the hybrid approach
        print(f"Creating Remap Grid ({dst_w}x{dst_h})...", flush=True)
        gdal.Grid(
            grid_path,
            src_path,
            format="GTiff",
            algorithm="linear",
            zfield="pixel",
            outputBounds=[
                geo_t[0],
                geo_t[3] + geo_t[5] * dst_h,
                geo_t[0] + geo_t[1] * dst_w,
                geo_t[3],
            ],
            width=dst_w,
            height=dst_h,
        )
        # Note: In production, we'd use a more direct method to get src_x/src_y for every dst_x/dst_y
        # but AutoCreateWarpedVRT is the most reliable for GCP-based S1 products.
        # For simplicity in this overdrive refactor, we use a small trick:
        # AutoCreateWarpedVRT + a custom transformer.
        return grid_path, dst_transform, dst_w, dst_h


def reproject_with_cuda(
    src_path: str,
    dst_path: str,
    dst_crs: str = "EPSG:3857",
    resolution: int = 10,
    block_size: int = 1024,
    src_nodata: float = 0,
    dst_alpha: bool = False,
) -> None:
    """
    Warps a dataset using CUDA streams for interpolation.
    Value-based alpha masking ensures no black border stripes.
    """
    if not HAS_CUDA:
        warp_opt = gdal.WarpOptions(
            dstSRS=dst_crs,
            xRes=resolution,
            yRes=resolution,
            multithread=True,
            dstAlpha=dst_alpha,
            srcNodata=src_nodata,
        )
        gdal.Warp(dst_path, src_path, options=warp_opt)
        return

    func.perf_logger.start_step(f"GPU Warp: {os.path.basename(src_path)}", use_gpu=True)

    # We use gdal.Warp to create a VRT first to get the transformer
    # Then we pull the transformer into Python to build our GPU grid
    with gpu_lock:
        try:
            # 1. Setup Transformer
            src_ds = gdal.Open(src_path)
            dst_srs = osr.SpatialReference()
            dst_srs.SetFromUserInput(dst_crs)

            vrt = gdal.AutoCreateWarpedVRT(
                src_ds, None, dst_srs.ExportToWkt(), gdal.GRA_Bilinear
            )
            dst_w, dst_h = vrt.RasterXSize, vrt.RasterYSize
            v_geo = vrt.GetGeoTransform()
            dst_transform = rio.transform.from_gdal(*v_geo)

            # 2. Open source and prepare output
            with rio.open(src_path) as src:
                profile = src.profile.copy()
                out_count = src.count + 1 if dst_alpha else src.count
                profile.update(
                    {
                        "crs": dst_crs,
                        "transform": dst_transform,
                        "width": dst_w,
                        "height": dst_h,
                        "compress": "DEFLATE",
                        "tiled": True,
                        "blockxsize": 256,
                        "blockysize": 256,
                        "nodata": 0,
                        "count": out_count,
                        "BIGTIFF": "YES",
                    }
                )

                m_pool = cp.get_default_memory_pool()

                # Transformer for coordinate mapping
                # We do this block-by-block to save VRAM
                with rio.open(dst_path, "w", **profile) as dst:
                    for r in range(0, dst_h, block_size):
                        rows = min(block_size, dst_h - r)
                        for c_off in range(0, dst_w, block_size):
                            cols = min(block_size, dst_w - c_off)
                            win = rio.windows.Window(c_off, r, cols, rows)

                            # Create Destination Pixel Grid
                            # meshgrid on CPU is fine for small blocks
                            y, x = np.mgrid[r : r + rows, c_off : c_off + cols]
                            # Map dst pixels to dst coords
                            dst_x, dst_y = dst_transform * (x, y)

                            # Use GDAL Transformer to map dst coords -> src pixels
                            # transformer.TransformPoints is vectorized
                            # We need to use gdal.Transformer
                            transformer = gdal.Transformer(vrt, None, [])
                            _, points = transformer.TransformPoints(
                                1,
                                list(
                                    zip(
                                        dst_x.flatten(),
                                        dst_y.flatten(),
                                        [0] * len(dst_x.flatten()),
                                    )
                                ),
                            )

                            # points is (src_x, src_y, src_z)
                            src_c = np.array([p[0] for p in points]).reshape(
                                (rows, cols)
                            )
                            src_r = np.array([p[1] for p in points]).reshape(
                                (rows, cols)
                            )

                            # Validity mask: Inside src dimensions AND NOT on black padding
                            valid_coords = (
                                (src_c >= 0)
                                & (src_r >= 0)
                                & (src_c < src.width)
                                & (src_r < src.height)
                            )

                            if not np.any(valid_coords):
                                for b in range(1, out_count + 1):
                                    dst.write(
                                        np.zeros((rows, cols), dtype=profile["dtype"]),
                                        b,
                                        window=win,
                                    )
                                continue

                            # VRAM Optimization: Load only required source chunk
                            s_c_min, s_c_max = int(np.min(src_c[valid_coords])), int(
                                np.max(src_c[valid_coords])
                            )
                            s_r_min, s_r_max = int(np.min(src_r[valid_coords])), int(
                                np.max(src_r[valid_coords])
                            )

                            # Add padding for bilinear interp
                            s_c_off, s_r_off = max(0, s_c_min - 2), max(0, s_r_min - 2)
                            s_w, s_h = min(
                                src.width - s_c_off, s_c_max + 2 - s_c_off
                            ), min(src.height - s_r_off, s_r_max + 2 - s_r_off)

                            if s_w <= 0 or s_h <= 0:
                                continue

                            gpu_coords = cp.array([src_r - s_r_off, src_c - s_c_off])
                            if dst_alpha:
                                # Start with absolute geometry mask
                                # Using the binary valid_coords mask ensures no 1px interpolation artifacts at edges
                                g_alpha = cp.array(valid_coords, dtype=cp.uint8)

                            # Warp original bands
                            for b in range(1, src.count + 1):
                                data = src.read(
                                    b,
                                    window=rio.windows.Window(
                                        s_c_off, s_r_off, s_w, s_h
                                    ),
                                )
                                gpu_data = cp.array(data, dtype=cp.float32)

                                g_warped = map_coordinates(
                                    gpu_data,
                                    gpu_coords,
                                    order=1,
                                    mode="constant",
                                    cval=0,
                                    prefilter=False,
                                )

                                # Refine alpha: only contribute if warped pixel is actually > threshold
                                # This helps with source data that might have its own artifacts
                                if dst_alpha:
                                    g_alpha &= g_warped > 1e-7

                                dst.write(
                                    cp.asnumpy(g_warped).astype(profile["dtype"]),
                                    b,
                                    window=win,
                                )
                                del gpu_data, g_warped

                            if dst_alpha:
                                dst.write(
                                    cp.asnumpy(g_alpha * 255).astype(np.uint8),
                                    out_count,
                                    window=win,
                                )
                                del g_alpha

                            del gpu_coords
                            m_pool.free_all_blocks()

                        print(
                            f"Warp Progress: {int((r+rows)/dst_h * 100)}%",
                            end="\r",
                            flush=True,
                        )
            print("\nWarp Complete.", flush=True)
        finally:
            src_ds = None
            vrt = None
            func.perf_logger.end_step()


if __name__ == "__main__":
    pass
