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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import constants as c
import functions as func
import legends
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


# ----- Sentinel 2 helper functions ---------------------------------
def get_utm(name):
    """Gets the UTM grid from a Sentinel 2 dataset name"""
    result = re.search(r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name)
    return result.groups()[0] if result else None


def get_time(name):
    """Gets the production time from a Sentinel 2 dataset name"""
    result = re.search(r"S2._......_(\d+T\d+)_\w\d+_\w\d+_.*_\d+T\d+.SAFE", name)
    return result.groups()[0] if result else None


def prepare(ds):
    """Reprojects required Sentinel-2 bands to EPSG:3857 at 10m resolution."""
    print("Reprojecting required S2 bands to EPSG:3857 (10m aligned)...")

    sub10m = ds.GetSubDatasets()[0][0]
    sub20m = ds.GetSubDatasets()[1][0]

    cores = max(1, multiprocessing.cpu_count() // 4)
    warp_options = {
        "dstSRS": "EPSG:3857",
        "xRes": 10,
        "yRes": 10,
        "multithread": True,
        "warpMemoryLimit": 256,
        "warpOptions": [f"NUM_THREADS={cores}"],
        "creationOptions": [
            "TILED=YES",
            "BLOCKXSIZE=256",
            "BLOCKYSIZE=256",
            "COMPRESS=DEFLATE",
            "BIGTIFF=YES",
        ],
        "resampleAlg": gdal.GRA_Bilinear,
    }

    print("  Bands 2,3,4,8 (Master Extent)")
    gdal.Warp("/tmp/s2_10m.tif", sub10m, **warp_options)

    master_info = gdal.Info("/tmp/s2_10m.tif", format="json")
    bounds = master_info["cornerCoordinates"]
    out_bounds = [
        bounds["lowerLeft"][0],
        bounds["lowerLeft"][1],
        bounds["upperRight"][0],
        bounds["upperRight"][1],
    ]

    print("  Bands 5,11,12 (Aligned to 10m grid)")
    gdal.Warp("/tmp/s2_20m.tif", sub20m, outputBounds=out_bounds, **warp_options)

    gc.collect()


def cleanup():
    for f in ["/tmp/s2_10m.tif", "/tmp/s2_20m.tif"]:
        if os.path.exists(f):
            os.remove(f)


def _build_ov_task(path):
    """Helper for parallel overview building."""
    print(f"Building overviews for {os.path.basename(path)}...")
    with rio.open(path, "r+") as ds:
        ds.build_overviews([2, 4, 8, 16, 32], rio.enums.Resampling.average)
    return path


def _get_ndvi_colormap():
    """Parses data/ndvi2.txt into a format usable by np.interp."""
    vals = []
    rs = []
    gs = []
    bs = []
    colormap_path = os.path.join(c.BASE_DIR, "data/ndvi2.txt")
    with open(colormap_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if len(parts) == 4:
                vals.append(float(parts[0]))
                rs.append(int(parts[1]))
                gs.append(int(parts[2]))
                bs.append(int(parts[3]))
    return np.array(vals), np.array(rs), np.array(gs), np.array(bs)


def _apply_rdylgn(data, vmin=-0.2, vmax=0.5):
    """Applies a hardcoded RdYlGn-like colormap to normalized data."""
    nodes = [vmin, (vmin + vmax) / 2, vmax]
    rs = [165, 255, 0]
    gs = [0, 255, 104]
    bs = [38, 191, 55]
    flat = data.flatten()
    r = np.interp(flat, nodes, rs).astype(np.uint8).reshape(data.shape)
    g = np.interp(flat, nodes, gs).astype(np.uint8).reshape(data.shape)
    b = np.interp(flat, nodes, bs).astype(np.uint8).reshape(data.shape)
    return r, g, b


def _apply_urban_heat(data):
    """Applies a custom Urban Heat Map colormap to NDBI data (-1 to 1)."""
    # Stat-based Nodes for Snow/Winter conditions:
    # Most terrain is -0.7 to -0.3. Infrastructure is likely > -0.2.
    nodes = [-0.6, -0.3, -0.15, 0.1]
    rs = [20, 60, 255, 255]
    gs = [20, 60, 255, 0]
    bs = [40, 60, 0, 0]
    flat = data.flatten()
    r = np.interp(flat, nodes, rs).astype(np.uint8).reshape(data.shape)
    g = np.interp(flat, nodes, gs).astype(np.uint8).reshape(data.shape)
    b = np.interp(flat, nodes, bs).astype(np.uint8).reshape(data.shape)
    return r, g, b


def _render_internal(product_paths, skip_overviews=False):
    """Single-pass renderer for all Sentinel-2 products including OSINT indices."""
    print("Starting optimized single-pass S2 render...")
    cores_render = 2
    ref_min, ref_max = c.S2_REF_MIN, c.S2_REF_MAX
    cv, cr, cg, cb = _get_ndvi_colormap()

    with rio.open("/tmp/s2_10m.tif") as src10, rio.open("/tmp/s2_20m.tif") as src20:
        total_blocks = len(list(src10.block_windows(1)))
        profile = src10.profile.copy()
        profile.update(
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
            for i, (ji, window) in enumerate(src10.block_windows(1)):
                if i % 500 == 0:
                    print(f"  Progress: {i}/{total_blocks} blocks processed", end="\r")

                b02 = src10.read(1, window=window)
                b03 = src10.read(2, window=window)
                b04 = src10.read(3, window=window)
                b08 = src10.read(4, window=window)
                b05 = src20.read(1, window=window, out_shape=b02.shape)
                b11 = src20.read(5, window=window, out_shape=b02.shape)
                b12 = src20.read(6, window=window, out_shape=b02.shape)
                alpha = np.where(b02 > 0, 255, 0).astype(np.uint8)

                def scale(band, mn=ref_min, mx=ref_max, gamma=2.2):
                    res = (band.astype(np.float32) - mn) / (mx - mn)
                    res = np.clip(res, 0, 1)
                    if gamma != 1.0:
                        res = np.power(res, 1 / gamma)
                    return (res * 255).astype(np.uint8)

                s_b02 = scale(b02)
                s_b03 = scale(b03)
                s_b04 = scale(b04)
                s_b08 = scale(b08)
                s_b11 = scale(b11, mn=ref_min, mx=4000, gamma=1.5)
                s_b12 = scale(b12, mn=ref_min, mx=4000, gamma=1.5)

                if "TCI" in dst_handles:
                    dst_handles["TCI"].write(
                        np.stack([s_b04, s_b03, s_b02, alpha], axis=0), window=window
                    )
                if "NIRFC" in dst_handles:
                    dst_handles["NIRFC"].write(
                        np.stack([s_b08, s_b04, s_b03, alpha], axis=0), window=window
                    )
                if "AP" in dst_handles:
                    dst_handles["AP"].write(
                        np.stack([s_b12, s_b11, s_b08, alpha], axis=0), window=window
                    )

                def scale_nd(val):
                    return np.clip((val + 1) / 2 * 255, 0, 255).astype(np.uint8)

                # Pre-calculate NDVI and NDRE for sharing
                def calc_idx(ba, bb):
                    denom = ba.astype(float) + bb.astype(float)
                    idx = np.full_like(ba, -1.0, dtype=np.float32)
                    m = (denom != 0) & (alpha > 0)
                    idx[m] = (ba[m].astype(float) - bb[m].astype(float)) / denom[m]
                    return idx

                ndvi_raw = calc_idx(b08, b04)
                ndre_raw = calc_idx(b08, b05)

                if "NDVI" in dst_handles:
                    flat_ndvi = ndvi_raw.flatten()
                    r_m = (
                        np.interp(flat_ndvi, cv, cr).astype(np.uint8).reshape(b08.shape)
                    )
                    g_m = (
                        np.interp(flat_ndvi, cv, cg).astype(np.uint8).reshape(b08.shape)
                    )
                    b_m = (
                        np.interp(flat_ndvi, cv, cb).astype(np.uint8).reshape(b08.shape)
                    )
                    dst_handles["NDVI"].write(
                        np.stack([r_m, g_m, b_m, alpha], axis=0), window=window
                    )

                if "NDRE" in dst_handles:
                    r, g, b = _apply_rdylgn(ndre_raw)
                    dst_handles["NDRE"].write(
                        np.stack([r, g, b, alpha], axis=0), window=window
                    )

                if "NDBI" in dst_handles:
                    idx = calc_idx(b11, b08)
                    r, g, b = _apply_urban_heat(idx)
                    dst_handles["NDBI"].write(
                        np.stack([r, g, b, alpha], axis=0), window=window
                    )

                if "NBR" in dst_handles:
                    idx = calc_idx(b08, b12)
                    r, g, b = _apply_rdylgn(idx, vmin=-0.2, vmax=0.5)
                    dst_handles["NBR"].write(
                        np.stack([r, g, b, alpha], axis=0), window=window
                    )

                if "CAMO" in dst_handles:
                    # CAMO Composite: R=NDVI, G=NDRE, B=TCI-Green
                    # All are scaled 0-255
                    s_ndvi = scale_nd(ndvi_raw)
                    s_ndre = scale_nd(ndre_raw)
                    dst_handles["CAMO"].write(
                        np.stack([s_ndvi, s_ndre, s_b03, alpha], axis=0), window=window
                    )

            print(f"\nRender complete.")

        finally:
            output_paths = []
            for h in dst_handles.values():
                output_paths.append(h.name)
                h.close()
            if output_paths and not skip_overviews:
                print(f"Building overviews in parallel (throttled to 2 workers)...")
                with ProcessPoolExecutor(max_workers=2) as executor:
                    list(executor.map(_build_ov_task, output_paths))
            
            # Update HTML legends for frontend consumption
            legends.save_all_legends(os.path.join(c.DIRS['OUT'], "scripts"))
            
            gc.collect()


def runPipeline(ds, processes):
    """Runs the Sentinel 2 pipeline"""
    productURI = gdal.Info(ds, format="json")["metadata"][""]["PRODUCT_URI"]
    utm = get_utm(productURI)
    time_str = get_time(productURI) + "Z"
    name = f"{utm}-{time_str}"

    product_paths = {}
    for p in ["TCI", "NIRFC", "AP", "NDVI", "NDBI", "NDRE", "NBR", "CAMO"]:
        if p in processes:
            product_paths[p] = f"{c.DIRS[f'S2_{p}']}/{name}-{p}"

    all_exist = all(func.outputExists(p) for p in product_paths.values())
    if all_exist:
        print(f"  All S2 products for {name} already exist. Skipping heavy processing.")
        return

    prepare(ds)
    _render_internal(product_paths)
    cleanup()


def render(base_name, processes, skip_overviews=False):
    """Helper for test script."""
    product_paths = {}
    for p in processes:
        product_paths[p] = f"{base_name}-{p}"
    _render_internal(product_paths, skip_overviews=skip_overviews)
