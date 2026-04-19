#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# correlate.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Fusion engine for Sentinel-1 and Sentinel-2 spatial correlation.
Handles sensor fusion products like RADAR-BURN and TARGET-PROBE-V2.
"""

import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import rasterio as rio
from rasterio.features import rasterize
from rasterio.warp import transform_geom
from rasterio.windows import Window, from_bounds
from shapely.geometry import mapping, shape
from shapely.wkt import loads

import cog_finalizer as cog
import constants as c
import functions as func
import legends
import metadata_engine as meta

# --- CUDA Autodetection ---
try:
    import cupy as cp

    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False


def build_overviews_gdal(path: str) -> None:
    """Uses gdaladdo for memory-efficient overview building."""
    func.perf_logger.start_step(f"Fusion Overviews: {os.path.basename(path)}")
    print(
        f"Building overviews for {os.path.basename(path)} (External Process)...",
        flush=True,
    )
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
            ],
            check=True,
            capture_output=True,
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Warning: gdaladdo failed for {path}: {e}", flush=True)
    func.perf_logger.end_step()


def turbo_colormap(x_arr: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Linear interpolation for a Turbo-like ramp."""
    r_c = np.clip(
        np.where(x_arr < 0.5, 0, np.where(x_arr < 0.75, (x_arr - 0.5) / 0.25, 1.0)),
        0,
        1,
    )
    g_c = np.clip(
        np.where(
            x_arr < 0.25,
            x_arr / 0.25,
            np.where(x_arr < 0.75, 1.0, 1.0 - (x_arr - 0.75) / 0.25),
        ),
        0,
        1,
    )
    b_c = np.clip(
        np.where(
            x_arr < 0.25, 1.0, np.where(x_arr < 0.5, 1.0 - (x_arr - 0.25) / 0.25, 0)
        ),
        0,
        1,
    )
    return (
        (r_c * 255).astype(np.uint8),
        (g_c * 255).astype(np.uint8),
        (b_c * 255).astype(np.uint8),
    )


def osint_ramp_colormap(x_arr: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Safety Green (0) -> Electric Cyan (0.5) -> Magma Red (1.0)"""
    if HAS_CUDA:
        x_g = cp.array(x_arr)
        r_g = cp.where(x_g < 0.5, 0, (x_g - 0.5) * 2 * 255)
        g_g = cp.where(x_g < 0.5, 200 + (x_g * 2) * 55, 255 - (x_g - 0.5) * 2 * 255)
        b_g = cp.where(x_g < 0.5, x_g * 2 * 255, 255 - (x_g - 0.5) * 2 * 255)
        res = (
            cp.asnumpy(r_g).astype(np.uint8),
            cp.asnumpy(g_g).astype(np.uint8),
            cp.asnumpy(b_g).astype(np.uint8),
        )
        del x_g, r_g, g_g, b_g
        return res

    r_c = np.where(x_arr < 0.5, 0, (x_arr - 0.5) * 2 * 255)
    g_c = np.where(x_arr < 0.5, 200 + (x_arr * 2) * 55, 255 - (x_arr - 0.5) * 2 * 255)
    b_c = np.where(x_arr < 0.5, x_arr * 2 * 255, 255 - (x_arr - 0.5) * 2 * 255)
    return r_c.astype(np.uint8), g_c.astype(np.uint8), b_c.astype(np.uint8)


def load_log(sat: str) -> Optional[Dict[str, Any]]:
    """Loads the last search log for a satellite."""
    logfile = os.path.join(c.DIRS["DL"], f"{sat}_last.json")
    if os.path.exists(logfile):
        with open(logfile, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def find_overlaps(max_hours: int = 24) -> List[Dict[str, Any]]:
    """Identifies temporal and spatial overlaps between S1 and S2."""
    s1_log: Optional[Dict[str, Any]] = load_log("s1")
    s2_log: Optional[Dict[str, Any]] = load_log("s2")
    if not s1_log or not s2_log:
        return []

    matches = []
    for s2_feat in s2_log["files"]:
        s2_props = s2_feat["properties"]
        if not s2_props.get("footprint"):
            continue
        s2_time = datetime.fromisoformat(s2_props["startDate"].replace("Z", "+00:00"))
        s2_geom = loads(s2_props["footprint"])

        for s1_feat in s1_log["files"]:
            s1_props = s1_feat["properties"]
            if not s1_props.get("footprint"):
                continue
            s1_time = datetime.fromisoformat(
                s1_props["startDate"].replace("Z", "+00:00")
            )
            s1_geom = loads(s1_props["footprint"])

            if abs(s1_time - s2_time) < timedelta(hours=max_hours):
                if s2_geom.intersects(s1_geom):
                    inter_geom = s2_geom.intersection(s1_geom)
                    matches.append(
                        {"s1": s1_feat, "s2": s2_feat, "inter_geom": inter_geom}
                    )
    return matches


def get_processed_paths(
    s1_feat: Dict[str, Any], s2_feat: Dict[str, Any]
) -> Tuple[str, str, str, str, str, str]:
    """Resolves file paths for processed analytic and visual products."""
    s1_title = s1_feat["properties"]["title"]
    s1_base = os.path.basename(s1_title)
    s1_name = f"S1_{s1_base.split('_')[4]}_{s1_base.split('_')[5]}"
    vh_ana_path = os.path.join(c.DIRS["ANA_S1_VH"], f"{s1_name}.tif")

    s2_props = s2_feat["properties"]
    utm = s2_props["title"].split("_")[5]
    raw_start = s2_props["startDate"]
    s2_time = raw_start.split(".")[0].replace("-", "").replace(":", "")
    if not s2_time.endswith("Z"):
        s2_time += "Z"
    s2_name = f"{utm}-{s2_time}"

    tci_vis_path = os.path.join(c.DIRS["VIS_S2_TCI"], f"{s2_name}-TCI.tif")
    nirfc_vis_path = os.path.join(c.DIRS["VIS_S2_NIRFC"], f"{s2_name}-NIRFC.tif")
    ndbi_ana_path = os.path.join(c.DIRS["ANA_S2_NDBI"], f"{s2_name}-NDBI.tif")
    ndre_ana_path = os.path.join(c.DIRS["ANA_S2_NDRE"], f"{s2_name}-NDRE.tif")

    return (
        vh_ana_path,
        tci_vis_path,
        s2_name,
        ndbi_ana_path,
        ndre_ana_path,
        nirfc_vis_path,
    )


def calculate_tight_window(
    inter_geom_4326: Any, s2_src: rio.DatasetReader
) -> Tuple[int, int, Any, Window, Any]:
    """Calculates clipped bounds with an internal buffer using precise pixel coordinates."""
    geom_3857 = transform_geom("EPSG:4326", s2_src.crs, mapping(inter_geom_4326))
    inter_shape = shape(geom_3857).buffer(-500)

    # Get Window directly from source to avoid rounding artifacts
    win = s2_src.window(*inter_shape.bounds).round_offsets().round_lengths()

    # Extract properties from the finalized window
    out_w = int(win.width)
    out_h = int(win.height)
    out_transform = s2_src.window_transform(win)

    return out_w, out_h, out_transform, win, inter_shape


def fuse_radar_optical(
    vh_path: str,
    tci_path: str,
    out_name: str,
    inter_geom: Any,
    threshold: float = -15.0,
) -> bool:
    """Fuses S1-VH detections over S2-TCI background."""
    if not os.path.exists(vh_path) or not os.path.exists(tci_path):
        missing = []
        if not os.path.exists(vh_path): missing.append(os.path.basename(vh_path))
        if not os.path.exists(tci_path): missing.append(os.path.basename(tci_path))
        print(f"Skipping RADAR-BURN for {out_name}: Missing {', '.join(missing)}", flush=True)
        return False
        
    out_path = os.path.join(c.DIRS["VIS_FUSED"], f"{out_name}-RADAR-BURN.tif")
    if func.output_exists(out_path.replace(".tif", "")):
        return False

    func.perf_logger.start_step(f"Fusion: {out_name} RADAR-BURN", use_gpu=True)
    try:
        with rio.open(tci_path) as s2_src, rio.open(vh_path) as s1_src:
            out_w, out_h, out_transform, s2_win, inter_poly = calculate_tight_window(
                inter_geom, s2_src
            )
            print(f"Fusing Target Probe (Ghost Blend: {out_w}x{out_h})...", flush=True)

            geom_mask = rasterize(
                [inter_poly],
                out_shape=(out_h, out_w),
                transform=out_transform,
                fill=0,
                default_value=255,
                dtype=np.uint8,
            )
            profile = s2_src.profile.copy()
            profile.update(
                width=out_w,
                height=out_h,
                transform=out_transform,
                compress="DEFLATE",
                tiled=True,
            )

            out_path = os.path.join(c.DIRS["VIS_FUSED"], f"{out_name}-RADAR-BURN.tif")
            with rio.open(out_path, "w", **profile) as dst:
                # Map S2 window to S1 space
                win_bounds = s2_src.window_bounds(s2_win)
                s1_win = s1_src.window(*win_bounds)  # pylint: disable=unused-variable

                for _, window in dst.block_windows(1):
                    # Calculate sub-windows for reading
                    dst_bounds = dst.window_bounds(window)
                    s2_sub_win = s2_src.window(*dst_bounds)
                    s1_sub_win = s1_src.window(*dst_bounds)

                    s2_win_data = s2_src.read(
                        [1, 2, 3, 4],
                        window=s2_sub_win,
                        out_shape=(4, window.height, window.width),
                    )
                    s1_vh_data = s1_src.read(
                        1, window=s1_sub_win, out_shape=(window.height, window.width)
                    )

                    if HAS_CUDA:
                        vh_g = cp.array(s1_vh_data, dtype=cp.float32)
                        vh_db_g = 10 * cp.log10(cp.maximum(vh_g, 1e-9))
                        vh_db = cp.asnumpy(vh_db_g)
                        del vh_g, vh_db_g
                    else:
                        vh_db = 10 * np.log10(np.maximum(s1_vh_data, 1e-9))

                    win_geom_mask = geom_mask[
                        window.row_off : window.row_off + window.height,
                        window.col_off : window.col_off + window.width,
                    ]

                    target_mask = (vh_db > threshold) & (s2_win_data[3] > 0)
                    if np.any(target_mask):
                        turbo_x = np.clip(
                            (vh_db[target_mask] - threshold) / abs(threshold), 0, 1
                        )
                        tr, tg, tb = turbo_colormap(turbo_x)
                        s2_win_data[0][target_mask] = np.clip(
                            tr * 0.4 + s2_win_data[0][target_mask] * 0.6, 0, 255
                        ).astype(np.uint8)
                        s2_win_data[1][target_mask] = np.clip(
                            tg * 0.4 + s2_win_data[1][target_mask] * 0.6, 0, 255
                        ).astype(np.uint8)
                        s2_win_data[2][target_mask] = np.clip(
                            tb * 0.4 + s2_win_data[2][target_mask] * 0.6, 0, 255
                        ).astype(np.uint8)

                    mask_norm = (s2_win_data[3].astype(float) / 255.0) * (
                        win_geom_mask.astype(float) / 255.0
                    )
                    s2_win_data[3] = (mask_norm * 255).astype(np.uint8)
                    for b in range(3):
                        s2_win_data[b] = (s2_win_data[b].astype(float) * mask_norm).astype(
                            np.uint8
                        )
                    dst.write(s2_win_data, window=window)

        func.perf_logger.end_step()
        build_overviews_gdal(out_path)
        meta.generate_sidecar(out_path, "FUSED-RADAR-BURN", "RADAR-BURN", effective_res=10.0)
        cog.convert_to_cog(out_path)
        return True
    except Exception as e:
        print(f"Error creating RADAR-BURN for {out_name}: {e}", flush=True)
        return False


# pylint: disable=too-many-locals
def fuse_target_probe_v2(
    vh_path: str,
    ndbi_path: str,
    ndre_path: str,
    tci_path: str,
    out_name: str,
    inter_geom: Any,
) -> bool:
    """Advanced Target Probe using NDBI-NDRE gated by S1-VH."""
    paths = {"VH": vh_path, "NDBI": ndbi_path, "NDRE": ndre_path, "TCI": tci_path}
    missing = [name for name, p in paths.items() if not os.path.exists(p)]
    if missing:
        print(f"Skipping TARGET-PROBE-V2 for {out_name}: Missing {', '.join(missing)} products", flush=True)
        return False
        
    out_path = os.path.join(c.DIRS["VIS_FUSED"], f"{out_name}-TARGET-PROBE-V2.tif")
    if func.output_exists(out_path.replace(".tif", "")):
        return False

    func.perf_logger.start_step(f"Fusion: {out_name} TARGET-PROBE-V2", use_gpu=True)
    try:
        with rio.open(tci_path) as tci_src, rio.open(vh_path) as vh_src, rio.open(
            ndbi_path
        ) as ndbi_src, rio.open(ndre_path) as ndre_src:
            out_w, out_h, out_transform, s2_win, inter_poly = (
                calculate_tight_window(  # pylint: disable=unused-variable
                    inter_geom, tci_src
                )
            )
            print(
                f"Creating Advanced Target Probe (Ghost Blend: {out_w}x{out_h})...",
                flush=True,
            )

            geom_mask = rasterize(
                [inter_poly],
                out_shape=(out_h, out_w),
                transform=out_transform,
                fill=0,
                default_value=255,
                dtype=np.uint8,
            )
            profile = tci_src.profile.copy()
            profile.update(
                width=out_w,
                height=out_h,
                transform=out_transform,
                count=4,
                compress="DEFLATE",
                tiled=True,
            )

            out_path = os.path.join(c.DIRS["VIS_FUSED"], f"{out_name}-TARGET-PROBE-V2.tif")
            with rio.open(out_path, "w", **profile) as dst:
                for _, window in dst.block_windows(1):
                    dst_bounds = dst.window_bounds(window)
                    tci_data = tci_src.read(
                        [1, 2, 3, 4],
                        window=tci_src.window(*dst_bounds),
                        out_shape=(4, window.height, window.width),
                    )
                    vh_data = vh_src.read(
                        1,
                        window=vh_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    )
                    ndbi_data = ndbi_src.read(
                        1,
                        window=ndbi_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    )
                    ndre_data = ndre_src.read(
                        1,
                        window=ndre_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    )

                    if HAS_CUDA:
                        v_g = cp.array(vh_data)
                        nbi_g = cp.array(ndbi_data)
                        nre_g = cp.array(ndre_data)
                        vh_db_g = 10 * cp.log10(cp.maximum(v_g, 1e-9))
                        ndbi_clean_g = nbi_g - (nre_g * 0.4)
                        vh_gate_g = vh_db_g > -15
                        x_g = cp.clip((ndbi_clean_g + 0.6) / 0.8, 0, 1)
                        x_g[vh_gate_g] = cp.clip(x_g[vh_gate_g] * 1.4, 0, 1)
                        vh_db = cp.asnumpy(vh_db_g)
                        ndbi_clean = cp.asnumpy(ndbi_clean_g)
                        vh_gate = cp.asnumpy(vh_gate_g)
                        x_val = cp.asnumpy(x_g)
                        del v_g, nbi_g, nre_g, vh_db_g, ndbi_clean_g, vh_gate_g, x_g
                    else:
                        vh_db = 10 * np.log10(np.maximum(vh_data, 1e-9))
                        ndbi_clean = ndbi_data - (ndre_data * 0.4)
                        vh_gate = vh_db > -15
                        x_val = np.clip((ndbi_clean + 0.6) / 0.8, 0, 1)
                        x_val[vh_gate] = np.clip(x_val[vh_gate] * 1.4, 0, 1)

                    tr, tg, tb = osint_ramp_colormap(x_val)
                    ghost_mask = np.clip((ndbi_clean + 0.1) / 0.2, 0, 1)
                    ghost_mask[vh_gate] = np.maximum(ghost_mask[vh_gate], 0.5)

                    win_geom_mask = geom_mask[
                        window.row_off : window.row_off + window.height,
                        window.col_off : window.col_off + window.width,
                    ]
                    final_alpha_norm = (tci_data[3].astype(float) / 255.0) * (
                        win_geom_mask.astype(float) / 255.0
                    )

                    for b, col in enumerate([tr, tg, tb]):
                        tci_data[b] = (
                            col.astype(float) * ghost_mask
                            + tci_data[b].astype(float) * (1 - ghost_mask)
                        ).astype(np.uint8)

                    tci_data[3] = (final_alpha_norm * 255).astype(np.uint8)
                    for b in range(3):
                        tci_data[b] = (tci_data[b].astype(float) * final_alpha_norm).astype(
                            np.uint8
                        )
                    dst.write(tci_data, window=window)

            func.perf_logger.end_step()
            build_overviews_gdal(out_path)
            meta.generate_sidecar(out_path, "FUSED-TARGET-PROBE-V2", "TARGET-PROBE-V2", effective_res=10.0)
            cog.convert_to_cog(out_path)
            return True
    except Exception as e:
        print(f"Error creating TARGET-PROBE-V2 for {out_name}: {e}", flush=True)
        return False


# pylint: disable=too-many-locals
def fuse_life_machine(
    vh_path: str, tci_path: str, nirfc_path: str, out_name: str, inter_geom: Any
) -> bool:
    """Life vs Machine discovery composite."""
    paths = {"VH": vh_path, "TCI": tci_path, "NIRFC": nirfc_path}
    missing = [name for name, p in paths.items() if not os.path.exists(p)]
    if missing:
        print(f"Skipping LIFE-MACHINE for {out_name}: Missing {', '.join(missing)} products", flush=True)
        return False
        
    out_path = os.path.join(c.DIRS["VIS_FUSED"], f"{out_name}-LIFE-MACHINE.tif")
    if func.output_exists(out_path.replace(".tif", "")):
        return False

    func.perf_logger.start_step(f"Fusion: {out_name} LIFE-MACHINE", use_gpu=True)
    try:
        with rio.open(tci_path) as tci_src, rio.open(vh_path) as vh_src, rio.open(
            nirfc_path
        ) as nirfc_src:
            out_w, out_h, out_transform, s2_win, inter_poly = (
                calculate_tight_window(  # pylint: disable=unused-variable
                    inter_geom, tci_src
                )
            )
            print(
                f"Creating Discovery Composite (Functional Mapping: {out_w}x{out_h})...",
                flush=True,
            )

            geom_mask = rasterize(
                [inter_poly],
                out_shape=(out_h, out_w),
                transform=out_transform,
                fill=0,
                default_value=255,
                dtype=np.uint8,
            )
            profile = tci_src.profile.copy()
            profile.update(
                width=out_w,
                height=out_h,
                transform=out_transform,
                count=4,
                compress="DEFLATE",
                tiled=True,
            )

            out_path = os.path.join(c.DIRS["VIS_FUSED"], f"{out_name}-LIFE-MACHINE.tif")
            with rio.open(out_path, "w", **profile) as dst:
                for _, window in dst.block_windows(1):
                    dst_bounds = dst.window_bounds(window)
                    nir = nirfc_src.read(
                        1,
                        window=nirfc_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    ).astype(float)
                    red_band = tci_src.read(
                        1,
                        window=tci_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    ).astype(float)
                    blue_band = tci_src.read(
                        3,
                        window=tci_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    ).astype(float)
                    vh_data = vh_src.read(
                        1,
                        window=vh_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    ).astype(float)

                    if HAS_CUDA:
                        vh_g = cp.array(vh_data)
                        vh_db_g = 10 * cp.log10(cp.maximum(vh_g, 1e-9))
                        vh_vis_g = cp.clip(
                            (vh_db_g - c.S1_DB_MIN) / (c.S1_DB_MAX - c.S1_DB_MIN) * 255,
                            0,
                            255,
                        )
                        vh_vis = cp.asnumpy(vh_vis_g)
                        del vh_g, vh_db_g, vh_vis_g
                    else:
                        vh_db = 10 * np.log10(np.maximum(vh_data, 1e-9))
                        vh_vis = np.clip(
                            (vh_db - c.S1_DB_MIN) / (c.S1_DB_MAX - c.S1_DB_MIN) * 255,
                            0,
                            255,
                        )

                    vh_boosted = np.clip(vh_vis * 1.5, 0, 255).astype(np.uint8)
                    denom = nir + red_band
                    ndvi = np.zeros_like(nir)
                    m = denom != 0
                    ndvi[m] = (nir[m] - red_band[m]) / denom[m]
                    ndvi_scaled = np.clip((ndvi - 0.0) / 0.8 * 255, 0, 255).astype(np.uint8)
                    context_blue = np.clip(blue_band * 1.2 + 20, 0, 255).astype(np.uint8)

                    win_geom_mask = geom_mask[
                        window.row_off : window.row_off + window.height,
                        window.col_off : window.col_off + window.width,
                    ]
                    tci_alpha = tci_src.read(
                        4,
                        window=tci_src.window(*dst_bounds),
                        out_shape=(window.height, window.width),
                    )
                    alpha_norm = (tci_alpha.astype(float) / 255.0) * (
                        win_geom_mask.astype(float) / 255.0
                    )

                    alpha_final = (alpha_norm * 255).astype(np.uint8)
                    vh_boosted = (vh_boosted.astype(float) * alpha_norm).astype(np.uint8)
                    ndvi_scaled = (ndvi_scaled.astype(float) * alpha_norm).astype(np.uint8)
                    context_blue = (context_blue.astype(float) * alpha_norm).astype(
                        np.uint8
                    )
                    dst.write(
                        np.stack(
                            [vh_boosted, ndvi_scaled, context_blue, alpha_final], axis=0
                        ),
                        window=window,
                    )

            func.perf_logger.end_step()
            build_overviews_gdal(out_path)
            meta.generate_sidecar(out_path, "FUSED-LIFE-MACHINE", "LIFE-MACHINE", effective_res=10.0)
            cog.convert_to_cog(out_path)
            return True
    except Exception as e:
        print(f"Error creating LIFE-MACHINE for {out_name}: {e}", flush=True)
        return False


def run_correlation(fusion_processes: List[str] = ["RADAR-BURN", "LIFE-MACHINE", "TARGET-PROBE-V2"]) -> int:
    """Main entry point for S1/S2 correlation. Returns count of created fusion products."""
    matches = find_overlaps()
    if not matches:
        return 0
        
    created_count = 0
    print(f"Found {len(matches)} potential S1/S2 matches.", flush=True)
    for match in matches:
        vh_ana, tci_vis, s2_name, ndbi_ana, ndre_ana, nirfc_vis = get_processed_paths(
            match["s1"], match["s2"]
        )
        
        if "RADAR-BURN" in fusion_processes:
            if fuse_radar_optical(vh_ana, tci_vis, s2_name, match["inter_geom"]):
                created_count += 1
        
        if "LIFE-MACHINE" in fusion_processes:
            if fuse_life_machine(vh_ana, tci_vis, nirfc_vis, s2_name, match["inter_geom"]):
                created_count += 1
            
        if "TARGET-PROBE-V2" in fusion_processes:
            if fuse_target_probe_v2(
                vh_ana, ndbi_ana, ndre_ana, tci_vis, s2_name, match["inter_geom"]
            ):
                created_count += 1

    legends.save_all_legends(c.DIRS["S1S2_LEGENDS"])
    return created_count


if __name__ == "__main__":
    run_correlation()
