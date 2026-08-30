#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions_s1_delta.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
S1-S1 Delta SAR change detection per ROI.
Generic: enabled via "DELTA" in roi_config.json products[] (any ROI).
Pilot: Kronstadt (only ROI with DELTA initially).
Orbit-matched only (RELATIVE_ORBIT_NUMBER + PASS_DIRECTION), VH-masked, GPU-accelerated.
"""

import gc
import json
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import rasterio as rio
from osgeo import gdal

import cog_finalizer as cog
import constants as c
import functions as func
import metadata_engine as meta
import roi_manager as rm

gdal.UseExceptions()

# --- CUDA detection (same pattern as functions_s1.py) ---
try:
    import importlib.util

    HAS_CUPY_INSTALLED = importlib.util.find_spec("cupy") is not None
    HAS_CUDA: bool = HAS_CUPY_INSTALLED and os.getenv(
        "DISABLE_GPU", "false"
    ).lower() not in ("true", "1")
    if HAS_CUDA:
        import cupy as cp  # type: ignore
except ImportError:
    HAS_CUDA = False

_DELTA_PALETTES = {
    "turbo": {
        "values": np.array([-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]),
        "r": np.array([48, 53, 52, 32, 30, 100, 190, 237, 122]),
        "g": np.array([18, 98, 166, 208, 231, 236, 191, 96, 8]),
        "b": np.array([59, 216, 249, 199, 120, 42, 14, 3, 3]),
    },
    "viridis": {
        "values": np.array([-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]),
        "r": np.array([68, 72, 62, 49, 38, 53, 110, 180, 253]),
        "g": np.array([1, 39, 73, 104, 130, 179, 186, 209, 231]),
        "b": np.array([84, 120, 137, 142, 137, 97, 53, 16, 37]),
    },
    "rdylgn": {
        "values": np.array([-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]),
        "r": np.array([165, 215, 244, 253, 255, 217, 166, 102, 0]),
        "g": np.array([0, 48, 109, 174, 255, 239, 217, 189, 104]),
        "b": np.array([38, 39, 67, 97, 191, 139, 109, 99, 55]),
    },
}


def _roi_wants_delta(roi: Dict[str, Any]) -> bool:
    """True if ROI products[] contains DELTA / S1-DELTA (generic)."""
    products = roi.get("products", [])
    norm = {str(p).upper().replace("-", "_") for p in products}
    return bool(norm & {"DELTA", "S1_DELTA", "S1DELTA"})


def _delta_to_rgb(delta: np.ndarray, vmin: float, vmax: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Map delta dB to configurable palette RGB (turbo / viridis / rdylgn)."""
    palette_key = c.S1_DELTA_PALETTE.lower()
    pal = _DELTA_PALETTES.get(palette_key, _DELTA_PALETTES["turbo"])
    t = np.clip((delta - vmin) / (vmax - vmin), 0, 1)
    vals = pal["values"]
    p_norm = (vals - vals[0]) / (vals[-1] - vals[0])
    r = np.interp(t, p_norm, pal["r"]).astype(np.uint8)
    g = np.interp(t, p_norm, pal["g"]).astype(np.uint8)
    b = np.interp(t, p_norm, pal["b"]).astype(np.uint8)
    return r, g, b


def _warp_crop_to_roi(src_path: str, bbox_str: str, tmp_path: str) -> bool:
    """Warp analytic Float32 to ROI bbox at 15m EPSG:3857 (common grid). Returns success."""
    try:
        west, south, east, north = map(float, bbox_str.split(","))
        warp_opts = gdal.WarpOptions(
            format="GTiff",
            outputBounds=[west, south, east, north],
            outputBoundsSRS="EPSG:4326",
            dstSRS="EPSG:3857",
            xRes=15,
            yRes=15,
            resampleAlg="near",
            creationOptions=["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=YES"],
            srcNodata=0,
            dstNodata=0,
        )
        gdal.Warp(tmp_path, src_path, options=warp_opts)
        return os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0
    except Exception as e:
        print(f"Delta warp failed {src_path}: {e}", flush=True)
        return False


def _compute_delta_pair(
    ana_new: str,
    ana_old: str,
    vh_new: Optional[str],
    bbox_str: str,
    vis_out: str,
    ana_out: str,
    rel_orbit: Optional[str],
    orbit_dir: Optional[str],
    satellite: Optional[str],
) -> bool:
    """Crop both analytics to ROI, compute VV_t - VV_{t-1} dB, write visual+analytic. Returns success."""
    tmp_new = None
    tmp_old = None
    tmp_vh = None
    try:
        fd, tmp_new = tempfile.mkstemp(suffix="_delta_new.tif")
        os.close(fd)
        fd, tmp_old = tempfile.mkstemp(suffix="_delta_old.tif")
        os.close(fd)
        if vh_new and os.path.exists(vh_new):
            fd, tmp_vh = tempfile.mkstemp(suffix="_delta_vh.tif")
            os.close(fd)

        if not _warp_crop_to_roi(ana_new, bbox_str, tmp_new):
            return False
        if not _warp_crop_to_roi(ana_old, bbox_str, tmp_old):
            return False
        vh_path = None
        if tmp_vh:
            if _warp_crop_to_roi(vh_new, bbox_str, tmp_vh):  # type: ignore[arg-type]
                vh_path = tmp_vh
            else:
                vh_path = None

        with rio.open(tmp_new) as src_new, rio.open(tmp_old) as src_old:
            if src_new.width != src_old.width or src_new.height != src_old.height:
                print(f"Delta crop size mismatch {src_new.width}x{src_new.height} vs {src_old.width}x{src_old.height}", flush=True)
                return False
            profile = src_new.profile.copy()
            width, height = src_new.width, src_new.height
            transform = src_new.transform
            crs = src_new.crs

            # Read full ROI arrays (Kronstadt ~2.6M px, St Petersburg ~13M px still fine)
            vv_new_lin = src_new.read(1).astype(np.float32)
            vv_old_lin = src_old.read(1).astype(np.float32)

            # VH for sea mask (optional)
            vh_db = None
            if vh_path and os.path.exists(vh_path):
                try:
                    with rio.open(vh_path) as vh_src:
                        vh_lin = vh_src.read(1).astype(np.float32)
                        # vh_lin is linear sigma0, convert to dB wherever >0
                        vh_db = np.full_like(vh_lin, -999.0, dtype=np.float32)
                        m = vh_lin > 0
                        vh_db[m] = 10 * np.log10(vh_lin[m])
                except Exception:
                    vh_db = None

            # Compute delta in dB where both valid
            valid = (vv_new_lin > 0) & (vv_old_lin > 0)
            if vh_db is not None:
                valid &= vh_db > c.S1_DELTA_VH_THRESH

            delta = np.zeros_like(vv_new_lin, dtype=np.float32)
            if HAS_CUDA:
                try:
                    m_pool = cp.get_default_memory_pool()
                    vv_n_g = cp.array(vv_new_lin, dtype=cp.float32)
                    vv_o_g = cp.array(vv_old_lin, dtype=cp.float32)
                    valid_g = cp.array(valid)
                    delta_g = cp.zeros_like(vv_n_g, dtype=cp.float32)
                    # 10*log10 only where valid
                    # avoid log10(0) -> use where valid
                    delta_g[valid_g] = 10 * cp.log10(vv_n_g[valid_g]) - 10 * cp.log10(vv_o_g[valid_g])
                    delta = cp.asnumpy(delta_g)
                    del vv_n_g, vv_o_g, valid_g, delta_g
                    m_pool.free_all_blocks()
                except Exception as e:
                    print(f"Delta GPU fallback to CPU: {e}", flush=True)
                    # CPU fallback
                    delta[valid] = 10 * np.log10(vv_new_lin[valid]) - 10 * np.log10(vv_old_lin[valid])
            else:
                delta[valid] = 10 * np.log10(vv_new_lin[valid]) - 10 * np.log10(vv_old_lin[valid])

            # Free large arrays early
            del vv_new_lin, vv_old_lin
            gc.collect()

            # Analytic output: Float32 delta, nodata 0 outside valid
            ana_profile = profile.copy()
            ana_profile.update(
                driver="GTiff",
                dtype=rio.float32,
                count=1,
                compress="DEFLATE",
                tiled=True,
                blockxsize=256,
                blockysize=256,
                nodata=0,
                BIGTIFF="YES",
                num_threads=2,
            )
            # delta already 0 outside valid (nodata)
            with rio.open(ana_out, "w", **ana_profile) as dst_ana:
                dst_ana.write(delta, 1)
                if rel_orbit:
                    dst_ana.update_tags(RELATIVE_ORBIT_NUMBER=str(rel_orbit))
                if orbit_dir:
                    dst_ana.update_tags(ORBIT_DIRECTION=orbit_dir)
                if satellite:
                    dst_ana.update_tags(SATELLITE=satellite)
                # Also store delta params
                dst_ana.update_tags(DELTA_VH_THRESH=str(c.S1_DELTA_VH_THRESH))

            # Visual output: RGBA diverge
            vmin, vmax = c.S1_DELTA_MIN, c.S1_DELTA_MAX
            # Clip delta for colormap but keep analytic unclipped
            delta_clipped = np.clip(delta, vmin, vmax)
            # Map to RGB only where valid, else 0
            r, g, b = _delta_to_rgb(delta_clipped, vmin, vmax)
            # Gate: |delta| < gate -> transparent (suppresses speckle noise)
            gated = valid & (np.abs(delta) >= c.S1_DELTA_GATE_DB)
            alpha = np.where(gated, 255, 0).astype(np.uint8)
            r = np.where(gated, r, 0).astype(np.uint8)
            g = np.where(gated, g, 0).astype(np.uint8)
            b = np.where(gated, b, 0).astype(np.uint8)

            vis_profile = profile.copy()
            vis_profile.update(
                driver="GTiff",
                dtype=rio.uint8,
                count=4,
                compress="DEFLATE",
                tiled=True,
                blockxsize=256,
                blockysize=256,
                photometric="RGB",
                nodata=None,
                BIGTIFF="YES",
                num_threads=2,
            )
            with rio.open(vis_out, "w", **vis_profile) as dst_vis:
                dst_vis.write(r, 1)
                dst_vis.write(g, 2)
                dst_vis.write(b, 3)
                dst_vis.write(alpha, 4)
                dst_vis.colorinterp = [
                    rio.enums.ColorInterp.red,
                    rio.enums.ColorInterp.green,
                    rio.enums.ColorInterp.blue,
                    rio.enums.ColorInterp.alpha,
                ]
                if rel_orbit:
                    dst_vis.update_tags(RELATIVE_ORBIT_NUMBER=str(rel_orbit))
                if orbit_dir:
                    dst_vis.update_tags(ORBIT_DIRECTION=orbit_dir)
                if satellite:
                    dst_vis.update_tags(SATELLITE=satellite)

            # COG + overviews + sidecar
            cog.convert_to_cog(vis_out)
            cog.ensure_overviews(vis_out)
            cog.convert_to_cog(ana_out)
            # Analytic does not need overviews but sidecar needs footprint from visual
            meta.generate_sidecar(
                vis_out,
                "S1-DELTA",
                "S1-DELTA",
                effective_res=15.0,
                relative_orbit=str(rel_orbit) if rel_orbit else None,
                orbit_direction=orbit_dir,
                satellite=satellite,
            )
            # Also generate sidecar for analytic? Not needed for viewer, but for completeness use same inventory path? Keep only visual sidecar
            gc.collect()
            return True

    except Exception as e:
        print(f"Delta compute failed {ana_new} vs {ana_old}: {e}", flush=True)
        # cleanup partials
        for p in [vis_out, ana_out]:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                    jp = p.replace(".tif", ".json")
                    if os.path.exists(jp):
                        os.remove(jp)
                except Exception:
                    pass
        return False
    finally:
        for tmp in [tmp_new, tmp_old, tmp_vh]:
            if tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
        gc.collect()


def _find_candidates_for_roi(
    roi: Dict[str, Any],
    s1_vv_layers: List[Dict[str, Any]],
) -> List[Tuple[Dict[str, Any], str]]:
    """Filter S1-VV layers covering ROI with bbox_match, return (layer, ana_path). Optimized with bounds pre-check."""
    bbox_str = func.resolve_env_variable(roi.get("bbox", ""))
    if not bbox_str:
        return []
    try:
        west, south, east, north = map(float, bbox_str.split(","))
    except Exception:
        return []
    roi_bbox = (west, south, east, north)
    thresh = roi.get("bbox_match", 90)
    candidates: List[Tuple[Dict[str, Any], str]] = []

    # Quick age window
    cutoff = datetime.now(timezone.utc) - timedelta(days=c.S1_DELTA_DAYS)

    for layer in s1_vv_layers:
        # orbit_direction required, relative_orbit optional (S1 pipeline may omit if manifest lacks tag)
        odir = layer.get("orbit_direction")
        if not odir:
            continue
        rel = layer.get("relative_orbit")  # may be None -> group by direction only
        # age
        acq_str = layer.get("acquisition_time", "")
        try:
            acq_dt = datetime.fromisoformat(acq_str.replace("Z", "+00:00"))
            if acq_dt < cutoff:
                continue
        except Exception:
            continue
        # quick bounds intersect before expensive polygon union
        b = layer.get("bounds")
        if b:
            try:
                b_south, b_west = b[0]
                b_north, b_east = b[1]
                if b_east < west or b_west > east or b_north < south or b_south > north:
                    continue
            except Exception:
                pass
        # precise coverage
        cov = rm.calculate_coverage(bbox_str, [layer])
        if cov < thresh:
            continue
        # analytic path exists?
        vis_rel = layer.get("path", "")
        if not vis_rel:
            continue
        base = os.path.basename(vis_rel)
        # Analytic is output/analytic/s1/vv/{base}
        ana_path = os.path.join(c.DIRS["ANA_S1_VV"], base)
        if not os.path.exists(ana_path):
            # fallback: replace visual->analytic in full path
            vis_full = os.path.join(c.DIRS["OUT"], vis_rel)
            ana_alt = vis_full.replace("/visual/s1/vv/", "/analytic/s1/vv/").replace("/visual/", "/analytic/")
            if os.path.exists(ana_alt):
                ana_path = ana_alt
            else:
                continue
        candidates.append((layer, ana_path))
    return candidates


def run_delta(
    dry_run: bool = False,
    roi_filter: Optional[str] = None,
) -> int:
    """
    Generic per-ROI Delta stage. Enabled via "DELTA" in products[].
    Returns number of deltas created.
    Optimized: only scans S1-VV inventory, orbit-matched, single ROI window.
    """
    func.perf_logger.start_step("S1 Delta Stage")

    config, rois = rm.load_roi_config()
    if not rois:
        print("No ROI config for Delta.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Generic filter: any ROI with DELTA in products
    delta_rois = [r for r in rois if _roi_wants_delta(r)]
    if roi_filter:
        target = roi_filter.strip().replace("_", " ").lower()
        delta_rois = [
            r for r in delta_rois if func.resolve_env_variable(r.get("name", "")).replace("_", " ").lower() == target
        ]
        if not delta_rois:
            print(f"No DELTA-enabled ROI matching '{roi_filter}'.", flush=True)
            func.perf_logger.end_step()
            return 0

    if not delta_rois:
        print("No ROIs request DELTA (add \"DELTA\" to products[]). Skipping.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Only run if S1 pipeline active (or analytic exists)
    # But allow manual run even if PIPELINES doesn't contain S1 if files exist

    inventory_path = os.path.join(c.DIRS["OUT"], "visual/inventory.json")
    if not os.path.exists(inventory_path):
        print("Inventory missing, skip Delta.", flush=True)
        func.perf_logger.end_step()
        return 0

    try:
        with open(inventory_path, "r", encoding="utf-8") as f:
            inventory = json.load(f)
    except Exception as e:
        print(f"Delta: inventory read failed {e}", flush=True)
        func.perf_logger.end_step()
        return 0

    layers = inventory.get("layers", [])
    # S1 processes RATIOVVVH creates S1-RATIO visuals + VV/VH analytics, but no S1-VV visuals
    # so accept both S1-VV and S1-RATIO as proxies for VV analytic
    s1_vv_layers = [l for l in layers if l.get("product") in ("S1-VV", "S1-RATIO")]
    if not s1_vv_layers:
        print("No S1-VV/RATIO layers for Delta.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Optimization: quick check if any new S1-VV in this run (render_time >= run_start)
    # If not, we still may have existing pair, but skip heavy warp if delta already exists.
    # We do per-ROI existence check later anyway; this is just a fast path to avoid scanning all ROIs when nothing new.
    try:
        run_start_dt = datetime.fromtimestamp(func.perf_logger.start_time, tz=timezone.utc)
        has_new = any(
            datetime.fromisoformat(l.get("render_time", "1970-01-01T00:00:00Z").replace("Z", "+00:00")) >= run_start_dt
            for l in s1_vv_layers
        )
    except Exception:
        has_new = True

    created = 0
    # Ensure output dirs exist
    for d in [c.DIRS["VIS_S1_DELTA"], c.DIRS["ANA_S1_DELTA"]]:
        os.makedirs(d, exist_ok=True)

    for roi in delta_rois:
        roi_name_raw = roi.get("name", "ROI")
        roi_name = func.resolve_env_variable(roi_name_raw).replace("_", " ")
        bbox_str = func.resolve_env_variable(roi.get("bbox", ""))
        print(f"Checking DELTA for ROI {roi_name} ({bbox_str})", flush=True)

        candidates = _find_candidates_for_roi(roi, s1_vv_layers)
        if len(candidates) < 2:
            print(f"  Delta: need ≥2 VV scenes covering ROI (found {len(candidates)}). Skip.", flush=True)
            continue

        # Group by orbit (relative_orbit may be missing -> group by direction only)
        by_orbit: Dict[Tuple[str, str], List[Tuple[Dict[str, Any], str, str]]] = defaultdict(list)
        for layer, ana_path in candidates:
            rel = layer.get("relative_orbit") or "unknown"
            odir = (layer.get("orbit_direction") or "unknown").upper()
            key = (str(rel), odir)
            by_orbit[key].append((layer, ana_path, layer["acquisition_time"]))

        for (rel_orbit, orbit_dir), group in by_orbit.items():
            group.sort(key=lambda x: x[2], reverse=True)
            if len(group) < 2:
                continue
            (l_new, ana_new, t_new), (l_old, ana_old, t_old) = group[0], group[1]
            # Use newest time for filename
            # inventory time is ISO like 2026-08-27T05:12:00Z -> clean to 2026-08-27T051200Z
            iso_clean = t_new.replace(":", "")
            safe_roi = roi_name.replace(" ", "_")
            base_name = f"{safe_roi}_DELTA_{iso_clean}"
            vis_out = os.path.join(c.DIRS["VIS_S1_DELTA"], base_name + ".tif")
            ana_out = os.path.join(c.DIRS["ANA_S1_DELTA"], base_name + ".tif")

            # If both outputs exist, skip unless dry_run or new data newer than existing
            if os.path.exists(vis_out) and os.path.exists(ana_out):
                if not dry_run:
                    # check mtime of output vs new acquisition render_time? Simple skip
                    # But if has_new and output older than newest layer's render_time, regenerate?
                    # For MVP, skip if exists.
                    print(f"  Delta exists: {base_name} (orbit {rel_orbit} {orbit_dir}), skipping.", flush=True)
                    continue
                else:
                    print(f"  [dry-run] would skip existing {base_name}", flush=True)
                    continue

            if dry_run:
                print(f"  [dry-run] would create DELTA {base_name} from {t_new} - {t_old} (orbit {rel_orbit} {orbit_dir})", flush=True)
                continue

            # Optimization: if no new data and we already checked existence, skip warp. Already handled.
            # Now compute
            print(f"  Creating DELTA {base_name} orbit {rel_orbit} {orbit_dir} ({t_new} vs {t_old})", flush=True)

            # Find VH for masking: try to locate VH analytic for newest acquisition
            # Derive VH path: same basename but in VH analytic folder
            # Inventory doesn't have VH sidecar pairing easily; guess via timestamp
            vh_new = None
            # Try to find VH layer with same acquisition_time and orbit
            for l in layers:
                if l.get("product") == "S1-VH" and l.get("acquisition_time") == t_new and str(l.get("relative_orbit")) == str(rel_orbit):
                    vh_vis = l.get("path", "")
                    vh_base = os.path.basename(vh_vis) if vh_vis else ""
                    vh_cand = os.path.join(c.DIRS["ANA_S1_VH"], vh_base)
                    if os.path.exists(vh_cand):
                        vh_new = vh_cand
                        break
                    alt = os.path.join(c.DIRS["OUT"], vh_vis).replace("/visual/", "/analytic/") if vh_vis else ""
                    if alt and os.path.exists(alt):
                        vh_new = alt
                        break

            sat = l_new.get("satellite")
            ok = _compute_delta_pair(
                ana_new, ana_old, vh_new, bbox_str, vis_out, ana_out, rel_orbit, orbit_dir, sat
            )
            if ok:
                created += 1
                print(f"  Delta created: {os.path.basename(vis_out)}", flush=True)
            else:
                print(f"  Delta failed for {roi_name} {rel_orbit}", flush=True)

            # For generic multi-orbit: only create one delta per ROI per run per orbit group.
            # For pilot Kronstadt, typically single orbit covers Gulf, so one delta.

    if created > 0 and not dry_run:
        # Rebuild inventory to include new deltas (single rebuild, not per ROI)
        try:
            import inventory_manager

            inventory_manager.rebuild_inventory()
        except Exception as e:
            print(f"Delta: inventory rebuild failed {e}", flush=True)

    print(f"S1 Delta stage complete. {created} deltas created.", flush=True)
    func.perf_logger.end_step()
    return created


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="S1 Delta per ROI")
    parser.add_argument("--dry-run", action="store_true", help="print what would be done")
    parser.add_argument("--roi", type=str, default=None, help="only this ROI")
    args = parser.parse_args()
    func.perf_logger.start_run()
    run_delta(dry_run=args.dry_run, roi_filter=args.roi)
    func.perf_logger.stop_run()
