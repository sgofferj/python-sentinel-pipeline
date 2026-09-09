#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# roi_manager.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
ROI Manager for creating cropped visual products based on Region of Interest definitions.
Scans pipeline outputs and extracts crops for specified ROIs.
Supports both pipeline integration (new files only) and standalone (all files) modes.
"""

import argparse
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional

import pillow_heif  # type: ignore
import requests  # type: ignore
from atproto import Client, client_utils  # type: ignore
from osgeo import gdal  # type: ignore
from PIL import Image
from shapely.geometry import box, shape  # type: ignore
from shapely.ops import unary_union  # type: ignore

import constants as c
import functions as func
import metadata_engine as meta
import inventory_manager
import notifications as notify
import cog_finalizer as cog
import numpy as np
import rasterio as rio

# --- AIS Correlator (optional, for per-ROI fallback) ---
AIS_DIR = os.path.join(os.path.dirname(__file__), "ais-correlator")
if os.path.exists(AIS_DIR) and AIS_DIR not in sys.path:
    sys.path.append(AIS_DIR)

# Register HEIF opener for Pillow
pillow_heif.register_heif_opener()

gdal.UseExceptions()


def load_roi_config() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Loads ROI definitions and global config from roi_config.json."""
    config_path = os.path.join(c.BASE_DIR, "roi_config.json")
    if not os.path.exists(config_path):
        return {}, []
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Support both old (list only) and new (dict with 'config' and 'rois') formats
            if isinstance(data, list):
                return {}, data
            return data.get("config", {}), data.get("rois", [])
    except Exception as e:
        print(f"Error loading ROI config: {e}", flush=True)
        return {}, []


def calculate_coverage(
    roi_bbox_str: str, layer_list: List[Dict[str, Any]], roi_poly: Optional[Any] = None
) -> float:
    """Calculates the percentage of ROI covered by the union of multiple product footprints."""
    try:
        if roi_poly is None:
            west, south, east, north = map(float, roi_bbox_str.split(","))
            roi_poly = box(west, south, east, north)

        product_polys = []
        for layer in layer_list:
            footprint_raw = layer.get("footprint")
            if not footprint_raw:
                # Fallback to bounds if footprint is missing
                bounds = layer.get("bounds")  # [[S, W], [N, E]]
                if bounds:
                    product_polys.append(
                        box(bounds[0][1], bounds[0][0], bounds[1][1], bounds[1][0])
                    )
            else:
                product_polys.append(shape(footprint_raw))

        if not product_polys:
            return 0.0

        combined_product_poly = unary_union(product_polys)

        if not roi_poly.intersects(combined_product_poly):
            return 0.0

        intersection_area = roi_poly.intersection(combined_product_poly).area
        roi_area = roi_poly.area

        if roi_area == 0:
            return 0.0

        return (intersection_area / roi_area) * 100
    except Exception as e:
        print(f"Error calculating coverage: {e}", flush=True)
        return 0.0


def crop_product(src_paths: List[str], dst_path: str, bbox_str: str) -> bool:
    """Crops one or more TIFF files to the specified WGS84 bounding box (mosaicing if needed)."""
    try:
        west, south, east, north = map(float, bbox_str.split(","))

        # Check source bands to avoid 5-band TIFFs (GDAL adds alpha to existing alpha)
        # We only take the RGB bands if it's a 4-band product, or 1 band if it's 2-band.
        # This ensures dstAlpha produces a clean, standard 4-band or 2-band output.
        src_bands = None
        ds = gdal.Open(src_paths[0])
        if ds:
            if ds.RasterCount == 4:
                src_bands = [1, 2, 3]
            elif ds.RasterCount == 2:
                src_bands = [1]
            ds = None

        # Use gdal.Warp for cropping and mosaicing
        warp_options = gdal.WarpOptions(
            format="GTiff",
            outputBounds=[west, south, east, north],
            outputBoundsSRS="EPSG:4326",
            srcBands=src_bands,
            creationOptions=["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=YES"],
            dstAlpha=True,
        )
        gdal.Warp(dst_path, src_paths, options=warp_options)
        return True
    except Exception as e:
        print(f"Error cropping {src_paths} to ROI: {e}", flush=True)
        return False


# --- S1 Delta helpers (ROI-integrated, with combining) ---
try:
    import importlib.util as _ilu

    _HAS_CUPY = _ilu.find_spec("cupy") is not None and os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
    if _HAS_CUPY:
        import cupy as _cp  # type: ignore
    else:
        _cp = None  # type: ignore
except Exception:
    _HAS_CUPY = False
    _cp = None  # type: ignore

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
    "grey-rdbu": {
        "values": np.array([-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]),
        "r": np.array([40, 45, 50, 45, 35, 70, 110, 150, 180]),
        "g": np.array([70, 60, 50, 42, 35, 35, 35, 35, 30]),
        "b": np.array([160, 140, 120, 80, 35, 35, 35, 35, 30]),
    },
    "grey-red": {
        "values": np.array([0.0, 0.33, 0.66, 1.0]),
        "r": np.array([35, 80, 130, 180]),
        "g": np.array([35, 35, 35, 30]),
        "b": np.array([35, 35, 35, 30]),
    },
}


def _roi_wants_delta(roi: Dict[str, Any]) -> bool:
    """True if ROI products[] contains DELTA / S1-DELTA (generic per-ROI)."""
    norm = {str(p).upper().replace("-", "_") for p in roi.get("products", [])}
    return bool(norm & {"DELTA", "S1_DELTA", "S1DELTA"})


def _delta_to_rgb(delta: np.ndarray, vmin: float, vmax: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
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



def _warp_analytic_to_roi(src_paths: List[str], bbox_str: str, tmp_path: str) -> bool:
    """Warp one or more analytic Float32 VV/VH to ROI bbox at 15 m EPSG:3857 (mosaic if needed)."""
    try:
        west, south, east, north = map(float, bbox_str.split(","))
        opts = gdal.WarpOptions(
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
        gdal.Warp(tmp_path, src_paths, options=opts)
        return os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0
    except Exception as e:
        print(f"Delta warp failed {src_paths}: {e}", flush=True)
        return False


def _find_best_s1_paths_for_group(
    roi_bbox_str: str,
    roi_poly: Any,
    group_layers: List[Dict[str, Any]],
    thresh: float,
) -> tuple[Optional[List[str]], float]:
    """For a single-date S1 group, find best single or combined src_paths achieving coverage >= thresh (like ROI manager). Returns (paths, coverage)."""
    # Try single best
    best_cov = 0.0
    best_layer = None
    for layer in group_layers:
        cov = calculate_coverage(roi_bbox_str, [layer], roi_poly=roi_poly)
        if cov > best_cov:
            best_cov = cov
            best_layer = layer
    if best_cov >= thresh and best_layer is not None:
        # Use vis path to derive analytic path via basename in ANA_S1_VV
        base = os.path.basename(best_layer.get("path", ""))
        ana = os.path.join(c.DIRS["ANA_S1_VV"], base)
        if not os.path.exists(ana):
            alt = os.path.join(c.DIRS["OUT"], best_layer.get("path", "")).replace("/visual/", "/analytic/")
            if os.path.exists(alt):
                ana = alt
            else:
                return None, 0.0
        return [ana], best_cov
    # Fallback combined
    cov_all = calculate_coverage(roi_bbox_str, group_layers, roi_poly=roi_poly)
    if cov_all >= thresh:
        anas = []
        for layer in group_layers:
            base = os.path.basename(layer.get("path", ""))
            ana = os.path.join(c.DIRS["ANA_S1_VV"], base)
            if not os.path.exists(ana):
                alt = os.path.join(c.DIRS["OUT"], layer.get("path", "")).replace("/visual/", "/analytic/")
                if os.path.exists(alt):
                    ana = alt
                else:
                    continue
            anas.append(ana)
        if anas:
            return anas, cov_all
    return None, 0.0


def _compute_roi_delta(
    ana_paths_new: List[str],
    ana_paths_old: List[str],
    vh_paths_new: Optional[List[str]],
    bbox_str: str,
    vis_out: str,
    rel_orbit: Optional[str],
    orbit_dir: Optional[str],
    satellite: Optional[str],
) -> bool:
    """Create ROI delta visual (RGBA) via warped VV difference. Handles multi-slice mosaic."""
    tmp_new = tmp_old = tmp_vh = None
    try:
        fd, tmp_new = tempfile.mkstemp(suffix="_delta_new.tif")
        os.close(fd)
        fd, tmp_old = tempfile.mkstemp(suffix="_delta_old.tif")
        os.close(fd)
        if vh_paths_new:
            # Check if any VH ana exists
            has_vh = any(os.path.exists(p) for p in vh_paths_new)
            if has_vh:
                fd, tmp_vh = tempfile.mkstemp(suffix="_delta_vh.tif")
                os.close(fd)
        if not _warp_analytic_to_roi(ana_paths_new, bbox_str, tmp_new):
            return False
        if not _warp_analytic_to_roi(ana_paths_old, bbox_str, tmp_old):
            return False
        vh_path = None
        if tmp_vh and vh_paths_new:
            # Warp VH mosaic as well
            # Filter to existing
            existing_vh = [p for p in vh_paths_new if os.path.exists(p)]
            if existing_vh and _warp_analytic_to_roi(existing_vh, bbox_str, tmp_vh):
                vh_path = tmp_vh
        with rio.open(tmp_new) as src_new, rio.open(tmp_old) as src_old:
            if src_new.width != src_old.width or src_new.height != src_old.height:
                print(f"Delta crop size mismatch {src_new.width}x{src_new.height} vs {src_old.width}x{src_old.height}", flush=True)
                return False
            profile = src_new.profile.copy()
            vv_new_lin = src_new.read(1).astype(np.float32)
            vv_old_lin = src_old.read(1).astype(np.float32)
            vh_db = None
            if vh_path and os.path.exists(vh_path):
                try:
                    with rio.open(vh_path) as vh_src:
                        vh_lin = vh_src.read(1).astype(np.float32)
                        vh_db = np.full_like(vh_lin, -999.0, dtype=np.float32)
                        m = vh_lin > 0
                        vh_db[m] = 10 * np.log10(vh_lin[m])
                except Exception:
                    vh_db = None
            # Valid is VV>0 on both dates (inside swath). VH is not used to mask
            # water to transparent — water with no change is middle colour, not see-through
            valid = (vv_new_lin > 0) & (vv_old_lin > 0)
            delta = np.zeros_like(vv_new_lin, dtype=np.float32)
            if _HAS_CUPY and _cp is not None:
                try:
                    m_pool = _cp.get_default_memory_pool()
                    vv_n_g = _cp.array(vv_new_lin, dtype=_cp.float32)
                    vv_o_g = _cp.array(vv_old_lin, dtype=_cp.float32)
                    valid_g = _cp.array(valid)
                    delta_g = _cp.zeros_like(vv_n_g, dtype=_cp.float32)
                    delta_g[valid_g] = 10 * _cp.log10(vv_n_g[valid_g]) - 10 * _cp.log10(vv_o_g[valid_g])
                    delta = _cp.asnumpy(delta_g)
                    del vv_n_g, vv_o_g, valid_g, delta_g
                    m_pool.free_all_blocks()
                except Exception as e:
                    print(f"Delta GPU fallback: {e}", flush=True)
                    delta[valid] = 10 * np.log10(vv_new_lin[valid]) - 10 * np.log10(vv_old_lin[valid])
            else:
                delta[valid] = 10 * np.log10(vv_new_lin[valid]) - 10 * np.log10(vv_old_lin[valid])
            import gc

            del vv_new_lin, vv_old_lin
            gc.collect()
            # Visual: magnitude |Δ|, stable (|Δ|<gate) is grey (0) opaque
            # Water uses higher gate + VH ship filter so rough water stays grey and only ships go red
            vmin, vmax = 0.0, c.S1_DELTA_MAX
            abs_delta = np.abs(delta)
            abs_clipped = np.clip(abs_delta, vmin, vmax)
            r, g, b = _delta_to_rgb(abs_clipped, vmin, vmax)
            # Water-aware gating: land gate 2.0, water gate 3.0 + VH>-15 (ship)
            gated = None
            try:
                import water_mask
                wmask = water_mask.get_water_mask(bbox_str, resolution=15.0, crs="EPSG:3857")
                if wmask is not None:
                    if wmask.shape != valid.shape:
                        # Resize via nearest-neighbour using numpy repeat for 1-2px mismatches
                        # (Kronstadt 2672x2079 vs water mask 2672x2079 should match; keep fallback)
                        try:
                            from scipy.ndimage import zoom  # type: ignore

                            zy = valid.shape[0] / wmask.shape[0]
                            zx = valid.shape[1] / wmask.shape[1]
                            wmask = zoom(wmask, (zy, zx), order=0)[: valid.shape[0], : valid.shape[1]]
                            if wmask.shape != valid.shape:
                                wmask = np.resize(wmask, valid.shape)
                        except Exception:
                            wmask = np.resize(wmask, valid.shape)
                    wbool = wmask.astype(bool)
                    land_gated = (~wbool & valid) & (abs_delta >= c.S1_DELTA_GATE_DB)
                    if vh_db is not None:
                        water_gated = (wbool & valid) & (abs_delta >= 3.0) & (vh_db > -15.0)
                    else:
                        water_gated = (wbool & valid) & (abs_delta >= 3.0)
                    gated = land_gated | water_gated
            except Exception as e:
                print(f"Water mask gating fallback: {e}", flush=True)
            if gated is None:
                gated = valid & (abs_delta >= c.S1_DELTA_GATE_DB)
            # For TIF (viewer): grey is transparent, gated has alpha ramp 80->255 with palette
            # For social: grey stays grey opaque (handled in create_social_image/create_full_image)
            r_mid, g_mid, b_mid = _delta_to_rgb(np.array([0.0], dtype=np.float32), vmin, vmax)
            r_mid, g_mid, b_mid = int(r_mid[0]), int(g_mid[0]), int(b_mid[0])
            # Alpha ramp for viewer: 0 for stable/transparent, 80-255 for gated proportional to |Δ|
            alpha_ramp = np.zeros_like(abs_delta, dtype=np.uint8)
            if np.any(gated):
                # Map |Δ| gate..vmax -> 80..255
                ramp = 80 + (np.clip(abs_delta, c.S1_DELTA_GATE_DB, vmax) - c.S1_DELTA_GATE_DB) / (vmax - c.S1_DELTA_GATE_DB) * 175
                alpha_ramp[gated] = ramp[gated].astype(np.uint8)
            # TIF alpha is ramp (transparent grey)
            alpha = alpha_ramp.astype(np.uint8)
            r = np.where(gated, r, 0).astype(np.uint8)
            g = np.where(gated, g, 0).astype(np.uint8)
            b = np.where(gated, b, 0).astype(np.uint8)
            vis_profile = profile.copy()
            vis_profile.update(driver="GTiff", dtype=rio.uint8, count=4, compress="DEFLATE", tiled=True, blockxsize=256, blockysize=256, photometric="RGB", nodata=None, BIGTIFF="YES", num_threads=2)
            with rio.open(vis_out, "w", **vis_profile) as dst_vis:
                dst_vis.write(r, 1)
                dst_vis.write(g, 2)
                dst_vis.write(b, 3)
                dst_vis.write(alpha, 4)
                dst_vis.colorinterp = [rio.enums.ColorInterp.red, rio.enums.ColorInterp.green, rio.enums.ColorInterp.blue, rio.enums.ColorInterp.alpha]
                if rel_orbit and rel_orbit != "unknown":
                    dst_vis.update_tags(RELATIVE_ORBIT_NUMBER=str(rel_orbit))
                if orbit_dir:
                    dst_vis.update_tags(ORBIT_DIRECTION=orbit_dir)
                if satellite:
                    dst_vis.update_tags(SATELLITE=satellite)
            cog.convert_to_cog(vis_out)
            cog.ensure_overviews(vis_out)
            # Sidecar for visual (ROI product)
            # Use ROI naming, legend S1-DELTA
            # Caller will have set product_type, but ensure sidecar generated
            # We generate here with visual path
            # Note: caller handles product_type, we just ensure file exists
            return True
    except Exception as e:
        print(f"Delta compute failed {ana_paths_new} vs {ana_paths_old}: {e}", flush=True)
        for p in [vis_out]:
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
        try:
            import gc

            gc.collect()
        except Exception:
            pass


def _stamp_image(img: Image.Image, acq_old: str, acq_new: str, orbit_dir: Optional[str] = None) -> Image.Image:
    """Add a semi-transparent data stamp to the bottom-right of an RGBA or RGB image."""
    from PIL import ImageDraw, ImageFont
    def _fmt(ts: str) -> str:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%MZ")
        except Exception:
            return ts[:16] + "Z"
    lines = [f"{_fmt(acq_new)}  →  {_fmt(acq_old)}", f"ΔSAR  |  {(orbit_dir or 'UNK').upper()}"]
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    w, h = img.size
    bar_h = max(int(h * 0.06), 32)
    overlay = Image.new("RGBA", (w, bar_h), (0, 0, 0, 160))
    img.paste(overlay, (0, h - bar_h), overlay)
    draw = ImageDraw.Draw(img)
    font_size = max(int(bar_h * 0.36), 10)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
    except Exception:
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", font_size)
        except Exception:
            font = ImageFont.load_default()
    for i, line in enumerate(lines):
        _, _, tw, th = draw.textbbox((0, 0), line, font=font)
        tx = w - tw - int(w * 0.015)
        ty = h - bar_h + int(bar_h * 0.08) + i * (th + 2)
        draw.text((tx, ty), line, fill=(230, 230, 230, 255), font=font)
    return img


def create_social_image(src_path: str, base_dst_path: str, stamp_acq_old: Optional[str] = None, stamp_acq_new: Optional[str] = None, stamp_orbit: Optional[str] = None) -> str:
    """
    Creates a non-georeferenced HEIC image from a TIFF for Bluesky.
    For S1-DELTA the TIF has grey transparent (alpha 0) and red gated with
    alpha ramp 80-255. Social image should have grey opaque (35,35,35) background
    so it is standalone, not basemap-dependent.
    """
    try:
        with Image.open(src_path) as img:
            # For DELTA, composite transparent grey over opaque grey background
            if "DELTA" in os.path.basename(src_path).upper():
                if img.mode != "RGBA":
                    img = img.convert("RGBA")
                # Create grey background 35,35,35 opaque
                grey_bg = Image.new("RGBA", img.size, (35, 35, 35, 255))
                # Composite TIF (with transparent grey) over grey background
                # This makes stable areas grey and gated red with ramp, as intended for social
                comp = Image.alpha_composite(grey_bg, img)
                rgb_img = comp.convert("RGB")
            else:
                if img.mode != "RGB":
                    rgb_img = img.convert("RGB")
                else:
                    rgb_img = img
            # Stamp delta metadata on social images
            if stamp_acq_old and stamp_acq_new:
                rgb_img = _stamp_image(rgb_img, stamp_acq_old, stamp_acq_new, stamp_orbit).convert("RGB")

            max_dim = 4000
            quality = 80
            dst_path = base_dst_path + ".heic"

            # Iterative downscaling and compression
            while True:
                # Current attempt with current max_dim
                if rgb_img.width > max_dim or rgb_img.height > max_dim:
                    temp_img = rgb_img.copy()
                    temp_img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
                else:
                    temp_img = rgb_img

                # Save as HEIF (HEIC)
                temp_img.save(dst_path, "HEIF", quality=quality)
                size = os.path.getsize(dst_path)

                if size <= 2000000:
                    return dst_path

                # If still too big, try lower quality first
                if quality > 40:
                    quality -= 10
                else:
                    # Then try smaller dimensions
                    max_dim = int(max_dim * 0.8)
                    quality = 70  # Reset quality for smaller dim
                    if max_dim < 1000:
                        break

            return dst_path
    except Exception as e:
        print(f"Error creating social image (HEIC): {e}", flush=True)
        return ""


def create_full_image(src_path: str, base_dst_path: str, stamp_acq_old: Optional[str] = None, stamp_acq_new: Optional[str] = None, stamp_orbit: Optional[str] = None) -> str:
    """
    Creates a full-size non-georeferenced JPEG from a TIFF.
    For DELTA, transparent grey becomes opaque grey for standalone JPEG.
    """
    try:
        with Image.open(src_path) as img:
            if "DELTA" in os.path.basename(src_path).upper():
                if img.mode != "RGBA":
                    img = img.convert("RGBA")
                grey_bg = Image.new("RGBA", img.size, (35, 35, 35, 255))
                comp = Image.alpha_composite(grey_bg, img)
                rgb_img = comp.convert("RGB")
            else:
                if img.mode != "RGB":
                    rgb_img = img.convert("RGB")
                else:
                    rgb_img = img
            # Stamp delta metadata on social images
            if stamp_acq_old and stamp_acq_new:
                rgb_img = _stamp_image(rgb_img, stamp_acq_old, stamp_acq_new, stamp_orbit).convert("RGB")

            dst_path = base_dst_path + "_full.jpg"
            # High quality JPEG, no downscaling
            rgb_img.save(dst_path, "JPEG", quality=90, optimize=True)
            return dst_path
    except Exception as e:
        print(f"Error creating full image: {e}", flush=True)
        return ""


PRODUCT_NAMES = {
    "TCI": "True Color",
    "TCI-GF": "True Color (Guided Filter)",
    "RATIO": "Radar Ratio (VV/VH)",
    "VV": "Radar VV",
    "VH": "Radar VH",
    "NIRFC": "False Color (NIR)",
    "NIRFC-GF": "False Color (Guided Filter)",
    "NDVI": "NDVI (Vegetation Index)",
    "NDRE": "NDRE (Plant Stress)",
    "NDBI": "NDBI (Urban/Built-up)",
    "NBR": "NBR (Burn Index)",
    "CAMO": "Camo Detection",
    "AP": "Atmospheric Penetration (SWIR)",
    "AP-GF": "Atmospheric Penetration (Guided Filter)",
    "LIFE-MACHINE": "Life/Machine Separation",
    "RADAR-BURN": "Radar Burn Detection",
}


def get_human_name(product_type: str) -> str:
    """Converts a technical product type to a human-readable name."""
    pt_up = product_type.upper()
    if pt_up in PRODUCT_NAMES:
        return PRODUCT_NAMES[pt_up]
    # Strip sensor prefix (e.g. "S2-TCI-GF" → "TCI-GF")
    rest = product_type.split("-", 1)[1] if "-" in product_type else product_type
    parts = rest.split("-")
    # Try progressively shorter suffixes: "TCI-GF", then "GF"
    for i in range(len(parts)):
        candidate = "-".join(parts[i:]).upper()
        if candidate in PRODUCT_NAMES:
            return PRODUCT_NAMES[candidate]
    return rest


# --- AIS per-ROI fallback ---
AIS_BASE_MAP: Dict[str, str] = {
    "S1-RATIO": "S1-RATIO-AIS",
    "S2-TCI": "S2-TCI-AIS",
    "S2-TCI-GF": "S2-TCI-AIS",  # TCI-GF is filtered TCI, also valid background for AIS
}


def _roi_wants_ais(roi: Dict[str, Any], base_product_type: str) -> bool:
    """True if ROI's products[] requests AIS overlay for the given base type.

    base_product_type is 'S1-RATIO', 'S2-TCI' or 'S2-TCI-GF'. Handles:
      - generic 'AIS' (wants both)
      - 'RATIO-AIS' / 'S1-RATIO-AIS' / 'RATIOVVVH-AIS' → S1
      - 'TCI-AIS' / 'S2-TCI-AIS' / 'TCI-GF-AIS' → S2
    Normalises hyphens/underscores and is case-insensitive.
    """
    for p in roi.get("products", []):
        up = str(p).upper().strip()
        norm = up.replace("-", "_").replace(" ", "_")
        if "AIS" not in norm:
            continue
        if norm == "AIS":
            return True
        if base_product_type == "S1-RATIO":
            if "RATIO" in norm or "S1" in norm:
                return True
        elif base_product_type in ("S2-TCI", "S2-TCI-GF"):
            if "TCI" in norm or "S2" in norm:
                return True
    return False


BASEMAP_CACHE: str = os.path.join(c.BASE_DIR, "temp", "basemap_cache")
BASEMAP_TILE_URL: str = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile"
)


def _mercator(lon: float, lat: float) -> Tuple[float, float]:
    """Convert lon/lat to EPSG:3857 meters."""
    x = lon * 20037508.34 / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * 20037508.34 / 180.0
    return x, y


def _lonlat_to_tile(lon: float, lat: float, zoom: int) -> Tuple[int, int]:
    """Get XYZ tile (x,y) for given lon/lat at zoom level."""
    lat_rad = math.radians(lat)
    n = 2.0**zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def _tile_epsg3857_extent(
    x: int, y: int, zoom: int
) -> Tuple[float, float, float, float]:
    """Return (x_min, y_min, x_max, y_max) in EPSG:3857 for tile (x,y) at zoom."""
    n = 2.0**zoom
    tile_m = 40075016.68 / n
    x_min = -20037508.34 + x * tile_m
    x_max = -20037508.34 + (x + 1) * tile_m
    y_max = 20037508.34 - y * tile_m
    y_min = 20037508.34 - (y + 1) * tile_m
    return x_min, y_min, x_max, y_max


def fetch_roi_basemap(roi_bbox_str: str, roi_name: str) -> Optional[str]:
    """
    Fetch and cache an ESRI satellite basemap covering the ROI.
    Returns the path to the cached PNG, or None on failure.
    """
    cache_dir = os.path.join(BASEMAP_CACHE, roi_name)
    cache_path = os.path.join(cache_dir, "basemap.png")
    if os.path.exists(cache_path):
        return cache_path

    west, south, east, north = map(float, roi_bbox_str.split(","))
    roi_w, roi_s = _mercator(west, south)
    roi_e, roi_n = _mercator(east, north)

    # Pick zoom so the ROI fits in at most ~6 tiles
    zoom = 12
    for z in range(14, 8, -1):
        x0, y0 = _lonlat_to_tile(west, north, z)
        x1, y1 = _lonlat_to_tile(east, south, z)
        if (x1 - x0 + 1) * (y1 - y0 + 1) <= 6:
            zoom = z
            break

    x0, y0 = _lonlat_to_tile(west, north, zoom)
    x1, y1 = _lonlat_to_tile(east, south, zoom)

    tile_m = 40075016.68 / (2.0**zoom)
    tile_px = 256

    cols = x1 - x0 + 1
    rows = y1 - y0 + 1
    mosaic = Image.new("RGB", (cols * tile_px, rows * tile_px))

    session = requests.Session()
    for dx in range(cols):
        for dy in range(rows):
            tx = x0 + dx
            ty = y0 + dy
            url = f"{BASEMAP_TILE_URL}/{zoom}/{ty}/{tx}.png"
            try:
                resp = session.get(url, timeout=10)
                if resp.status_code == 200:
                    tile_img = Image.open(BytesIO(resp.content)).convert("RGB")
                    mosaic.paste(tile_img, (dx * tile_px, dy * tile_px))
            except Exception:
                continue

    # Crop mosaic to exact ROI extent in EPSG:3857 pixel coordinates
    origin_x = -20037508.34 + x0 * tile_m
    origin_y = 20037508.34 - y0 * tile_m

    px = int((roi_w - origin_x) / tile_m * tile_px)
    py = int((origin_y - roi_n) / tile_m * tile_px)
    pw = int((roi_e - roi_w) / tile_m * tile_px)
    ph = int((roi_n - roi_s) / tile_m * tile_px)

    if pw > 0 and ph > 0:
        mosaic = mosaic.crop((px, py, px + pw, py + ph))

    os.makedirs(cache_dir, exist_ok=True)
    mosaic.save(cache_path)
    return cache_path


def _composite_thermal(basemap_path: str, thermal_path: str, output_path: str) -> str:
    """Overlay RGBA thermal crop on basemap with attribution text."""
    basemap = Image.open(basemap_path).convert("RGBA")
    thermal = Image.open(thermal_path).convert("RGBA")
    basemap = basemap.resize(thermal.size, Image.LANCZOS)
    comp = Image.alpha_composite(basemap, thermal)
    comp = comp.convert("RGB")

    w, h = comp.size
    bar_h = max(int(h * 0.025), 14)
    overlay = Image.new("RGBA", (w, bar_h), (0, 0, 0, 140))
    comp.paste(overlay, (0, h - bar_h), overlay)

    from PIL import ImageDraw

    draw = ImageDraw.Draw(comp)
    text = "Basemap (C) ESRI, made with material from Copernicus Sentinel"
    font_size = max(int(bar_h * 0.55), 8)
    try:
        from PIL import ImageFont

        font = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", font_size
        )
    except Exception:
        font = ImageFont.load_default()
    _, _, tw, th = draw.textbbox((0, 0), text, font=font)
    tx = (w - tw) // 2
    ty = h - bar_h + (bar_h - th) // 2
    draw.text((tx, ty), text, fill=(200, 200, 200), font=font)

    comp.save(output_path, "PNG")
    return output_path


def check_thermal_anomaly(
    ana_src_paths: List[str],
    roi_bbox_str: str,
    roi_name: str,
    base_acq_time: str,
    threshold_kelvin: float,
    apprise_url: str,
) -> bool:
    """Crop analtic S3 BT to ROI, check max BT against threshold, send Apprise alert."""
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tif")
    os.close(tmp_fd)
    try:
        if not crop_product(ana_src_paths, tmp_path, roi_bbox_str):
            return False
        ds = gdal.Open(tmp_path)
        if not ds:
            return False
        band = ds.GetRasterBand(1)
        stats = band.GetStatistics(True, True)
        ds = None
        max_temp = stats[1]
        if max_temp > threshold_kelvin:
            msg = (
                f"Thermal anomaly detected at {roi_name}\n"
                f"Max temperature: {max_temp:.1f}K ({max_temp - 273.15:.1f}C)\n"
                f"Threshold: {threshold_kelvin:.1f}K\n"
                f"Acquired: {base_acq_time}"
            )
            notify.send_notification(
                message=msg,
                title=f"Thermal Alert: {roi_name}",
                urls=apprise_url,
            )
            return True
        return False
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def post_to_bsky(
    config: Dict[str, Any],
    roi_name: str,
    product_type: str,
    acq_time: str,
    constellation: str,
    image_path: str,
) -> bool:
    """Posts the ROI image to Bluesky with functional hashtags."""
    username = config.get("roi_bsky_username")
    password = config.get("roi_bsky_pw")

    if not username or not password or "your-" in username:
        print("Bluesky credentials not configured correctly.", flush=True)
        return False

    try:
        client = Client()
        client.login(username, password)

        human_prod = get_human_name(product_type)

        # Build rich text with functional hashtags
        tb = client_utils.TextBuilder()
        tb.text(f"Updated {human_prod} image of {roi_name}\n")
        tb.text(f"Satellite: {constellation}\n")
        tb.text(f"Acquisition time: {acq_time}\n")
        tb.text("Made with material from Copernicus Sentinel\n\n")

        # Hashtag sequence: #OSINT, #satelliteimagery, #Sentinel, #<constellation>,
        # Copernicus, #<ROI-name>
        tb.tag("#OSINT", "OSINT")
        tb.text(" ")
        tb.tag("#satelliteimagery", "satelliteimagery")
        tb.text(" ")
        tb.tag("#Sentinel", "Sentinel")
        tb.text(" ")

        # constellation: remove spaces and hyphens
        const_tag = constellation.replace(" ", "").replace("-", "")
        tb.tag(f"#{const_tag}", const_tag)
        tb.text(" ")

        tb.tag("#Copernicus", "Copernicus")
        tb.text(" ")

        # ROI name: replace spaces with _
        roi_tag = roi_name.replace(" ", "_")
        tb.tag(f"#{roi_tag}", roi_tag)

        with open(image_path, "rb") as f:
            img_data = f.read()
            upload = client.upload_blob(img_data)

        from atproto_client.models.app.bsky.embed.images import (  # type: ignore
            Main,
            Image as BskyImage,
        )

        embed = Main(
            images=[
                BskyImage(
                    alt=f"{human_prod} image of {roi_name} ({acq_time})",
                    image=upload.blob,
                )
            ]
        )

        client.send_post(text=tb, embed=embed)
        print(f"Successfully posted {roi_name} to Bluesky.", flush=True)
        return True
    except Exception as e:
        print(f"Error posting to Bluesky: {e}", flush=True)
        return False


def run_roi_stage(
    process_all: bool = False,
    roi_filter: Optional[str] = None,
    date_filter: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """
    Main entry point for the ROI stage.
    Groups inventory by acquisition time and product type to handle multi-tile orbits.
    Scans the inventory and processes ROI crops and social posts.

    Args:
        process_all: process every group in the inventory instead of only
            groups rendered since this run started.
        roi_filter: restrict processing to a single ROI (name from
            roi_config.json, case-insensitive, '_' treated as space).
        date_filter: restrict processing to acquisitions from this date
            (YYYY-MM-DD). Matching groups are processed even if they were
            not just rendered (re-run of a specific acquisition date).
        dry_run: print what would be done without writing crops, sidecars,
            or sending notifications/posts.
    """
    func.perf_logger.start_step("ROI Cropping Stage")

    config, rois = load_roi_config()
    if not rois:
        print("No ROI configurations found. Skipping.", flush=True)
        func.perf_logger.end_step()
        return 0

    if roi_filter:
        target = roi_filter.strip().replace("_", " ").lower()
        matched = []
        for r in rois:
            name = func.resolve_env_variable(r.get("name", "ROI")).replace("_", " ")
            if name.lower() == target:
                matched.append(r)
        if not matched:
            available = ", ".join(
                func.resolve_env_variable(r.get("name", "ROI")) for r in rois
            )
            print(
                f"No ROI matching '{roi_filter}'. Available ROIs: {available}",
                flush=True,
            )
            func.perf_logger.end_step()
            return 0
        rois = matched
        print(
            f"Restricting ROI processing to: {matched[0]['name']}",
            flush=True,
        )

    inventory_path = os.path.join(c.DIRS["OUT"], "visual/inventory.json")
    if not os.path.exists(inventory_path):
        print("Inventory not found. Skipping ROI stage.", flush=True)
        func.perf_logger.end_step()
        return 0

    try:
        with open(inventory_path, "r", encoding="utf-8") as f:
            inventory = json.load(f)
    except Exception as e:
        print(f"Error reading inventory: {e}", flush=True)
        func.perf_logger.end_step()
        return 0

    all_layers = inventory.get("layers", [])
    if not all_layers:
        print("No products found in inventory.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Group all layers by (Date, Orbit, Direction, Product)
    # Filter out ROI crops themselves to avoid recursion
    grouped_layers: Dict[
        Tuple[str, Optional[str], Optional[str], str], List[Dict[str, Any]]
    ] = {}
    for layer in all_layers:
        p_type = layer.get("product", "")
        if p_type.startswith("ROI-"):
            continue

        acq_time = layer.get("acquisition_time", "Unknown")
        # Extract date part for broader grouping if orbit info is available
        # ISO: 2026-05-24T10:00:00Z -> 2026-05-24
        date_part = acq_time[:10] if len(acq_time) >= 10 else acq_time

        rel_orbit = layer.get("relative_orbit")
        orbit_dir = layer.get("orbit_direction")

        # If we don't have orbit info, we fallback to acq_time (original logic)
        # but if we have it, we use the date + orbit + dir which is rock solid.
        key = (date_part if rel_orbit else acq_time, rel_orbit, orbit_dir, p_type)
        if key not in grouped_layers:
            grouped_layers[key] = []
        grouped_layers[key].append(layer)

    # Identify groups to process
    groups_to_process = []
    run_start_dt = datetime.fromtimestamp(func.perf_logger.start_time, tz=timezone.utc)

    for key, layers in grouped_layers.items():
        # Restrict to the requested acquisition date. Uses the layer's
        # acquisition_time (works whether the group key is date+orbit or a
        # full timestamp when orbit info is missing).
        if date_filter:
            group_date = layers[0].get("acquisition_time", "")[:10]
            if group_date != date_filter:
                continue

        if process_all or date_filter:
            groups_to_process.append((key, layers))
        else:
            is_dirty = False
            for layer in layers:
                render_time_str = layer.get("render_time", "")
                if not render_time_str:
                    continue
                try:
                    render_dt = datetime.fromisoformat(
                        render_time_str.replace("Z", "+00:00")
                    )
                    if render_dt >= run_start_dt:
                        is_dirty = True
                        break
                except Exception:
                    continue
            if is_dirty:
                groups_to_process.append((key, layers))

    if not groups_to_process:
        if date_filter:
            print(
                f"No products found for acquisition date {date_filter}.",
                flush=True,
            )
        else:
            print("No new products found for ROI processing.", flush=True)
        func.perf_logger.end_step()
        return 0

    # Sort groups by acquisition time (oldest first) to ensure chronological processing
    # (i.e. oldest events are processed and posted first)
    groups_to_process.sort(
        key=lambda x: min(l.get("acquisition_time", "9999") for l in x[1])
    )

    print(
        f"Checking {len(groups_to_process)} product groups against {len(rois)} ROIs.",
        flush=True,
    )

    bsky_post_enabled = config.get("roi_bsky_post", False)
    bsky_roi_names = config.get("roi_bsky_names", [])

    crops_created = 0
    for (date_part, rel_orbit, orbit_dir, product_type), layers in groups_to_process:
        constellation = "Sentinel-2" if product_type.startswith("S2") else "Sentinel-1"

        # Use the earliest acquisition time in the group for the filename
        # to keep it consistent.
        base_acq_time = min(l.get("acquisition_time", "Unknown") for l in layers)
        iso_clean = base_acq_time.replace(":", "")

        # Aggregate metadata and paths for the group
        src_paths = [
            os.path.join(c.DIRS["OUT"], l["path"])
            for l in layers
            if os.path.exists(os.path.join(c.DIRS["OUT"], l["path"]))
        ]
        if not src_paths:
            continue

        # Cloud cover (average)
        cc_vals = [l["cloud_cover"] for l in layers if l.get("cloud_cover") is not None]
        avg_cloud_cover = sum(cc_vals) / len(cc_vals) if cc_vals else None

        # Resolution (max)
        resolution = max(l.get("resolution", 0) for l in layers)

        for roi in rois:
            roi_name_raw = roi.get("name", "ROI")
            # Resolve env vars but DON'T force lowercase
            roi_name = func.resolve_env_variable(roi_name_raw).replace("_", " ")

            roi_bbox = func.resolve_env_variable(roi.get("bbox", ""))
            roi_match_threshold = roi.get("bbox_match", 0)
            desired_products = roi.get("products", [])

            # Check if this product is desired by the ROI
            match_found = False
            for dp in desired_products:
                dp_up = dp.upper()
                pt_up = product_type.upper()
                # Direct match (e.g., "TCI" in "S2-TCI")
                if dp_up in pt_up:
                    match_found = True
                    break
                # Flexible match for RATIO variants (e.g., config RATIOVVVH vs inventory RATIO)
                dp_norm = dp_up.replace("RATIOVVVH", "RATIO")
                pt_norm = pt_up.replace("RATIOVVVH", "RATIO")
                if dp_norm in pt_norm:
                    match_found = True
                    break
                # Normalize hyphens/underscores for GF variants (e.g., "TCI-GF" vs "S2-TCI_GF")
                dp_norm2 = dp_up.replace("-", "_")
                pt_norm2 = pt_up.replace("-", "_")
                if dp_norm2 in pt_norm2:
                    match_found = True
                    break

            # Per-ROI AIS: does this ROI want an AIS overlay for this base product?
            wants_ais_for_base = False
            if product_type in AIS_BASE_MAP:
                wants_ais_for_base = _roi_wants_ais(roi, product_type)

            if not match_found and not wants_ais_for_base:
                continue

            # Optimization: Check if any single tile in the group satisfies the ROI
            west, south, east, north = map(float, roi_bbox.split(","))
            roi_poly = box(west, south, east, north)

            best_src_paths = None
            effective_coverage = 0.0

            # Find best single tile coverage first
            max_single_coverage = 0.0
            best_single_layer = None
            for layer in layers:
                cov = calculate_coverage(roi_bbox, [layer], roi_poly=roi_poly)
                if cov > max_single_coverage:
                    max_single_coverage = cov
                    best_single_layer = layer

            if max_single_coverage >= roi_match_threshold and best_single_layer:
                best_src_paths = [
                    os.path.join(c.DIRS["OUT"], best_single_layer["path"])
                ]
                effective_coverage = max_single_coverage
            else:
                # If no single tile fits, check combined coverage
                combined_coverage = calculate_coverage(
                    roi_bbox, layers, roi_poly=roi_poly
                )
                if combined_coverage >= roi_match_threshold:
                    best_src_paths = src_paths
                    effective_coverage = combined_coverage

            if best_src_paths:
                # Precompute footprint_geom for reuse (base and per-ROI AIS)
                sat_val = layers[0].get("satellite")
                parent_layers_fp = [best_single_layer] if (best_single_layer and max_single_coverage >= roi_match_threshold) else layers
                footprint_geom = None
                try:
                    _geoms = []
                    for _l in parent_layers_fp:
                        _fp = _l.get("footprint")
                        if _fp:
                            try:
                                _geoms.append(shape(_fp))
                            except Exception:
                                pass
                        else:
                            _b = _l.get("bounds")
                            if _b:
                                try:
                                    _geoms.append(box(_b[0][1], _b[0][0], _b[1][1], _b[1][0]))
                                except Exception:
                                    pass
                    if _geoms:
                        _union = unary_union(_geoms)
                        _inter = _union.intersection(roi_poly)
                        if not _inter.is_empty:
                            footprint_geom = _inter
                except Exception:
                    footprint_geom = None

                # --- Base product crop (if ROI explicitly wants this product_type) ---
                base_dst_path = None
                base_p_suffix = (
                    product_type.split("-", 1)[1]
                    if "-" in product_type
                    else product_type
                )
                base_dst_filename = f"{roi_name}_{base_p_suffix}_{iso_clean}.tif"
                base_dst_path_tmp = os.path.join(c.DIRS["VIS_ROI"], base_dst_filename)

                if match_found:
                    print(
                        f"ROI Match: {roi_name} ({effective_coverage:.1f}%) -> "
                        f"Cropping {base_dst_filename} ({len(best_src_paths)} tiles)",
                        flush=True,
                    )
                    if dry_run:
                        detail = "crop and sidecar"
                        if product_type.startswith("S3-"):
                            if roi.get("thermal_monitor", False):
                                detail += (
                                    ", thermal anomaly check "
                                    "(notifications only if anomaly detected)"
                                )
                            else:
                                detail += (
                                    ", no notifications "
                                    "(S3 products skip posting unless thermal "
                                    "monitoring is enabled)"
                                )
                        else:
                            detail += (
                                ", notifications "
                                "(Apprise/Bluesky per ROI config)"
                            )
                        print(
                            f"  [dry-run] would {detail} for {base_dst_filename}",
                            flush=True,
                        )
                        # Do not continue; still evaluate per-ROI AIS fallback for dry-run
                        base_dst_path = base_dst_path_tmp
                    else:
                        if not crop_product(best_src_paths, base_dst_path_tmp, roi_bbox):
                            base_dst_path = None
                        else:
                            meta.generate_sidecar(
                                base_dst_path_tmp,
                                f"ROI-{roi_name}-{base_p_suffix}",
                                product_type,
                                effective_res=resolution,
                                cloud_cover=avg_cloud_cover,
                                relative_orbit=rel_orbit,
                                orbit_direction=orbit_dir,
                                satellite=sat_val,
                                footprint=footprint_geom,
                            )
                            crops_created += 1
                            base_dst_path = base_dst_path_tmp

                            # Thermal monitoring: check S3-BT analytic data for hot spots
                            thermal_alert = False
                            thermal_checked = False
                            if product_type.startswith("S3-"):
                                thermal_enabled = roi.get("thermal_monitor", False)
                                if thermal_enabled:
                                    thermal_checked = True
                                    threshold = roi.get("thermal_threshold", 310.0)
                                    ana_src = [
                                        p.replace("/visual/", "/analytic/") for p in best_src_paths
                                    ]
                                    ana_src = [p for p in ana_src if os.path.exists(p)]
                                    if ana_src:
                                        thermal_alert = check_thermal_anomaly(
                                            ana_src,
                                            roi_bbox,
                                            roi_name,
                                            base_acq_time,
                                            threshold,
                                            roi.get("apprise_url", ""),
                                        )

                            # Delete all-transparent FIRE crops when no thermal anomaly
                            if "FIRE" in product_type and thermal_checked and not thermal_alert:
                                if os.path.exists(base_dst_path_tmp):
                                    os.remove(base_dst_path_tmp)
                                json_path = base_dst_path_tmp.replace(".tif", ".json")
                                if os.path.exists(json_path):
                                    os.remove(json_path)
                                crops_created -= 1
                                base_dst_path = None
                            else:
                                # Apprise and Bluesky posting for base product
                                apprise_url = roi.get("apprise_url", "")
                                social_needed = bsky_post_enabled and roi_name_raw in bsky_roi_names

                                # Skip image posts for S3 thermal products unless anomaly detected
                                if product_type.startswith("S3-") and not thermal_alert:
                                    apprise_url = ""
                                    social_needed = False
                                    func.perf_logger.log_info(
                                        f"Skipping Apprise/Bluesky for {roi_name} {product_type}: "
                                        "no thermal anomaly detected"
                                    )

                                # Composite thermal crop on basemap for anomaly posts
                                post_path = base_dst_path_tmp
                                if thermal_alert and product_type.startswith("S3-"):
                                    basemap_path = fetch_roi_basemap(roi_bbox, roi_name)
                                    if basemap_path:
                                        comp_path = base_dst_path_tmp.replace(".tif", "_comp.png")
                                        post_path = _composite_thermal(
                                            basemap_path, base_dst_path_tmp, comp_path
                                        )

                                social_base = os.path.join(
                                    c.DIRS["VIS_ROI"],
                                    f"{roi_name}_{base_p_suffix}_{iso_clean}_social",
                                )

                                # Apprise gets full size JPEG
                                if apprise_url:
                                    func.perf_logger.log_info(
                                        f"Creating full-size JPEG for {roi_name} {base_p_suffix}"
                                    )
                                    full_image = create_full_image(post_path, social_base)
                                    if full_image:
                                        human_prod = get_human_name(product_type)
                                        msg = (
                                            f"New {human_prod} image for ROI {roi_name}\n"
                                            f"Acquired: {base_acq_time}\n"
                                            f"Satellite: {constellation}"
                                        )
                                        func.perf_logger.log_info(
                                            f"Sending Apprise notification for {roi_name} {base_p_suffix}"
                                        )
                                        notify.send_notification(
                                            message=msg,
                                            title=f"ROI Update: {roi_name}",
                                            urls=apprise_url,
                                            attachment=full_image,
                                        )
                                    else:
                                        func.perf_logger.log_info(
                                            f"Failed to create full-size JPEG for {roi_name} {base_p_suffix}"
                                        )

                                # Bluesky gets downscaled social image
                                if social_needed:
                                    image_path = create_social_image(post_path, social_base)
                                    if image_path:
                                        post_to_bsky(
                                            config,
                                            roi_name,
                                            product_type,
                                            base_acq_time,
                                            constellation,
                                            image_path,
                                        )
                # --- Per-ROI AIS fallback (if ROI wants AIS for this base type) ---
                if wants_ais_for_base:
                    ais_product_type = AIS_BASE_MAP[product_type]
                    ais_p_suffix = ais_product_type.split("-", 1)[1]
                    dst_ais_filename = f"{roi_name}_{ais_p_suffix}_{iso_clean}.tif"
                    dst_ais_path = os.path.join(c.DIRS["VIS_ROI"], dst_ais_filename)

                    # Check if whole-scene AIS already covers this ROI
                    ais_key = (date_part, rel_orbit, orbit_dir, ais_product_type)
                    ais_layers = grouped_layers.get(ais_key, [])
                    global_covers = False
                    global_cov = 0.0
                    if ais_layers:
                        try:
                            global_cov = calculate_coverage(roi_bbox, ais_layers, roi_poly=roi_poly)
                            if global_cov >= roi_match_threshold:
                                global_covers = True
                        except Exception:
                            global_covers = False

                    if global_covers:
                        if dry_run:
                            print(
                                f"  [dry-run] ROI {roi_name} AIS already covered by global {ais_product_type} ({global_cov:.1f}%), would crop {dst_ais_filename} via global product",
                                flush=True,
                            )
                        else:
                            print(
                                f"ROI {roi_name} AIS already covered by global {ais_product_type} ({global_cov:.1f}%), skipping per-ROI correlation (global crop will be handled via {ais_product_type} group).",
                                flush=True,
                            )
                    else:
                        # GF priority: S2-TCI-GF takes precedence over S2-TCI for AIS
                        if product_type == "S2-TCI":
                            gf_key = (date_part, rel_orbit, orbit_dir, "S2-TCI-GF")
                            gf_layers = grouped_layers.get(gf_key, [])
                            if gf_layers:
                                try:
                                    gf_cov = calculate_coverage(roi_bbox, gf_layers, roi_poly=roi_poly)
                                    if gf_cov >= roi_match_threshold:
                                        if dry_run:
                                            print(
                                                f"  [dry-run] ROI {roi_name} AIS for S2-TCI skipped, S2-TCI-GF has priority (filtered, {gf_cov:.1f}% >= {roi_match_threshold}%)",
                                                flush=True,
                                            )
                                        else:
                                            print(
                                                f"ROI {roi_name} AIS for S2-TCI skipped, S2-TCI-GF has priority.",
                                                flush=True,
                                            )
                                        continue
                                except Exception:
                                    pass
                        # Need per-ROI AIS - only the ROI bbox
                        if dry_run:
                            print(
                                f"  [dry-run] would run per-ROI AIS correlation for {roi_name} ({ais_product_type}) on ROI bbox only -> {dst_ais_filename} (global {ais_product_type} coverage {global_cov:.1f}% < {roi_match_threshold}%)",
                                flush=True,
                            )
                        else:
                            if os.path.exists(dst_ais_path):
                                print(
                                    f"Per-ROI AIS {dst_ais_filename} already exists, skipping.",
                                    flush=True,
                                )
                            else:
                                # Determine base image for AIS overlay
                                base_path_for_ais = None
                                base_is_temp = False
                                if base_dst_path and os.path.exists(base_dst_path):
                                    base_path_for_ais = base_dst_path
                                elif match_found and os.path.exists(base_dst_path_tmp):
                                    base_path_for_ais = base_dst_path_tmp
                                if base_path_for_ais is None or not os.path.exists(base_path_for_ais):
                                    # Create temp base crop solely for AIS correlation
                                    tmp_base_path = base_dst_path_tmp
                                    if not os.path.exists(tmp_base_path):
                                        if not crop_product(best_src_paths, tmp_base_path, roi_bbox):
                                            print(
                                                f"Per-ROI AIS: failed to create temp base crop for {roi_name} {product_type}",
                                                flush=True,
                                            )
                                            tmp_base_path = None
                                    if tmp_base_path and os.path.exists(tmp_base_path):
                                        base_path_for_ais = tmp_base_path
                                        base_is_temp = not match_found
                                    else:
                                        base_path_for_ais = None

                                if base_path_for_ais and os.path.exists(base_path_for_ais):
                                    try:
                                        import ais_correlator

                                        # Use ROI crop's sidecar/bounds for AIS query (ROI bbox only)
                                        meta_ais = ais_correlator.get_metadata(base_path_for_ais)
                                        ais_data = ais_correlator.fetch_ais_data(meta_ais)
                                        if not ais_data:
                                            print(
                                                f"No AIS data for per-ROI {roi_name} {ais_product_type} ({base_acq_time}, ROI bbox)",
                                                flush=True,
                                            )
                                            if base_is_temp and base_path_for_ais and os.path.exists(base_path_for_ais):
                                                try:
                                                    os.remove(base_path_for_ais)
                                                    jp = base_path_for_ais.replace(".tif", ".json")
                                                    if os.path.exists(jp):
                                                        os.remove(jp)
                                                except Exception:
                                                    pass
                                        else:
                                            ais_correlator.plot_on_image(
                                                base_path_for_ais, ais_data, meta_ais["time"]
                                            )
                                            intermediate = base_path_for_ais.replace(".tif", "_AIS.tif")
                                            if not os.path.exists(intermediate):
                                                print(
                                                    f"Per-ROI AIS: plot failed, no {intermediate}",
                                                    flush=True,
                                                )
                                            else:
                                                # Move/rename intermediate to final hyphen-named product
                                                if intermediate != dst_ais_path:
                                                    if os.path.exists(dst_ais_path):
                                                        try:
                                                            os.remove(dst_ais_path)
                                                        except Exception:
                                                            pass
                                                        jp2 = dst_ais_path.replace(".tif", ".json")
                                                        if os.path.exists(jp2):
                                                            try:
                                                                os.remove(jp2)
                                                            except Exception:
                                                                pass
                                                    try:
                                                        os.rename(intermediate, dst_ais_path)
                                                    except Exception as e:
                                                        print(
                                                            f"Per-ROI AIS rename failed {intermediate} -> {dst_ais_path}: {e}",
                                                            flush=True,
                                                        )
                                                        dst_ais_path = intermediate
                                                else:
                                                    dst_ais_path = intermediate

                                                if os.path.exists(dst_ais_path):
                                                    cog.convert_to_cog(dst_ais_path)
                                                    cog.ensure_overviews(dst_ais_path)
                                                    meta.generate_sidecar(
                                                        dst_ais_path,
                                                        f"ROI-{roi_name}-{ais_p_suffix}",
                                                        ais_product_type,
                                                        effective_res=resolution,
                                                        cloud_cover=avg_cloud_cover,
                                                        relative_orbit=rel_orbit,
                                                        orbit_direction=orbit_dir,
                                                        satellite=sat_val,
                                                        footprint=footprint_geom,
                                                    )
                                                    crops_created += 1
                                                    print(
                                                        f"Per-ROI AIS created: {dst_ais_filename} ({len(ais_data)} tracks)",
                                                        flush=True,
                                                    )

                                                    if base_is_temp and base_path_for_ais != dst_ais_path and os.path.exists(base_path_for_ais):
                                                        try:
                                                            os.remove(base_path_for_ais)
                                                            jp = base_path_for_ais.replace(".tif", ".json")
                                                            if os.path.exists(jp):
                                                                os.remove(jp)
                                                        except Exception:
                                                            pass

                                                    # Notifications for per-ROI AIS
                                                    apprise_url_ais = roi.get("apprise_url", "")
                                                    social_needed_ais = bsky_post_enabled and roi_name_raw in bsky_roi_names
                                                    social_base_ais = os.path.join(
                                                        c.DIRS["VIS_ROI"],
                                                        f"{roi_name}_{ais_p_suffix}_{iso_clean}_social",
                                                    )
                                                    if apprise_url_ais:
                                                        func.perf_logger.log_info(
                                                            f"Creating full-size JPEG for {roi_name} {ais_p_suffix} (per-ROI AIS)"
                                                        )
                                                        full_image_ais = create_full_image(
                                                            dst_ais_path, social_base_ais
                                                        )
                                                        if full_image_ais:
                                                            human_prod_ais = get_human_name(ais_product_type)
                                                            msg_ais = (
                                                                f"New {human_prod_ais} image for ROI {roi_name} (AIS per-ROI)\n"
                                                                f"Acquired: {base_acq_time}\n"
                                                                f"Satellite: {constellation}"
                                                            )
                                                            func.perf_logger.log_info(
                                                                f"Sending Apprise notification for {roi_name} {ais_p_suffix} (per-ROI AIS)"
                                                            )
                                                            notify.send_notification(
                                                                message=msg_ais,
                                                                title=f"ROI Update: {roi_name} {ais_p_suffix}",
                                                                urls=apprise_url_ais,
                                                                attachment=full_image_ais,
                                                            )
                                                    if social_needed_ais:
                                                        image_path_ais = create_social_image(
                                                            dst_ais_path, social_base_ais
                                                        )
                                                        if image_path_ais:
                                                            post_to_bsky(
                                                                config,
                                                                roi_name,
                                                                ais_product_type,
                                                                base_acq_time,
                                                                constellation,
                                                                image_path_ais,
                                                            )
                                                else:
                                                    print(
                                                        f"Per-ROI AIS final file missing: {dst_ais_path}",
                                                        flush=True,
                                                    )
                                    except Exception as e:
                                        print(
                                            f"Per-ROI AIS correlation failed for {roi_name} {ais_product_type}: {e}",
                                            flush=True,
                                        )
                                        import traceback

                                        traceback.print_exc()
                                        if base_is_temp and base_path_for_ais and os.path.exists(base_path_for_ais):
                                            try:
                                                os.remove(base_path_for_ais)
                                                jp = base_path_for_ais.replace(".tif", ".json")
                                                if os.path.exists(jp):
                                                    os.remove(jp)
                                            except Exception:
                                                pass
                                else:
                                    print(
                                        f"Per-ROI AIS: no base image available for {roi_name} {product_type}",
                                        flush=True,
                                    )

    # --- S1 Delta per-ROI (with combining, orbit-matched, via products[] DELTA) ---
    # Runs after normal crops, so new S1 visuals are already in inventory if needed, but delta uses VV analytics
    delta_rois = [r for r in rois if _roi_wants_delta(r)]
    if delta_rois:
        # Build S1 groups from inventory (like groups_to_process but for S1 only, using coverage)
        # Use all_layers, not just new groups, to find history for pairing
        # Filter S1 layers that have a VV analytic (via S1-VV or S1-RATIO proxy)
        s1_layers_all = [l for l in all_layers if l.get("product") in ("S1-VV", "S1-RATIO")]
        # Prefer S1-RATIO over S1-VV for the same date+orbit to avoid self-pairs
        ratio_dates = {(l.get("acquisition_time", "")[:10], l.get("orbit_direction")) for l in s1_layers_all if l.get("product") == "S1-RATIO"}
        s1_layers_all = [l for l in s1_layers_all if l.get("product") == "S1-RATIO" or (l.get("acquisition_time", "")[:10], l.get("orbit_direction")) not in ratio_dates]
        print(f"  DELTA: {len(delta_rois)} ROI(s) want delta, {len(s1_layers_all)} S1-VV/RATIO layers in inventory.", flush=True)
        # Group S1 layers by (date, orbit) for coverage testing (like ROI manager groups)
        # For delta we need per-date groups, so rebuild grouping similar to earlier but S1-specific
        s1_grouped: Dict[Tuple[str, Optional[str], Optional[str], str], List[Dict[str, Any]]] = {}
        for layer in s1_layers_all:
            p_type = layer.get("product", "")
            if p_type.startswith("ROI-"):
                continue
            acq = layer.get("acquisition_time", "Unknown")
            date_part = acq[:10] if len(acq) >= 10 else acq
            rel = layer.get("relative_orbit") or "unknown"
            odir = layer.get("orbit_direction")
            key = (date_part, rel, odir, p_type)
            s1_grouped.setdefault(key, []).append(layer)
        for roi in delta_rois:
            roi_name_raw = roi.get("name", "ROI")
            roi_name = func.resolve_env_variable(roi_name_raw).replace("_", " ")
            # Respect roi_filter
            if roi_filter:
                target = roi_filter.strip().replace("_", " ").lower()
                if roi_name.lower() != target:
                    continue
            roi_bbox = func.resolve_env_variable(roi.get("bbox", ""))
            if not roi_bbox:
                continue
            thresh = roi.get("bbox_match", 90)
            try:
                west, south, east, north = map(float, roi_bbox.split(","))
                roi_poly = box(west, south, east, north)
            except Exception:
                continue
            # Find all S1 date groups that cover this ROI (with combining)
            covered_groups: List[Tuple[str, Optional[str], Optional[str], List[Dict[str, Any]], List[str], float, str]] = []
            # Each entry: (date_part, rel, odir, layers, ana_paths, coverage, base_acq_time)
            for (date_part, rel, odir, p_type), layers in s1_grouped.items():
                # Only consider S1-VV/RATIO groups
                if p_type not in ("S1-VV", "S1-RATIO"):
                    continue
                best_paths, cov = _find_best_s1_paths_for_group(roi_bbox, roi_poly, layers, thresh)
                if best_paths is None:
                    continue
                base_acq = min(l.get("acquisition_time", "9999") for l in layers)
                # Age filter 14 days
                try:
                    acq_dt = datetime.fromisoformat(base_acq.replace("Z", "+00:00"))
                    if datetime.now(timezone.utc) - acq_dt > timedelta(days=c.S1_DELTA_DAYS):
                        continue
                except Exception:
                    continue
                covered_groups.append((date_part, rel, odir, layers, best_paths, cov, base_acq))
                # Find VH paths for same date+orbit for masking (optional)
                vh_paths: List[str] = []
                # Find VH analytic for same date+orbit
                for l in s1_layers_all:
                    if l.get("product") == "S1-VH" and l.get("acquisition_time", "")[:10] == base_acq[:10] and l.get("orbit_direction") == odir and str(l.get("relative_orbit")) == str(rel):
                        base_vh = os.path.basename(l.get("path", ""))
                        ana_vh = os.path.join(c.DIRS["ANA_S1_VH"], base_vh)
                        if not os.path.exists(ana_vh):
                            alt = os.path.join(c.DIRS["OUT"], l.get("path", "")).replace("/visual/", "/analytic/")
                            if os.path.exists(alt):
                                ana_vh = alt
                            else:
                                continue
                        vh_paths.append(ana_vh)
                # Also try to find VH via same group if p_type is RATIO and VH exists with same time
                if not vh_paths:
                    # Fallback: look for any VH with same acquisition_time
                    for l in s1_layers_all:
                        if l.get("product") == "S1-VH" and l.get("acquisition_time") == base_acq:
                            base_vh = os.path.basename(l.get("path", ""))
                            ana_vh = os.path.join(c.DIRS["ANA_S1_VH"], base_vh)
                            if os.path.exists(ana_vh):
                                vh_paths.append(ana_vh)
            if len(covered_groups) < 2:
                continue
            # Group by orbit for pairing (relative_orbit may be None -> use unknown)
            by_orbit: Dict[Tuple[str, str], List[Tuple[str, List[str], List[str], float, str]]] = {}
            # Actually need to group covered_groups by orbit
            from collections import defaultdict as _dd
            orbit_groups: Dict[Tuple[str, str], List[Any]] = _dd(list)
            for date_part, rel, odir, layers_g, best_paths, cov, base_acq in covered_groups:
                key_orbit = (str(rel) if rel is not None else "unknown", (odir or "unknown").upper())
                orbit_groups[key_orbit].append((base_acq, best_paths, vh_paths, cov, date_part, rel, odir))
            for (rel_key, odir_key), grp in orbit_groups.items():
                grp.sort(key=lambda x: x[0])  # by base_acq
                # Generate delta for each consecutive pair where newer is new (dirty) and output missing
                for i in range(1, len(grp)):
                    base_acq_old, paths_old, vh_old, cov_old, date_old, rel_old, odir_old = grp[i - 1]
                    base_acq_new, paths_new, vh_new, cov_new, date_new, rel_new, odir_new = grp[i]
                    # Only generate for newest pair where newer is recent (within 14d and new render)
                    # Check if newer group was in groups_to_process or is dirty
                    # Find if any layer in newer group is dirty (render_time >= run_start)
                    is_new_dirty = False
                    # Find the original group layers for newer date
                    # Use s1_grouped to find layers for this date_orbit
                    # Simpler: check if any s1 layer for this base_acq has render_time >= run_start
                    for l in s1_layers_all:
                        if l.get("acquisition_time") == base_acq_new:
                            rt = l.get("render_time", "")
                            try:
                                rdt = datetime.fromisoformat(rt.replace("Z", "+00:00"))
                                if rdt >= run_start_dt:
                                    is_new_dirty = True
                                    break
                            except Exception:
                                pass
                    if not is_new_dirty and not process_all and not date_filter:
                        continue
                    iso_clean = base_acq_new.replace(":", "")
                    safe_roi = roi_name.replace(" ", "_")
                    dst_filename = f"{safe_roi}_DELTA_{iso_clean}.tif"
                    dst_path = os.path.join(c.DIRS["VIS_ROI"], dst_filename)
                    if os.path.exists(dst_path):
                        continue
                    if dry_run:
                        print(f"ROI Delta: {roi_name} {rel_key} {odir_key} {base_acq_old} -> {base_acq_new} ({cov_new:.1f}%) -> would create {dst_filename} (combining {len(paths_new)}+{len(paths_old)} VV tiles)", flush=True)
                        continue
                    print(f"ROI Delta: {roi_name} {rel_key} {odir_key} {base_acq_old} -> {base_acq_new} ({cov_new:.1f}%) -> Creating {dst_filename} ({len(paths_new)}+{len(paths_old)} VV tiles)", flush=True)
                    # Compute delta: need to handle mosaic of multiple VV paths per date
                    # Paths_new/old are lists of VV analytic paths (1 or 2 tiles)
                    # For VH masking, we need corresponding VH paths for newer date
                    # Find VH paths for newer date specifically
                    vh_for_new: List[str] = []
                    # Re-derive VH for newer date
                    for l in s1_layers_all:
                        if l.get("product") == "S1-VH" and l.get("acquisition_time") == base_acq_new and (l.get("orbit_direction") or "").upper() == odir_key:
                            base_vh = os.path.basename(l.get("path", ""))
                            ana_vh = os.path.join(c.DIRS["ANA_S1_VH"], base_vh)
                            if os.path.exists(ana_vh):
                                vh_for_new.append(ana_vh)
                    # If no VH found for exact orbit, try any VH for that date
                    if not vh_for_new:
                        for l in s1_layers_all:
                            if l.get("product") == "S1-VH" and l.get("acquisition_time") == base_acq_new:
                                base_vh = os.path.basename(l.get("path", ""))
                                ana_vh = os.path.join(c.DIRS["ANA_S1_VH"], base_vh)
                                if os.path.exists(ana_vh):
                                    vh_for_new.append(ana_vh)
                    ok = _compute_roi_delta(paths_new, paths_old, vh_for_new if vh_for_new else None, roi_bbox, dst_path, rel_new, odir_new, None)
                    if not ok:
                        continue
                    # Generate sidecar for ROI delta (like other ROI crops)
                    # Find resolution and satellite from newer group
                    res = 15.0
                    sat = None
                    for l in s1_layers_all:
                        if l.get("acquisition_time") == base_acq_new:
                            res = l.get("resolution", 15.0) or 15.0
                            sat = l.get("satellite")
                            break
                    meta.generate_sidecar(dst_path, f"ROI-{roi_name}-DELTA", "S1-DELTA", effective_res=res, relative_orbit=str(rel_new) if rel_new and rel_new != "unknown" else None, orbit_direction=odir_new, satellite=sat)
                    crops_created += 1
                    # Notifications for delta (like other ROI products)
                    apprise_url = roi.get("apprise_url", "")
                    social_needed = bsky_post_enabled and roi_name_raw in bsky_roi_names
                    # For delta, always notify if ROI has apprise/bsky and delta created (unlike S3 FIRE which skips)
                    if apprise_url or social_needed:
                        if dry_run:
                            print(f"  [dry-run] would notify {roi_name} DELTA {dst_filename}", flush=True)
                        else:
                            # Create images for notifications
                            social_base = os.path.join(c.DIRS["VIS_ROI"], f"{safe_roi}_DELTA_{iso_clean}_social")
                            if apprise_url:
                                func.perf_logger.log_info(f"Creating full-size JPEG for {roi_name} DELTA")
                                full_image = create_full_image(dst_path, social_base, stamp_acq_old=base_acq_old, stamp_acq_new=base_acq_new, stamp_orbit=odir_new)
                                if full_image:
                                    human_prod = get_human_name("S1-DELTA")
                                    msg = f"New {human_prod} image for ROI {roi_name}\nAcquired: {base_acq_new}\nSatellite: {sat or 'Sentinel-1'}\nDelta: {base_acq_old} -> {base_acq_new} ({rel_key} {odir_key})"
                                    func.perf_logger.log_info(f"Sending Apprise notification for {roi_name} DELTA")
                                    notify.send_notification(message=msg, title=f"ROI Update: {roi_name} DELTA", urls=apprise_url, attachment=full_image)
                            if social_needed:
                                img_path = create_social_image(dst_path, social_base, stamp_acq_old=base_acq_old, stamp_acq_new=base_acq_new, stamp_orbit=odir_new)
                                if img_path:
                                    post_to_bsky(config, roi_name, "S1-DELTA", base_acq_new, "Sentinel-1", img_path)
                    # Only one delta per ROI per orbit per run (newest pair)
                    break

    print(f"ROI stage complete. {crops_created} crops created/updated.", flush=True)
    if crops_created > 0:
        inventory_manager.rebuild_inventory()

    func.perf_logger.end_step()
    return crops_created


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone ROI Manager")
    parser.add_argument(
        "--all", action="store_true", help="Process all files in the inventory"
    )
    parser.add_argument(
        "--roi",
        type=str,
        default=None,
        help="Only process this ROI (name from roi_config.json, case-insensitive)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only process acquisitions from this date; re-runs the ROI stage "
        "for that date even if it was already processed",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended actions without writing crops or sending "
        "notifications/posts",
    )
    args = parser.parse_args()

    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            parser.error(f"--date must be YYYY-MM-DD, got '{args.date}'")

    # Start performance logger for standalone run
    func.perf_logger.start_run()
    run_roi_stage(
        process_all=args.all,
        roi_filter=args.roi,
        date_filter=args.date,
        dry_run=args.dry_run,
    )
    func.perf_logger.stop_run()
