#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions_s3.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Sentinel-3 SLSTR fire detection processing module.
Handles thermal band warping, BT composite, and fire detection rendering.
"""

import gc
import os
import re
import subprocess
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import numpy as np
import rasterio as rio
import warnings

from osgeo import gdal, osr
from scipy.spatial import cKDTree

gdal.UseExceptions()
warnings.filterwarnings("ignore", category=FutureWarning, module="osgeo")

from rasterio.enums import ColorInterp
import cog_finalizer as cog
import constants as c
import functions as func
import legends
import metadata_engine as meta

# S3 SLSTR band configuration: band_id -> (netcdf_suffix, geodetic_grid)
# Suffix: the part after "BT_" in the NetCDF filename
# Geodetic grid: the grid name for the tie-point file (geodetic_{grid}.nc)
_S3_BAND_CFG = {
    "S7": ("in", "in"),
    "S8": ("in", "in"),
    "S9": ("in", "in"),
    "F1": ("fn", "fn"),
    "F2": ("in", "in"),
}


def build_overviews_gdal(path: str) -> None:
    """Uses gdaladdo for memory-efficient overview building."""
    func.perf_logger.start_step(f"S3 Overviews: {os.path.basename(path)}")
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
    except Exception as e:
        print(f"Warning: gdaladdo failed for {path}: {e}", flush=True)
    func.perf_logger.end_step()


def get_time(name: str) -> Optional[str]:
    """Gets the production time from a Sentinel-3 dataset name."""
    result = re.search(r"S3._.*_(\d{8}T\d{6})_", name)
    return result.group(1) if result else None


def _s3_parse_manifest(manifest_path: str) -> Dict[str, str]:
    """Extract pass direction and platform from S3 xfdumanifest.xml."""
    result = {}
    try:
        tree = ET.parse(manifest_path)
        root = tree.getroot()
        ns = {"safe": "http://www.esa.int/safe/sentinel/1.1"}
        orbit = root.find(
            ".//safe:orbitNumber",
            ns,
        )
        if orbit is not None:
            direction = orbit.get("groundTrackDirection", "")
            result["pass_direction"] = direction.upper() if direction else None
    except Exception as e:
        print(f"WARNING: Could not parse manifest for orbit direction: {e}", flush=True)
    return result


def _s3_band_paths(product_dir: str, band: str) -> str:
    """Build GDAL NetCDF subdataset path for an SLSTR BT band."""
    suffix, _ = _S3_BAND_CFG[band]
    nc_file = f"{band}_BT_{suffix}.nc"
    nc_path = os.path.join(product_dir, nc_file)
    var_name = nc_file[:-3]
    return f'NETCDF:"{nc_path}":{var_name}'


def _build_kdtree_mapping_3857(
    lon_deg: np.ndarray,
    lat_deg: np.ndarray,
    target_srs: str = "EPSG:3857",
    resolution: float = 1000.0,
) -> tuple:
    """Build KD-tree from tie-point (lon, lat) -> (px, py) and compute 3857 output grid.

    Computes the output extent in 3857 from tie-point bounds, creates a regular
    grid at the specified resolution, converts each grid pixel center to (lon, lat)
    via the inverse projection, and queries the KD-tree for the nearest input pixel.

    Returns (px_map, py_map, meta_dict).
    """
    h, w = lat_deg.shape
    y_in, x_in = np.mgrid[0:h, 0:w]
    pts = np.column_stack((lon_deg.ravel(), lat_deg.ravel()))
    px_py = np.column_stack((x_in.ravel(), y_in.ravel()))

    valid = np.isfinite(pts[:, 0]) & np.isfinite(pts[:, 1])
    pts = pts[valid]
    px_py = px_py[valid]

    tree = cKDTree(pts)

    # Compute output extent in 3857 by sampling tie-point grid at step=50
    srs_4326 = osr.SpatialReference()
    srs_4326.ImportFromEPSG(4326)
    srs_tgt = osr.SpatialReference()
    srs_tgt.SetFromUserInput(target_srs)
    ct = osr.CoordinateTransformation(srs_4326, srs_tgt)

    margin = resolution * 2.0
    x_min = y_min = float("inf")
    x_max = y_max = float("-inf")
    for py in range(0, h, 50):
        for px in range(0, w, 50):
            try:
                x, y, _ = ct.TransformPoint(
                    float(lat_deg[py, px]), float(lon_deg[py, px])
                )
                x_min = min(x_min, x)
                x_max = max(x_max, x)
                y_min = min(y_min, y)
                y_max = max(y_max, y)
            except Exception:
                pass
    x_min -= margin
    x_max += margin
    y_min -= margin
    y_max += margin

    out_w = max(1, int((x_max - x_min) / resolution))
    out_h = max(1, int((y_max - y_min) / resolution))

    # Create output grid in target CRS
    oy, ox = np.mgrid[0:out_h, 0:out_w]
    out_x = x_min + (ox + 0.5) * resolution
    out_y = y_max - (oy + 0.5) * resolution

    # Batch-convert grid pixel centers to (lon, lat) via GDAL TransformPoints
    ct_inv = osr.CoordinateTransformation(srs_tgt, srs_4326)
    n_pixels = out_h * out_w
    out_lon = np.full(n_pixels, 1e100, dtype=np.float64)
    out_lat = np.full(n_pixels, 1e100, dtype=np.float64)

    chunk = 65536
    for start in range(0, n_pixels, chunk):
        end = min(start + chunk, n_pixels)
        xs = out_x.ravel()[start:end]
        ys = out_y.ravel()[start:end]
        batch = [(float(xs[i]), float(ys[i]), 0.0) for i in range(end - start)]
        results = ct_inv.TransformPoints(batch)
        for i, (lat, lon, _) in enumerate(results):
            out_lat[start + i] = lat
            out_lon[start + i] = lon

    # Query KD-tree for all output pixels
    # max_dist: distance threshold in (lon, lat) degrees (~0.015° ≈ 1.5km).
    # Pixels farther than this from any tie-point lie outside the curved SLSTR
    # swath boundary — marking them invalid prevents edge-pixel smearing.
    max_dist = 0.03
    qpts = np.column_stack((out_lon, out_lat))
    valid_q = (
        np.isfinite(qpts[:, 0])
        & np.isfinite(qpts[:, 1])
        & np.less(np.abs(qpts[:, 0]), 180)
    )
    dist, idx = tree.query(qpts[valid_q], k=1, distance_upper_bound=max_dist)

    # Initialize maps with -1 (invalid)
    px_map = np.full((out_h, out_w), -1, dtype=np.int32)
    py_map = np.full((out_h, out_w), -1, dtype=np.int32)
    in_swath = dist < max_dist
    px_map.ravel()[np.where(valid_q)[0][in_swath]] = px_py[idx[in_swath], 0].astype(
        np.int32
    )
    py_map.ravel()[np.where(valid_q)[0][in_swath]] = px_py[idx[in_swath], 1].astype(
        np.int32
    )

    meta = {
        "x_min": float(x_min),
        "y_max": float(y_max),
        "out_w": out_w,
        "out_h": out_h,
        "res": resolution,
        "srs_wkt": srs_tgt.ExportToWkt(),
    }
    return px_map, py_map, meta


def _read_bt_band(product_dir: str, band: str) -> np.ndarray:
    """Read a BT band from NetCDF and return Float32 array in Kelvin."""
    band_sd = _s3_band_paths(product_dir, band)
    ds = gdal.Open(band_sd)
    if ds is None:
        raise RuntimeError(f"Cannot open {band_sd}")
    scale = ds.GetRasterBand(1).GetScale() or 1.0
    offset = ds.GetRasterBand(1).GetOffset() or 0.0
    data = ds.ReadAsArray().astype(np.float32) * np.float32(scale) + np.float32(offset)
    ds = None
    data[data <= 0] = np.nan
    return data


def _read_tie_points(product_dir: str, grid: str) -> tuple:
    """Read tie-point arrays for a geodetic grid, return (lon_deg, lat_deg)."""
    geo_file = f"geodetic_{grid}.nc"
    geo_path = os.path.join(product_dir, geo_file)
    lat_sd = f'NETCDF:"{geo_path}":latitude_{grid}'
    lon_sd = f'NETCDF:"{geo_path}":longitude_{grid}'
    ds_lat = gdal.Open(lat_sd)
    ds_lon = gdal.Open(lon_sd)
    if ds_lat is None or ds_lon is None:
        raise RuntimeError(f"Cannot open tie points for grid {grid}")
    lat_deg = ds_lat.ReadAsArray().astype(np.float64) * 1e-6
    lon_deg = ds_lon.ReadAsArray().astype(np.float64) * 1e-6
    ds_lat = None
    ds_lon = None
    return lon_deg, lat_deg


def _s3_warp_bands_kdtree(
    product_dir: str,
    bands: List[str],
    out_path: str,
    target_srs: str = "EPSG:3857",
    resolution: float = 1000.0,
) -> bool:
    """Per-pixel KD-tree warp for multiple SLSTR BT bands directly to target CRS.

    Uses tie-point arrays to build an accurate (lon, lat) -> (px, py) mapping
    via KD-tree nearest-neighbor, avoiding the ~250km errors from GCP+TSP warps.

    Output grid pixels are computed directly in the target CRS — each pixel
    center is inverse-projected to (lon, lat) and the nearest tie-point pixel is
    sampled. No intermediate resampling cascade.

    Bands are stacked as separate layers in the output. All bands must share the
    same geodetic grid. Returns True on success.
    """
    _, grid = _S3_BAND_CFG[bands[0]]
    for b in bands:
        if _S3_BAND_CFG[b][1] != grid:
            raise ValueError(
                f"Bands {bands} must share the same geodetic grid, got mixed"
            )

    print(f"  Reading tie points (grid={grid})...", flush=True)
    lon_deg, lat_deg = _read_tie_points(product_dir, grid)
    print(
        f"  Building KD-tree mapping ({lon_deg.shape[1]}x{lon_deg.shape[0]} tie-points)...",
        flush=True,
    )
    px_map, py_map, meta = _build_kdtree_mapping_3857(
        lon_deg, lat_deg, target_srs, resolution
    )

    out_h = meta["out_h"]
    out_w = meta["out_w"]
    n_bands = len(bands)
    print(f"  Output grid: {out_w}x{out_h} pixels, {n_bands} band(s)", flush=True)

    # Stack all band data into one array
    stack = np.zeros((n_bands, out_h, out_w), dtype=np.float32)
    for i, band in enumerate(bands):
        print(f"    Sampling {band}...", flush=True)
        bt = _read_bt_band(product_dir, band)
        valid = (px_map >= 0) & (py_map >= 0)
        sampled = np.full((out_h, out_w), 0.0, dtype=np.float32)
        sampled[valid] = bt[py_map[valid], px_map[valid]]
        stack[i] = sampled

    # Write multi-band GeoTIFF in target CRS
    print(f"  Writing {target_srs} GeoTIFF...", flush=True)
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(
        out_path,
        out_w,
        out_h,
        n_bands,
        gdal.GDT_Float32,
        [
            "TILED=YES",
            "BLOCKXSIZE=256",
            "BLOCKYSIZE=256",
            "COMPRESS=DEFLATE",
            "BIGTIFF=YES",
        ],
    )
    ds.SetGeoTransform([meta["x_min"], meta["res"], 0, meta["y_max"], 0, -meta["res"]])
    ds.SetProjection(meta["srs_wkt"])
    for i in range(n_bands):
        band_ds = ds.GetRasterBand(i + 1)
        band_ds.WriteArray(stack[i])
        band_ds.SetNoDataValue(0)
    ds = None

    gc.collect()
    return True


def prepare(product_dir: str) -> None:
    """Reprojects S3 SLSTR thermal bands to EPSG:3857 at 1km resolution.

    Uses per-pixel KD-tree warp from tie-point arrays — each output pixel is
    directly computed from the nearest input pixel via (lon, lat) -> (px, py)
    mapping, eliminating the ~250 km error from GCP+TPS warps and the ~2K BT
    offset from intermediate 4326 resampling.
    """
    func.perf_logger.start_step("S3 Warp (EPSG:3857)")
    print("Reprojecting S3 SLSTR thermal bands to EPSG:3857...", flush=True)

    out_path = "/tmp/s3_bt.tif"

    success = _s3_warp_bands_kdtree(product_dir, ["S7", "S8", "S9"], out_path)

    if success:
        print(f"Thermal stack written to {out_path}", flush=True)
    else:
        print("WARNING: Could not build BT stack.", flush=True)

    gc.collect()
    func.perf_logger.end_step()


def cleanup() -> None:
    """Removes intermediate temporary files."""
    for pattern in ["s3_bt.tif"]:
        path = f"/tmp/{pattern}"
        if os.path.exists(path):
            os.remove(path)


def _bt_colormap(
    data: np.ndarray,
    vmin: float = c.S3_BT_MIN,
    vmax: float = c.S3_BT_MAX,
) -> List[np.ndarray]:
    """Thermal colormap matching the BT legend: black→blue→white→orange→dark red."""
    cleaned = np.nan_to_num(data, nan=0.0)
    flat = cleaned.flatten()
    t = np.clip((flat - vmin) / (vmax - vmin), 0, 1)

    a = np.where(t > 0, 255, 0).astype(np.uint8).reshape(data.shape)

    color_nodes = np.array([0.0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0])
    r_nodes = np.array([0, 0, 0, 102, 255, 255, 255, 204, 136], dtype=np.uint8)
    g_nodes = np.array([0, 26, 51, 153, 255, 221, 153, 68, 17], dtype=np.uint8)
    b_nodes = np.array([0, 77, 204, 255, 255, 204, 51, 0, 0], dtype=np.uint8)

    r = np.interp(t, color_nodes, r_nodes).astype(np.uint8).reshape(data.shape)
    g = np.interp(t, color_nodes, g_nodes).astype(np.uint8).reshape(data.shape)
    b = np.interp(t, color_nodes, b_nodes).astype(np.uint8).reshape(data.shape)

    return [r, g, b, a]


def _fire_colormap(
    data: np.ndarray,
    vmin: float = 300.0,
    vmax: float = 380.0,
) -> List[np.ndarray]:
    """Hot-body colormap (transparent -> dark red -> yellow -> white) for fire detection.
    Alpha starts at 0 below vmin and ramps up, so cool backgrounds are transparent."""
    cleaned = np.nan_to_num(data, nan=vmin)
    flat = cleaned.flatten()
    t = np.clip((flat - vmin) / (vmax - vmin), 0, 1)

    # Alpha: transparent below vmin, ramping to full at vmax
    a = (t * 255).astype(np.uint8)

    # Color nodes: dark red -> bright red -> orange -> yellow -> white
    color_nodes = np.array([0.0, 0.25, 0.5, 0.75, 1.0])
    r_nodes = np.array([0, 80, 255, 255, 255], dtype=np.uint8)
    g_nodes = np.array([0, 0, 50, 200, 255], dtype=np.uint8)
    b_nodes = np.array([0, 0, 0, 50, 255], dtype=np.uint8)

    r = np.interp(t, color_nodes, r_nodes).astype(np.uint8).reshape(data.shape)
    g = np.interp(t, color_nodes, g_nodes).astype(np.uint8).reshape(data.shape)
    b = np.interp(t, color_nodes, b_nodes).astype(np.uint8).reshape(data.shape)
    a = a.reshape(data.shape)

    return [r, g, b, a]


def _render_internal(
    visual_paths: Dict[str, str],
    analytic_paths: Dict[str, str],
    skip_overviews: bool = False,
    platform: Optional[str] = None,
    orbit_direction: Optional[str] = None,
    cloud_cover: Optional[float] = None,
) -> None:
    """Block renderer for S3 fire detection products."""
    func.perf_logger.start_step("S3 Single-Pass Render")
    print("Starting S3 thermal render...", flush=True)

    bt_path = "/tmp/s3_bt.tif"
    if not os.path.exists(bt_path):
        print("ERROR: No BT stack available at /tmp/s3_bt.tif", flush=True)
        func.perf_logger.end_step()
        return

    with rio.open(bt_path) as src:
        v_prof = src.profile.copy()
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

        a_prof = src.profile.copy()
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

        for h in v_handles.values():
            h.colorinterp = [
                ColorInterp.red,
                ColorInterp.green,
                ColorInterp.blue,
                ColorInterp.alpha,
            ]

        block_size = c.BLOCK_SIZE
        fire_detected = False

        for r in range(0, src.height, block_size):
            for col in range(0, src.width, block_size):
                window = rio.windows.Window(
                    col,
                    r,
                    min(block_size, src.width - col),
                    min(block_size, src.height - r),
                )

                # Read thermal bands: S7=1, S8=2, S9=3
                s7 = src.read(1, window=window).astype(np.float32)
                s8 = src.read(2, window=window).astype(np.float32)
                s9 = src.read(3, window=window).astype(np.float32)

                results = {}

                # Alpha mask: valid data pixels (non-zero in any band)
                alpha = np.where((s7 > 0) & (s8 > 0) & (s9 > 0), 255, 0).astype(
                    np.uint8
                )

                if "BT" in v_handles:
                    # Brightness Temperature — single-band colormap matching legend.
                    # Uses mean of S7/S8/S9 to reduce per-pixel noise, mapped through
                    # thermal colormap (black→blue→white→orange→dark red).
                    # Inside-swath alpha is forced to 255 (opaque) — the colormap's
                    # own alpha (based on BT threshold) is discarded. Outside swath
                    # (all bands = 0 from KD-tree distance cutoff) is transparent,
                    # clipping the tile to the curved swath boundary.
                    bt_mean = (s7 + s8 + s9) / 3.0
                    bt_rgba = _bt_colormap(bt_mean)
                    bt_rgba[3] = alpha
                    results["BT_VIS"] = np.stack(bt_rgba, axis=0)

                if "FIRE" in v_handles:
                    # Fire detection using hot-body colormap
                    # Use S7 (3.74µm MIR) as primary fire band.
                    # Alpha is transparent for cold backgrounds.
                    fire_rgba = _fire_colormap(
                        s7,
                        vmin=c.S3_FIRE_THRESHOLD,
                        vmax=380.0,
                    )
                    # Mask cold cloud pixels using S8 (10.85µm).
                    # During daytime S7 has a strong reflected solar component,
                    # making cirrus/cloud tops appear warm (310-330K) and
                    # triggering false fire detections. S8 sees cold cloud tops
                    # at 10.85µm, unaffected by solar reflection.
                    cold_cloud = s8 < c.S3_CLOUD_TEMP_THRESHOLD
                    fire_rgba[3][cold_cloud] = 0
                    if not fire_detected and np.any(fire_rgba[3] > 0):
                        fire_detected = True
                    results["FIRE_VIS"] = np.stack(fire_rgba, axis=0)

                if "BT_ANA" in a_handles:
                    results["BT_ANA"] = s7

                gc.collect()

                for p, h in v_handles.items():
                    if f"{p}_VIS" in results:
                        h.write(results[f"{p}_VIS"], window=window)
                for p, h in a_handles.items():
                    if f"{p}_ANA" in results:
                        h.write(results[f"{p}_ANA"], 1, window=window)

        vis_output_paths = [h.name for h in v_handles.values()]
        for h in list(v_handles.values()) + list(a_handles.values()):
            if platform:
                h.update_tags(SATELLITE=platform)
            if orbit_direction:
                h.update_tags(PASS_DIRECTION=orbit_direction)
            if cloud_cover is not None:
                h.update_tags(CLOUD_COVERAGE_ASSESSMENT=str(cloud_cover))
            h.close()

        if "FIRE" in v_handles and not fire_detected:
            fire_path = v_handles["FIRE"].name
            if os.path.exists(fire_path):
                os.remove(fire_path)
            vis_output_paths = [p for p in vis_output_paths if p != fire_path]

        func.perf_logger.end_step()

        if vis_output_paths and not skip_overviews:
            max_finalizers = int(os.getenv("MAX_PARALLEL_FINALIZERS", "2"))

            def finalize_product(path):
                os.environ["GDAL_NUM_THREADS"] = "1"
                cog.convert_to_cog(path)
                cog.ensure_overviews(path)
                p_type = path.split("/")[-2].upper()
                eff_res = 1000.0
                meta.generate_sidecar(
                    path,
                    f"S3-{p_type}",
                    f"S3-{p_type}",
                    effective_res=eff_res,
                    satellite=platform,
                    orbit_direction=orbit_direction,
                    cloud_cover=cloud_cover,
                )

            with ThreadPoolExecutor(
                max_workers=min(len(vis_output_paths), max_finalizers)
            ) as executor:
                executor.map(finalize_product, vis_output_paths)

        legends.save_all_legends(c.DIRS["S1S2_LEGENDS"])
        gc.collect()


def run_pipeline(
    product_dir: str,
    processes: List[str],
    fusion_processes: Optional[List[str]] = None,
    cloud_cover: Optional[float] = None,
) -> None:
    """Entry point for S3 pipeline."""
    if fusion_processes is None:
        fusion_processes = []

    product_name = os.path.basename(product_dir.rstrip("/"))
    manifest_path = os.path.join(product_dir, "xfdumanifest.xml")
    manifest_meta = _s3_parse_manifest(manifest_path)

    time_str = (
        str(get_time(product_name)) + "Z" if get_time(product_name) else "unknown"
    )
    name = f"S3-{time_str}"

    platform_match = re.search(r"(S3[AB])_", product_name)
    platform = platform_match.group(1) if platform_match else "S3"
    orbit_dir = manifest_meta.get("pass_direction")

    v_paths: Dict[str, str] = {}
    a_paths: Dict[str, str] = {}

    processes = [p.replace("-", "_") for p in processes]

    s3_deps: Dict[str, List[str]] = {
        "FIRE": [],
        "BT": [],
    }

    needed_analytics = set()
    for p in processes:
        if p in s3_deps:
            needed_analytics.update(s3_deps[p])

    for p in processes:
        if p in ["BT", "FIRE"]:
            v_paths[p] = f"{c.DIRS[f'VIS_S3_{p}']}/{name}-{p}"

        if f"ANA_S3_{p}" in c.DIRS:
            a_paths[p] = f"{c.DIRS[f'ANA_S3_{p}']}/{name}-{p}"

    if v_paths or a_paths:
        prepare(product_dir)
        _render_internal(
            v_paths,
            a_paths,
            platform=platform,
            orbit_direction=orbit_dir,
            cloud_cover=cloud_cover,
        )

    cleanup()
