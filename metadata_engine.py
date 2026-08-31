#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# metadata_engine.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Metadata engine for generating sidecar JSON files for visual products.
Generates footprint (EPSG:4326) + bounds + acquisition/orbit tags.
Optimised for large rasters: downsample mask before vectorisation, single
simplify + topology clean, optional pre-computed footprint to skip raster I/O.
"""

import gc
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

import numpy as np
import rasterio as rio
from rasterio.features import shapes
from rasterio.warp import transform_bounds, transform_geom
from shapely.geometry import mapping, shape, MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.wkt import loads as wkt_loads


def fill_holes(geom: Any) -> Any:
    """Fills all interior rings in a Polygon/MultiPolygon."""
    if geom.is_empty:
        return geom
    if geom.geom_type == "Polygon":
        return Polygon(geom.exterior)
    if geom.geom_type == "MultiPolygon":
        return MultiPolygon([Polygon(p.exterior) for p in geom.geoms])
    return geom


def round_coordinates(geom_dict: Any, precision: int = 5) -> Any:
    """Recursively rounds coordinates in a GeoJSON dict to `precision` decimals."""
    if "coordinates" in geom_dict:
        geom_dict["coordinates"] = _round_list(geom_dict["coordinates"], precision)
    return geom_dict


def _round_list(lst: Any, precision: int) -> Any:
    if isinstance(lst, (int, float)):
        return round(lst, precision)
    return [_round_list(x, precision) for x in lst]


# Effective resolution in m/px
RES_MAP = {
    "S1-VV": 15.0,
    "S1-VH": 15.0,
    "S1-RATIO": 15.0,
    "S1-RATIO-AIS": 15.0,
    "S1-DELTA": 15.0,
    "S2-TCI": 10.0,
    "S2-TCI-AIS": 10.0,
    "S2-TCI-GF": 10.0,
    "S2-NDVI": 10.0,
    "S2-NIRFC": 10.0,
    "S2-NIRFC-GF": 10.0,
    "S2-AP": 20.0,
    "S2-AP-GF": 10.0,
    "S2-NDBI": 20.0,
    "S2-NDBI_CLEAN": 20.0,
    "S2-NDRE": 20.0,
    "S2-NBR": 20.0,
    "S2-CAMO": 20.0,
    "FUSED-LIFE-MACHINE": 10.0,
    "FUSED-RADAR-BURN": 10.0,
    "FUSED-TARGET-PROBE-V2": 10.0,
    "S3-BT": 1000.0,
    "S3-FIRE": 1000.0,
}


def identify_tif(tif_path: str) -> tuple[str, str]:
    """Identifies (product_id, legend_id) from visual path/filename."""
    filename = os.path.basename(tif_path)
    parts = tif_path.split(os.sep)
    try:
        idx = parts.index("visual")
        sat = parts[idx + 1].upper()
    except (ValueError, IndexError):
        return "UNKNOWN", "S2-TCI"

    if sat == "FUSED":
        p_type = parts[idx + 2].upper()
        if p_type == "FUSED" or p_type == filename.upper():
            m = re.search(r"-(LIFE-MACHINE|RADAR-BURN|TARGET-PROBE-V2)\.tif", filename, re.I)
            p_type = m.group(1).upper() if m else "UNKNOWN"
        return f"FUSED-{p_type}", p_type

    if sat == "ROI":
        fn_no_ext = filename.rsplit(".", 1)[0]
        fn_parts = fn_no_ext.split("_")
        if len(fn_parts) >= 3:
            roi_name = fn_parts[0]
            p_suffix = "_".join(fn_parts[1:-1]).upper()
            norm_suffix = p_suffix.replace("_", "-")
            product_id = f"ROI-{roi_name}-{p_suffix}"
            if norm_suffix in ["VV", "VH", "RATIO", "RATIO-AIS"]:
                legend_id = f"S1-{norm_suffix}"
            elif norm_suffix in ["LIFE-MACHINE", "RADAR-BURN", "TARGET-PROBE-V2"]:
                legend_id = f"FUSED-{norm_suffix}"
            else:
                legend_id = f"S2-{norm_suffix}"
            return product_id, legend_id
        return f"ROI-{filename}", "S2-TCI"

    if sat == "S1" and len(parts) > idx + 2 and parts[idx + 2].upper() == "DELTA":
        return "S1-DELTA", "S1-DELTA"

    p_type = parts[idx + 2].upper() if len(parts) > idx + 2 else "UNKNOWN"
    return f"{sat}-{p_type}", f"{sat}-{p_type}"


def _parse_footprint_input(footprint: Any, footprint_crs: Optional[str]) -> Optional[Any]:
    """Normalise footprint param (WKT str / GeoJSON dict / shapely geom) to shapely geom in EPSG:4326.

    Returns shapely geometry in EPSG:4326 or None if parsing fails.
    """
    if footprint is None:
        return None
    try:
        if isinstance(footprint, str):
            # WKT like POLYGON((lon lat,...)) — already 4326
            geom = wkt_loads(footprint)
        elif isinstance(footprint, dict) and "coordinates" in footprint:
            geom = shape(footprint)
        else:
            # Assume shapely geometry
            geom = footprint
        if geom is None or geom.is_empty:
            return None
        if footprint_crs and footprint_crs.upper() != "EPSG:4326":
            # Transform to 4326 if caller says it's in another CRS (typically 3857)
            geom_wkt = mapping(geom)
            geom_4326_dict = transform_geom(footprint_crs, "EPSG:4326", geom_wkt)
            geom = shape(geom_4326_dict)
        return geom
    except Exception:
        return None


def _clean_footprint_geometry(geom: Any, effective_res: Optional[float], factor: int, is_degrees: bool = False) -> Any:
    """Topology-clean, hole-fill, simplify, and cap parts.

    For raster-derived geoms (`is_degrees=False`) the geometry is in EPSG:3857
    (metres) and simplify tolerance is `mask_pixel * 2.5` metres.
    For supplied WKT/GeoJSON (`is_degrees=True`) the geometry is already in
    EPSG:4326 and would be collapsed by a metre tolerance — use a small
    degree tolerance (~0.0003° ≈ 30 m) and preserve shape exactly.
    """
    if geom.is_empty:
        return geom
    geom = geom.buffer(0)
    geom = fill_holes(geom)
    if is_degrees:
        # Supplied footprints from OData/manifest are already clean (4–70 verts).
        # Only ensure validity and cap parts; don't simplify aggressively.
        if geom.geom_type == "MultiPolygon" and len(geom.geoms) > 25:
            parts = sorted(geom.geoms, key=lambda p: p.area, reverse=True)[:25]
            geom = MultiPolygon(parts)
        geom = geom.buffer(0)
        geom = fill_holes(geom)
        return geom
    mask_pixel = effective_res * factor if effective_res else 100
    simplify_tol = max(40.0, mask_pixel * 2.5)
    geom = geom.simplify(simplify_tol, preserve_topology=True)
    if geom.geom_type == "MultiPolygon" and len(geom.geoms) > 25:
        parts = sorted(geom.geoms, key=lambda p: p.area, reverse=True)[:25]
        geom = MultiPolygon(parts)
        geom = geom.simplify(simplify_tol, preserve_topology=True)
    geom = geom.buffer(0)
    geom = fill_holes(geom)
    return geom


def _footprint_from_raster(src: rio.DatasetReader, effective_res: Optional[float]) -> tuple[Optional[Any], Optional[List[List[float]]], Optional[Any]]:
    """Vectorise mask band to footprint GeoJSON + Leaflet bounds.

    Downsamples mask to ~100–150 m pixels for fine products (15 m -> 10x). For
    very narrow S2 stripes the 10x downsample can erase the valid data and
    the footprint will fallback to bounds (slight over-estimate, but rare).
    Returns (footprint_geojson_or_None, leaflet_bounds, combined_geom_in_src_crs_or_None).
    """
    mask_band = src.count if src.count > 1 else 1
    if effective_res:
        factor = max(1, min(10, int(round(500 / effective_res))))
    else:
        factor = 10
    new_height = max(1, src.height // factor)
    new_width = max(1, src.width // factor)

    mask = src.read(mask_band, out_shape=(new_height, new_width), resampling=rio.enums.Resampling.mode)
    transform = src.transform * src.transform.scale((src.width / mask.shape[-1]), (src.height / mask.shape[-2]))
    mask_bit = (mask > 0).astype(np.uint8)
    del mask

    results = ({"properties": {"raster_val": v}, "geometry": s} for _, (s, v) in enumerate(shapes(mask_bit, mask=mask_bit, transform=transform)))
    geoms: List[Any] = []
    for r in results:
        g = shape(r["geometry"])
        if g.area > 40000:
            geoms.append(g)
    del mask_bit

    if not geoms:
        return None, None, None

    combined = unary_union(geoms)
    combined = _clean_footprint_geometry(combined, effective_res, factor, is_degrees=False)

    # Bounds in src CRS then to 4326 for Leaflet
    b = combined.bounds  # (minx, miny, maxx, maxy) in src CRS (3857)
    b4326 = transform_bounds(src.crs, "EPSG:4326", *b)
    leaflet_bounds: List[List[float]] = [
        [round(b4326[1], 5), round(b4326[0], 5)],
        [round(b4326[3], 5), round(b4326[2], 5)],
    ]
    # Footprint transformed to 4326 + rounded
    footprint_raw = transform_geom(src.crs, "EPSG:4326", mapping(combined))
    footprint = round_coordinates(footprint_raw, 5)

    return footprint, leaflet_bounds, combined


def generate_sidecar(
    tif_path: str,
    product_type: Optional[str] = None,
    legend_id: Optional[str] = None,
    effective_res: Optional[float] = None,
    cloud_cover: Optional[float] = None,
    relative_orbit: Optional[str] = None,
    orbit_direction: Optional[str] = None,
    satellite: Optional[str] = None,
    footprint: Optional[Any] = None,
    footprint_crs: Optional[str] = None,
) -> None:
    """
    Generates a .json sidecar for a Visual TIF.

    If `footprint` is provided (WKT string, GeoJSON dict, or shapely geometry),
    it is used directly instead of vectorising the raster (fast path for
    S1/S2/S3 source footprints and ROI intersections). Assumed to be in
    EPSG:4326 unless `footprint_crs` is set.

    Otherwise the mask band (alpha for RGBA, band 1 for single-band) is
    downsampled and vectorised. Vectorisation target is ~500/effective_res
    (max 10x), so 15 m -> 150 m mask pixels, 1000 m -> 1000 m.
    """
    if not os.path.exists(tif_path):
        return

    if not product_type or not legend_id:
        auto_prod, auto_legend = identify_tif(tif_path)
        product_type = product_type or auto_prod
        legend_id = legend_id or auto_legend

    if not effective_res:
        effective_res = RES_MAP.get(product_type) or RES_MAP.get(legend_id)

    sidecar_path: str = tif_path.replace(".tif", ".json")
    start_time = time.time()

    try:
        # Fast path: caller supplied footprint (WKT / GeoJSON / shapely)
        supplied_geom = _parse_footprint_input(footprint, footprint_crs)
        if supplied_geom is not None and not supplied_geom.is_empty:
            # Supplied footprints are already in EPSG:4326 and pre-cleaned
            # (e.g. OData POLYGON with 5–70 verts). Don't simplify in degrees.
            if effective_res:
                factor = max(1, min(10, int(round(500 / effective_res))))
            else:
                factor = 10
            supplied_geom = _clean_footprint_geometry(supplied_geom, effective_res, factor, is_degrees=True)
            # Bounds from cleaned geom
            minx, miny, maxx, maxy = supplied_geom.bounds
            leaflet_bounds: List[List[float]] = [
                [round(miny, 5), round(minx, 5)],
                [round(maxy, 5), round(maxx, 5)],
            ]
            footprint_geojson = round_coordinates(mapping(supplied_geom), 5)
            # Need src CRS for metadata only; acquire minimal info from tif for tags
            with rio.open(tif_path) as src:
                # Reuse tag extraction logic below but skip raster vectorisation
                filename = os.path.basename(tif_path)
                # Extract time / orbit tags without re-reading mask
                meta_dict = src.tags()
                # Fall through to shared tag/time handling after this block
                src_res = effective_res if effective_res is not None else round(src.res[0], 1)
                src_crs_str = str(src.crs) if src.crs else "EPSG:3857"
            # Continue to time/orbit handling below using supplied footprint
            footprint = footprint_geojson
            effective_res_val = src_res
            # Jump to metadata assembly (avoid re-entering raster branch)
            do_raster_branch = False
        else:
            do_raster_branch = True

        if do_raster_branch:
            with rio.open(tif_path) as src:
                footprint, leaflet_bounds, _combined = _footprint_from_raster(src, effective_res)

                if footprint is None:
                    # Fallback to raster bounds rectangle
                    bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
                    leaflet_bounds = [
                        [round(bounds[1], 5), round(bounds[0], 5)],
                        [round(bounds[3], 5), round(bounds[2], 5)],
                    ]

                filename = os.path.basename(tif_path)
                meta_dict = src.tags()
                effective_res_val = effective_res if effective_res is not None else round(src.res[0], 1)
                src_crs_str = str(src.crs) if src.crs else "EPSG:3857"
        else:
            # Supplied branch: filename/meta_dict already set, but need filename for timestamp parsing
            filename = os.path.basename(tif_path)
            # meta_dict already set in branch above; ensure leaflet_bounds/footprint defined
            pass

        # Extract acquisition time from filename
        timestamp: str = "Unknown"
        rel_orbit = relative_orbit
        orbit_dir = orbit_direction
        sat_val = satellite

        fn_no_ext = filename.rsplit(".", 1)[0]
        fn_parts = fn_no_ext.split("_")

        if len(fn_parts) >= 3 and re.match(r"^\d{4}-\d{2}-\d{2}T\d{6}Z$", fn_parts[-1]):
            raw_t = fn_parts[-1]
            timestamp = f"{raw_t[:10]}T{raw_t[11:13]}:{raw_t[13:15]}:{raw_t[15:17]}Z"
        else:
            s1_match: Optional[re.Match] = re.search(r"S1_(\d{8}T\d{6})", filename)
            s2_match: Optional[re.Match] = re.search(r"-(\d{8}T\d{6}Z)", filename)
            s3_match: Optional[re.Match] = re.search(r"S3-(\d{8}T\d{6}Z)", filename)
            if s3_match:
                raw_t_s3 = s3_match.group(1)
                timestamp = f"{raw_t_s3[:4]}-{raw_t_s3[4:6]}-{raw_t_s3[6:8]}T{raw_t_s3[9:11]}:{raw_t_s3[11:13]}:{raw_t_s3[13:15]}Z"
            elif s1_match:
                raw_t = s1_match.group(1)
                timestamp = f"{raw_t[:4]}-{raw_t[4:6]}-{raw_t[6:8]}T{raw_t[9:11]}:{raw_t[11:13]}:{raw_t[13:15]}Z"
            elif s2_match:
                raw_t_s2 = s2_match.group(1)
                timestamp = f"{raw_t_s2[:4]}-{raw_t_s2[4:6]}-{raw_t_s2[6:8]}T{raw_t_s2[9:11]}:{raw_t_s2[11:13]}:{raw_t_s2[13:15]}Z"

        # Fallback: read tags if any orbit/satellite field missing
        # (used by rebuild_metadata.py without explicit params)
        if not rel_orbit or not orbit_dir or not sat_val or cloud_cover is None:
            try:
                # meta_dict is already available from whichever branch above
                pass
            except NameError:
                meta_dict = {}
                try:
                    with rio.open(tif_path) as _tmp_src:
                        meta_dict = _tmp_src.tags()
                except Exception:
                    meta_dict = {}
            if not rel_orbit:
                rel_orbit = meta_dict.get("RELATIVE_ORBIT_NUMBER")
            if not orbit_dir:
                orbit_dir = meta_dict.get("ORBIT_DIRECTION") or meta_dict.get("PASS_DIRECTION")
            if not sat_val:
                sat_val = meta_dict.get("SATELLITE")
            if cloud_cover is None:
                cc_tag = meta_dict.get("CLOUD_COVERAGE_ASSESSMENT")
                if cc_tag is not None:
                    try:
                        cloud_cover = float(cc_tag)
                    except (ValueError, TypeError):
                        pass

        # Leaflet bounds must be defined — if supplied branch had no raster, ensure fallback
        if "leaflet_bounds" not in locals() or leaflet_bounds is None:
            # Try to derive from footprint, else from raster bounds
            if footprint is not None:
                try:
                    geom_for_bounds = shape(footprint)  # type: ignore[arg-type]
                    minx, miny, maxx, maxy = geom_for_bounds.bounds
                    leaflet_bounds = [[round(miny, 5), round(minx, 5)], [round(maxy, 5), round(maxx, 5)]]
                except Exception:
                    leaflet_bounds = [[0, 0], [0, 0]]
            else:
                leaflet_bounds = [[0, 0], [0, 0]]

        # Effective resolution for metadata
        if "effective_res_val" not in locals():
            effective_res_val = effective_res if effective_res is not None else 10.0

        metadata = {
            "product": product_type,
            "acquisition_time": timestamp,
            "render_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "resolution": effective_res_val,
            "bounds": leaflet_bounds,
            "footprint": footprint,
            "legend_id": legend_id,
            "crs": "EPSG:3857",
        }

        if rel_orbit:
            metadata["relative_orbit"] = rel_orbit
        if orbit_dir:
            metadata["orbit_direction"] = orbit_dir
        if sat_val:
            metadata["satellite"] = sat_val
        if cloud_cover is not None:
            metadata["cloud_cover"] = round(float(cloud_cover), 1)

        with open(sidecar_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, separators=(",", ":"))

        elapsed = time.time() - start_time
        print(f"Sidecar generated in {elapsed:.2f}s: {os.path.basename(sidecar_path)}", flush=True)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error processing {os.path.basename(tif_path)}: {e}", flush=True)
    finally:
        gc.collect()


if __name__ == "__main__":
    pass
