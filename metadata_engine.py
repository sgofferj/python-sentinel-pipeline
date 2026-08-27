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
Optimized for extreme JSON compactness and memory efficiency.
Now includes coordinate rounding to prevent floating-point bloat.
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
from shapely.geometry import shape, mapping, MultiPolygon, Polygon
from shapely.ops import unary_union


def fill_holes(geom: Any) -> Any:
    """Fills all holes (interior rings) in a Polygon or MultiPolygon."""
    if geom.is_empty:
        return geom
    if geom.geom_type == "Polygon":
        return Polygon(geom.exterior)
    if geom.geom_type == "MultiPolygon":
        return MultiPolygon([Polygon(p.exterior) for p in geom.geoms])
    return geom


def round_coordinates(geom_dict: Any, precision: int = 5) -> Any:
    """Recursively rounds coordinates in a GeoJSON-like dictionary."""
    if "coordinates" in geom_dict:
        geom_dict["coordinates"] = _round_list(geom_dict["coordinates"], precision)
    return geom_dict


def _round_list(lst: Any, precision: int) -> Any:
    """Helper to walk nested coordinate lists."""
    if isinstance(lst, (int, float)):
        return round(lst, precision)
    return [_round_list(x, precision) for x in lst]


# Resolution mapping (Effective resolution in m/px)
RES_MAP = {
    "S1-VV": 15.0,
    "S1-VH": 15.0,
    "S1-RATIO": 15.0,
    "S1-RATIO-AIS": 15.0,
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
    """
    Identifies a visual TIFF based on its path and filename.
    Returns (product_id, legend_id).
    """
    filename = os.path.basename(tif_path)
    parts = tif_path.split(os.sep)

    try:
        # We assume the path contains 'visual' to determine sensor/product
        idx = parts.index("visual")
        sat = parts[idx + 1].upper()  # S1, S2, S3, FUSED, ROI
    except (ValueError, IndexError):
        return "UNKNOWN", "S2-TCI"

    if sat == "FUSED":
        # Extract product from folder or filename
        p_type = parts[idx + 2].upper()
        if p_type == "FUSED" or p_type == filename.upper():
            m = re.search(
                r"-(LIFE-MACHINE|RADAR-BURN|TARGET-PROBE-V2)\.tif", filename, re.I
            )
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

    # Standard S1/S2
    p_type = parts[idx + 2].upper()
    return f"{sat}-{p_type}", f"{sat}-{p_type}"


def generate_sidecar(
    tif_path: str,
    product_type: Optional[str] = None,
    legend_id: Optional[str] = None,
    effective_res: Optional[float] = None,
    cloud_cover: Optional[float] = None,
    relative_orbit: Optional[str] = None,
    orbit_direction: Optional[str] = None,
    satellite: Optional[str] = None,
) -> None:
    """
    Generates a .json sidecar for a Visual TIF.
    If product_type or legend_id are missing, it attempts to self-identify.
    """
    if not os.path.exists(tif_path):
        return

    # Self-identify if needed
    if not product_type or not legend_id:
        auto_prod, auto_legend = identify_tif(tif_path)
        product_type = product_type or auto_prod
        legend_id = legend_id or auto_legend

    if not effective_res:
        effective_res = RES_MAP.get(product_type) or RES_MAP.get(legend_id)

    sidecar_path: str = tif_path.replace(".tif", ".json")
    start_time = time.time()

    try:
        with rio.open(tif_path) as src:
            # 1. Calculate footprint from Alpha channel (usually last band)
            # Downsample to keep vectorisation fast but preserve boundary shape.
            # For coarse data (S3 at 1000m) this means factor=2–3 (2-3km mask pixels);
            # for fine data (S2 at 10m) it caps at factor=10 (100m mask pixels).
            mask_band = src.count if src.count > 1 else 1

            if effective_res:
                # Target ~500m mask pixels so vectorisation is fast but
                # the resulting polygon doesn't have 10km stair-step edges.
                factor = max(1, min(10, int(effective_res / 500)))
            else:
                factor = 10
            new_height = max(1, src.height // factor)
            new_width = max(1, src.width // factor)

            # Use 'mode' resampling to keep the mask clean
            mask = src.read(
                mask_band,
                out_shape=(new_height, new_width),
                resampling=rio.enums.Resampling.mode,
            )

            # Adjust transform for downsampled mask
            transform = src.transform * src.transform.scale(
                (src.width / mask.shape[-1]), (src.height / mask.shape[-2])
            )

            # Only pixels > 0 are valid data
            # For NDVI, values > 0 are usually vegetation, but here we want the footprint
            # If it's a visual product (4 bands), the last band is a dedicated Alpha.
            # If it's a single band analytic, we take what we have.
            mask_bit = (mask > 0).astype(np.uint8)
            del mask

            # Extract shapes (polygons) from the mask
            results = (
                {"properties": {"raster_val": v}, "geometry": s}
                for i, (s, v) in enumerate(
                    shapes(mask_bit, mask=mask_bit, transform=transform)
                )
            )

            # Convert to Shapely objects with area filter
            geoms = []
            for r in results:
                g = shape(r["geometry"])
                # Area filter: ignore anything smaller than 4 hectares (40,000 m2)
                # to keep the inventory really clean.
                if g.area > 40000:
                    geoms.append(g)

            del mask_bit

            if not geoms:
                # Fallback to bounds
                bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
                leaflet_bounds: List[List[float]] = [
                    [round(bounds[1], 5), round(bounds[0], 5)],
                    [round(bounds[3], 5), round(bounds[2], 5)],
                ]
                footprint = None
            else:
                # Merge and simplify polygons
                # Downsampled shapes are already much fewer, making union fast
                combined = unary_union(geoms)
                del geoms

                # Use buffer(0) to clean up
                combined = combined.buffer(0)

                # --- Robust Hole Filling ---
                # Removes all internal 'voids' (sensor noise, cloud shadows)
                # only from the SIDE-CAR metadata to keep JSON compact.
                combined = fill_holes(combined)

                # Simplify with resolution-aware tolerance.
                # Coarse mask pixels create stair-step aliasing where each corner
                # deviates ~mask_pixel/sqrt(2) from the true diagonal boundary.
                # Tolerance must be large enough to remove these corner vertices.
                mask_pixel = effective_res * factor if effective_res else 100
                simplify_tol = max(40.0, mask_pixel * 3.0)
                combined = combined.simplify(simplify_tol, preserve_topology=True)

                # Extreme noise reduction: keep top 25 parts max
                if combined.geom_type == "MultiPolygon":
                    parts = sorted(combined.geoms, key=lambda p: p.area, reverse=True)
                    combined = MultiPolygon(parts[:25]) if len(parts) > 1 else parts[0]
                    combined = combined.simplify(simplify_tol, preserve_topology=True)

                # Final buffer(0) to clean any self-intersections from simplify
                combined = combined.buffer(0)

                # Re-apply hole-filling after the buffer(0) might have re-created holes
                combined = fill_holes(combined)

                # Transform to EPSG:4326
                footprint_raw = transform_geom(src.crs, "EPSG:4326", mapping(combined))

                # --- Round Coordinates ---
                # Shaves off 60-70% of JSON size by limiting precision to ~1.1m
                footprint = round_coordinates(footprint_raw, 5)

                # Bounds for quick Leaflet fitBounds
                b = combined.bounds
                b4326 = transform_bounds(src.crs, "EPSG:4326", *b)
                leaflet_bounds = [
                    [round(b4326[1], 5), round(b4326[0], 5)],
                    [round(b4326[3], 5), round(b4326[2], 5)],
                ]
                del combined

            # Extract Acquisition Time from filename
            filename: str = os.path.basename(tif_path)
            timestamp: str = "Unknown"
            rel_orbit = relative_orbit
            orbit_dir = orbit_direction
            sat_val = satellite

            # Check if it's an ROI file first (Name_Prod_Time.tif)
            fn_no_ext = filename.rsplit(".", 1)[0]
            fn_parts = fn_no_ext.split("_")

            if len(fn_parts) >= 3 and re.match(
                r"^\d{4}-\d{2}-\d{2}T\d{6}Z$", fn_parts[-1]
            ):
                raw_t = fn_parts[-1]
                # raw_t is like 2026-05-24T100000Z
                timestamp = (
                    f"{raw_t[:10]}T{raw_t[11:13]}:{raw_t[13:15]}:{raw_t[15:17]}Z"
                )
            else:
                # Standard S1/S2 parsing
                s1_match: Optional[re.Match] = re.search(r"S1_(\d{8}T\d{6})", filename)
                s2_match: Optional[re.Match] = re.search(r"-(\d{8}T\d{6}Z)", filename)
                s3_match: Optional[re.Match] = re.search(r"S3-(\d{8}T\d{6}Z)", filename)

                if s3_match:
                    raw_t_s3 = s3_match.group(1)
                    timestamp = (
                        f"{raw_t_s3[:4]}-{raw_t_s3[4:6]}-{raw_t_s3[6:8]}T"
                        f"{raw_t_s3[9:11]}:{raw_t_s3[11:13]}:{raw_t_s3[13:15]}Z"
                    )
                elif s1_match:
                    raw_t = s1_match.group(1)
                    timestamp = (
                        f"{raw_t[:4]}-{raw_t[4:6]}-{raw_t[6:8]}T"
                        f"{raw_t[9:11]}:{raw_t[11:13]}:{raw_t[13:15]}Z"
                    )
                elif s2_match:
                    raw_t_s2 = s2_match.group(1)
                    timestamp = (
                        f"{raw_t_s2[:4]}-{raw_t_s2[4:6]}-{raw_t_s2[6:8]}T"
                        f"{raw_t_s2[9:11]}:{raw_t_s2[11:13]}:{raw_t_s2[13:15]}Z"
                    )

            # Fallback: read metadata from TIFF tags for any product type
            # (used when generate_sidecar is called from rebuild_metadata.py
            #  without explicit parameters)
            if not rel_orbit or not orbit_dir or not sat_val or cloud_cover is None:
                meta_dict = src.tags()
                if not rel_orbit:
                    rel_orbit = meta_dict.get("RELATIVE_ORBIT_NUMBER")
                if not orbit_dir:
                    orbit_dir = meta_dict.get("ORBIT_DIRECTION") or meta_dict.get(
                        "PASS_DIRECTION"
                    )
                if not sat_val:
                    sat_val = meta_dict.get("SATELLITE")
                if cloud_cover is None:
                    cc_tag = meta_dict.get("CLOUD_COVERAGE_ASSESSMENT")
                    if cc_tag is not None:
                        try:
                            cloud_cover = float(cc_tag)
                        except (ValueError, TypeError):
                            pass

            metadata = {
                "product": product_type,
                "acquisition_time": timestamp,
                "render_time": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "resolution": (
                    effective_res if effective_res is not None else round(src.res[0], 1)
                ),
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
            print(
                f"Sidecar generated in {elapsed:.2f}s: {os.path.basename(sidecar_path)}",
                flush=True,
            )
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error processing {os.path.basename(tif_path)}: {e}", flush=True)

    gc.collect()


if __name__ == "__main__":
    pass
