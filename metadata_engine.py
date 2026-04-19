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
from datetime import datetime
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


def generate_sidecar(
    tif_path: str, product_type: str, legend_id: str, effective_res: Optional[float] = None
) -> None:
    """
    Generates a .json sidecar for a Visual TIF.
    Contains acquisition time, precise footprint (GeoJSON-like), and product metadata.
    Optimized: Downsamples mask for extreme speed gain in vectorization.
    """
    if not os.path.exists(tif_path):
        return

    sidecar_path: str = tif_path.replace(".tif", ".json")
    start_time = time.time()

    with rio.open(tif_path) as src:
        # 1. Calculate footprint from Alpha channel (usually last band)
        # We downsample by factor of 10 (10m -> 100m) for footprint extraction.
        # This makes the vectorization 100x faster and reduces noise automatically.
        mask_band = src.count if src.count > 1 else 1
        
        factor = 10
        new_height = max(1, src.height // factor)
        new_width = max(1, src.width // factor)
        
        # Use 'mode' resampling to keep the mask clean
        mask = src.read(
            mask_band, 
            out_shape=(new_height, new_width),
            resampling=rio.enums.Resampling.mode
        )
        
        # Adjust transform for downsampled mask
        transform = src.transform * src.transform.scale(
            (src.width / mask.shape[-1]),
            (src.height / mask.shape[-2])
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
            
            # Simplify with 40m tolerance
            combined = combined.simplify(40.0, preserve_topology=True)
            
            # Extreme noise reduction: keep top 25 parts max
            if combined.geom_type == 'MultiPolygon':
                parts = sorted(combined.geoms, key=lambda p: p.area, reverse=True)
                combined = MultiPolygon(parts[:25]) if len(parts) > 1 else parts[0]
                combined = combined.simplify(40.0, preserve_topology=True)
            
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

        s1_match: Optional[re.Match] = re.search(r"S1_(\d{8}T\d{6})", filename)
        s2_match: Optional[re.Match] = re.search(r"-(\d{8}T\d{6}Z)", filename)

        if s1_match:
            raw_t: str = s1_match.group(1)
            timestamp = (
                f"{raw_t[:4]}-{raw_t[4:6]}-{raw_t[6:8]}T"
                f"{raw_t[9:11]}:{raw_t[11:13]}:{raw_t[13:15]}Z"
            )
        elif s2_match:
            raw_t_s2: str = s2_match.group(1)
            timestamp = (
                f"{raw_t_s2[:4]}-{raw_t_s2[4:6]}-{raw_t_s2[6:8]}T"
                f"{raw_t_s2[9:11]}:{raw_t_s2[11:13]}:{raw_t_s2[13:15]}Z"
            )

        metadata = {
            "product": product_type,
            "acquisition_time": timestamp,
            "render_time": datetime.now().isoformat() + "Z",
            "resolution": effective_res if effective_res is not None else round(src.res[0], 1),
            "bounds": leaflet_bounds,
            "footprint": footprint,
            "legend_id": legend_id,
            "crs": "EPSG:3857",
        }

        with open(sidecar_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, separators=(",", ":"))

    elapsed = time.time() - start_time
    print(f"Sidecar generated in {elapsed:.2f}s: {os.path.basename(sidecar_path)}", flush=True)
    gc.collect()


if __name__ == "__main__":
    pass
