#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Water mask helper for S1 delta ship vs wind separation.
Rasterizes SeaVox V19 Gulf of Finland polygon to a given BBOX at 15m EPSG:3857,
cached by BBOX string (future-proof for any ROI footprint).
"""

import hashlib
import json
import os
from typing import Optional

import numpy as np
import rasterio as rio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from shapely.geometry import box, shape
from shapely.ops import transform as shp_transform
import pyproj

# SeaVox V19 single-feature polygon, Gulf of Finland 22.89,59.21-30.34,60.74, 9152 pts
SEAVOX_PATH = "/home/sgofferj/Documents/maps/seavox_v19/seavox_v19.json"
# Cache dir: use /tmp for now, but also check output/.cache
CACHE_DIR = "/tmp/water_mask_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# Load SeaVox geometry once (in EPSG:4326)
_SEAVOX_GEOM_4326 = None
_SEAVOX_BOUNDS_4326 = None

def _load_seavox():
    global _SEAVOX_GEOM_4326, _SEAVOX_BOUNDS_4326
    if _SEAVOX_GEOM_4326 is not None:
        return _SEAVOX_GEOM_4326
    if not os.path.exists(SEAVOX_PATH):
        return None
    try:
        with open(SEAVOX_PATH, "r", encoding="utf-8") as f:
            gj = json.load(f)
        # FeatureCollection with 1 polygon
        feat = gj["features"][0] if gj.get("type") == "FeatureCollection" else gj
        geom = shape(feat["geometry"] if "geometry" in feat else feat)
        _SEAVOX_GEOM_4326 = geom
        _SEAVOX_BOUNDS_4326 = geom.bounds  # (minx, miny, maxx, maxy) lon, lat
        return geom
    except Exception as e:
        print(f"Water mask: failed to load SeaVox {e}", flush=True)
        return None


def _bbox_to_cache_key(bbox_str: str, resolution: float, crs: str) -> str:
    h = hashlib.md5(f"{bbox_str}|{resolution}|{crs}".encode()).hexdigest()[:12]
    safe = bbox_str.replace(",", "_").replace(".", "p").replace("-", "m")
    return f"watermask_{safe}_{int(resolution)}m_{h}.tif"


def get_water_mask(bbox_str: str, resolution: float = 15.0, crs: str = "EPSG:3857") -> Optional[np.ndarray]:
    """
    Returns uint8 water mask (1=water, 0=land/outside Gulf) for the given BBOX.

    BBOX is "west,south,east,north" in EPSG:4326. Mask is rasterized at `resolution`
    in `crs` (default 3857, 15m for S1 delta). Result is cached as GeoTIFF in
    CACHE_DIR keyed by BBOX+res+crs, so future calls for same footprint are instant.

    Returns None if SeaVox not available or BBOX is outside Gulf (all land).
    """
    seavox = _load_seavox()
    if seavox is None:
        return None
    try:
        west, south, east, north = map(float, bbox_str.split(","))
    except Exception:
        return None

    # Quick reject: if BBOX is completely outside SeaVox bounds, return all zeros without rasterizing
    minx, miny, maxx, maxy = _SEAVOX_BOUNDS_4326
    if east < minx or west > maxx or north < miny or south > maxy:
        # No overlap with Gulf
        return None

    cache_key = _bbox_to_cache_key(bbox_str, resolution, crs)
    cache_path = os.path.join(CACHE_DIR, cache_key)

    # If cache exists, try to load
    if os.path.exists(cache_path):
        try:
            with rio.open(cache_path) as src:
                # Verify shape matches expected for this BBOX (in case BBOX string same but resolution diff)
                # We can just return the array
                return src.read(1)
        except Exception:
            pass

    # Need to rasterize
    try:
        # Transform SeaVox from 4326 to target CRS for rasterization
        # We need to create an output grid that exactly matches the delta's grid:
        # Use same logic as delta's _warp_analytic_to_roi: outputBounds in 4326, dstSRS 3857, xRes/yRes
        # Instead of recomputing, we can create a transform that matches gdal.Warp's output for this BBOX
        # Simplest: create a raster in target CRS with bounds derived from BBOX transformed to target CRS
        # Use pyproj to transform BBOX corners to target CRS to get bounds
        transformer = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        # Transform BBOX corners
        xs, ys = transformer.transform([west, east, east, west], [south, south, north, north])
        # For 3857, x is easting, y is northing
        x_min, x_max = min(xs), max(xs)
        y_min, y_max = min(ys), max(ys)
        # For delta, the output grid is defined by gdal.Warp with outputBounds in 4326 and xRes/yRes
        # That grid's dimensions are not simply (x_max-x_min)/res due to reprojection distortion,
        # but for a small ROI like Kronstadt (0.36 deg) the distortion is minimal (<1%).
        # We can approximate by using the same method as delta: let gdal.Warp define the grid
        # Instead, we can directly rasterize in 4326 at a comparable resolution and then warp?
        # Easier: rasterize in 4326 at ~0.00013 deg (~15m at 60N, 1 deg lon ~55km, so 0.00027 deg ~15m)
        # Then the delta code will warp the mask similarly? No, delta's mask is in 3857 15m, we need same grid.
        # Alternative: create mask in 3857 by transforming SeaVox polygon to 3857 and rasterizing
        # on a grid that matches the delta's output grid exactly by using the same gdal.Warp logic.
        # To do that, we can create a temporary VRT or just use the same gdal.Warp to rasterize SeaVox
        # as a vector layer.
        # Simplest: transform SeaVox polygon to target CRS via pyproj and rasterize on a regular grid
        # defined by x_min/x_max/y_min/y_max and resolution.

        # Transform polygon to target CRS
        project = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True).transform
        seavox_3857 = shp_transform(project, seavox)

        # Define output dimensions (same as delta would: width = ceil((x_max - x_min)/res), height = ceil((y_max - y_min)/res))
        # Use ceil to ensure coverage
        import math
        width = max(1, math.ceil((x_max - x_min) / resolution))
        height = max(1, math.ceil((y_max - y_min) / resolution))
        # For Kronstadt 29.52,59.94,29.88,60.08 at 15m, width ~2672, height ~2079 as seen
        transform = from_origin(x_min, y_max, resolution, resolution)

        # Rasterize
        mask = rasterize(
            [(seavox_3857, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype="uint8",
        )

        # Cache it
        try:
            with rio.open(
                cache_path,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype="uint8",
                crs=crs,
                transform=transform,
                compress="DEFLATE",
                tiled=True,
                blockxsize=256,
                blockysize=256,
            ) as dst:
                dst.write(mask, 1)
        except Exception as e:
            print(f"Water mask cache write failed {cache_path}: {e}", flush=True)

        return mask
    except Exception as e:
        print(f"Water mask rasterize failed for {bbox_str}: {e}", flush=True)
        return None
