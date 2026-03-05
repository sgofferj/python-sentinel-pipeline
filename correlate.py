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

import os
import json
import constants as c
import functions as func
import legends
from datetime import datetime, timedelta
from shapely.wkt import loads
from shapely.geometry import mapping, shape
import rasterio as rio
from rasterio.warp import reproject, Resampling, transform_geom
from rasterio.features import rasterize
import numpy as np

def turbo_colormap(x):
    """Linear interpolation for a Turbo-like ramp (Blue -> Cyan -> Green -> Yellow -> Red)"""
    r = np.clip(np.where(x < 0.5, 0, np.where(x < 0.75, (x-0.5)/0.25, 1.0)), 0, 1)
    g = np.clip(np.where(x < 0.25, x/0.25, np.where(x < 0.75, 1.0, 1.0 - (x-0.75)/0.25)), 0, 1)
    b = np.clip(np.where(x < 0.25, 1.0, np.where(x < 0.5, 1.0 - (x-0.25)/0.25, 0)), 0, 1)
    return (r * 255).astype(np.uint8), (g * 255).astype(np.uint8), (b * 255).astype(np.uint8)

def load_log(sat):
    logfile = os.path.join(c.DIRS['DL'], f"{sat}_last.json")
    if os.path.exists(logfile):
        with open(logfile, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def find_overlaps(max_hours=24):
    s1_log = load_log('s1'); s2_log = load_log('s2')
    if not s1_log or not s2_log: return []

    matches = []
    for s2_feat in s2_log['files']:
        s2_props = s2_feat['properties']
        if not s2_props.get('footprint'): continue
        s2_time = datetime.fromisoformat(s2_props['startDate'].replace('Z', '+00:00'))
        s2_geom = loads(s2_props['footprint'])
        
        for s1_feat in s1_log['files']:
            s1_props = s1_feat['properties']
            if not s1_props.get('footprint'): continue
            s1_time = datetime.fromisoformat(s1_props['startDate'].replace('Z', '+00:00'))
            s1_geom = loads(s1_props['footprint'])
            
            if abs(s1_time - s2_time) < timedelta(hours=max_hours):
                if s2_geom.intersects(s1_geom):
                    # Footprint-based intersection for metadata discovery
                    inter_geom = s2_geom.intersection(s1_geom)
                    matches.append({'s1': s1_feat, 's2': s2_feat, 'inter_geom': inter_geom})
    return matches

def get_processed_paths(s1_feat, s2_feat):
    s1_title = s1_feat['properties']['title']
    s1_name = f"S1_{os.path.basename(s1_title).split('_')[4]}_{os.path.basename(s1_title).split('_')[5]}"
    vh_path = os.path.join(c.DIRS['S1_VH'], f"{s1_name}.tif")

    s2_props = s2_feat['properties']
    utm = s2_props['title'].split('_')[5]
    raw_start = s2_props['startDate']
    s2_time = raw_start.split('.')[0].replace('-', '').replace(':', '')
    if not s2_time.endswith('Z'): s2_time += 'Z'
    s2_name = f"{utm}-{s2_time}"
    tci_path = os.path.join(c.DIRS['S2_TCI'], f"{s2_name}-TCI.tif")
    return vh_path, tci_path, s2_name

def calculate_tight_window(inter_geom_4326, s2_crs):
    """Calculates clipped bounds with an internal buffer to kill swath edge noise."""
    geom_3857 = transform_geom('EPSG:4326', s2_crs, mapping(inter_geom_4326))
    # Buffer -500m to kill remaining S1 swath artifacts. 
    # The 9km stripe is now handled by pixel-wise alpha intersection.
    inter_shape = shape(geom_3857).buffer(-500)
    l, b, r, t = inter_shape.bounds
    res = 10.0
    w = int((r - l) / res); h = int((t - b) / res)
    transform = rio.transform.from_origin(l, t, res, res)
    return w, h, transform, inter_shape

def fuse_radar_optical(vh_path, tci_path, out_name, inter_geom, threshold=-15):
    if not os.path.exists(vh_path) or not os.path.exists(tci_path): return

    with rio.open(tci_path) as s2_src, rio.open(vh_path) as s1_src:
        out_w, out_h, out_transform, inter_poly = calculate_tight_window(inter_geom, s2_src.crs)
        print(f"Fusing Target Probe (Ghost Blend: {out_w}x{out_h})...")
        
        geom_mask = rasterize([inter_poly], out_shape=(out_h, out_w), transform=out_transform, fill=0, default_value=255, dtype=np.uint8)

        profile = s2_src.profile.copy()
        profile.update(width=out_w, height=out_h, transform=out_transform, compress='DEFLATE', tiled=True)
        
        out_path = os.path.join(c.DIRS['S1S2_FUSED'], f"{out_name}-RADAR-BURN.tif")
        with rio.open(out_path, 'w', **profile) as dst:
            for _, window in dst.block_windows(1):
                win_transform = rio.windows.transform(window, out_transform)
                s2_win = np.zeros((4, window.height, window.width), dtype=np.uint8)
                for b in range(1, 5):
                    reproject(rio.band(s2_src, b), destination=s2_win[b-1], src_transform=s2_src.transform, 
                              src_crs=s2_src.crs, dst_transform=win_transform, dst_crs=s2_src.crs, resampling=Resampling.bilinear)

                # Read processed S1 Intensity (Band 1) and S1 Alpha (Band 4)
                # Note: vh_src is already filtered and scaled in functions_s1.py
                s1_vh_win = np.zeros((window.height, window.width), dtype=np.uint8)
                reproject(rio.band(s1_src, 1), destination=s1_vh_win, src_transform=s1_src.transform, 
                          src_crs=s1_src.crs, dst_transform=win_transform, dst_crs=s1_src.crs, resampling=Resampling.bilinear)
                
                s1_alpha_win = np.zeros((window.height, window.width), dtype=np.uint8)
                reproject(rio.band(s1_src, 4), destination=s1_alpha_win, src_transform=s1_src.transform, 
                          src_crs=s1_src.crs, dst_transform=win_transform, dst_crs=s1_src.crs, resampling=Resampling.bilinear)
                
                # Convert 0-255 scaled intensity back to dB for thresholding
                vh_db = (s1_vh_win.astype(float) / 255.0) * (c.S1_dB_MAX - c.S1_dB_MIN) + c.S1_dB_MIN
                
                # Comprehensive Alpha intersection (S1 Alpha * S2 Alpha * Metadata Mask)
                win_geom_mask = geom_mask[window.row_off:window.row_off+window.height, window.col_off:window.col_off+window.width]
                s2_win[3] = (s2_win[3].astype(float) * 
                             (win_geom_mask.astype(float) / 255.0) * 
                             (s1_alpha_win.astype(float) / 255.0)).astype(np.uint8)

                # Target Highlight (40% Turbo opacity over 60% TCI)
                target_mask = (vh_db > threshold) & (s2_win[3] > 0)
                turbo_x = np.clip((vh_db[target_mask] - threshold) / abs(threshold), 0, 1)
                tr, tg, tb = turbo_colormap(turbo_x)
                
                s2_win[0][target_mask] = np.clip(tr * 0.4 + s2_win[0][target_mask] * 0.6, 0, 255).astype(np.uint8)
                s2_win[1][target_mask] = np.clip(tg * 0.4 + s2_win[1][target_mask] * 0.6, 0, 255).astype(np.uint8)
                s2_win[2][target_mask] = np.clip(tb * 0.4 + s2_win[2][target_mask] * 0.6, 0, 255).astype(np.uint8)
                
                # Mask RGB bands with final alpha to prevent unmasked data leakage
                mask_norm = s2_win[3].astype(float) / 255.0
                for b in range(3):
                    s2_win[b] = (s2_win[b].astype(float) * mask_norm).astype(np.uint8)

                dst.write(s2_win, window=window)
            dst.build_overviews([2, 4, 8, 16], Resampling.average)

def fuse_life_machine(s1_feat, s2_feat, out_name, inter_geom):
    vh_path, tci_path, s2_base = get_processed_paths(s1_feat, s2_feat)
    nirfc_path = os.path.join(c.DIRS['S2_NIRFC'], f"{s2_base}-NIRFC.tif")
    if not all(os.path.exists(p) for p in [vh_path, tci_path, nirfc_path]): return

    with rio.open(tci_path) as tci_src, rio.open(vh_path) as vh_src, rio.open(nirfc_path) as nirfc_src:
        out_w, out_h, out_transform, inter_poly = calculate_tight_window(inter_geom, tci_src.crs)
        print(f"Creating Discovery Composite (Functional Mapping: {out_w}x{out_h})...")
        
        geom_mask = rasterize([inter_poly], out_shape=(out_h, out_w), transform=out_transform, fill=0, default_value=255, dtype=np.uint8)

        profile = tci_src.profile.copy()
        profile.update(width=out_w, height=out_h, transform=out_transform, count=4, compress='DEFLATE', tiled=True)
        
        out_path = os.path.join(c.DIRS['S1S2_FUSED'], f"{s2_base}-LIFE-MACHINE.tif")
        with rio.open(out_path, 'w', **profile) as dst:
            for _, window in dst.block_windows(1):
                win_transform = rio.windows.transform(window, out_transform)
                def read_win(src, b_idx):
                    arr = np.zeros((window.height, window.width), dtype=np.uint8)
                    reproject(rio.band(src, b_idx), destination=arr, src_transform=src.transform, 
                              src_crs=src.crs, dst_transform=win_transform, dst_crs=src.crs, resampling=Resampling.bilinear)
                    return arr

                nir = read_win(nirfc_src, 1).astype(float); red_band = read_win(tci_src, 1).astype(float)
                blue_band = read_win(tci_src, 3); vh = read_win(vh_src, 1); alpha = read_win(tci_src, 4)
                s1_alpha = read_win(vh_src, 4)
                
                # Machine (Red): SAR Intensity. Already optimized in functions_s1.py
                # Scaled for structural pop.
                vh_boosted = np.clip(vh.astype(float) * 1.5, 0, 255).astype(np.uint8)

                # Life (Green): NDVI. Strictly vegetation signal.
                # Adjusted scale to 0.8 to preserve forest texture
                denom = nir + red_band; ndvi = np.zeros_like(nir); m = (denom != 0); ndvi[m] = (nir[m] - red_band[m]) / denom[m]
                ndvi_scaled = np.clip((ndvi - 0.0) / 0.8 * 255, 0, 255).astype(np.uint8)
                
                # Context (Blue): Optical Blue band. Provides natural winter detail.
                # Add a small floor to prevent deep shadows from being pitch black
                context_blue = np.clip(blue_band * 1.2 + 20, 0, 255).astype(np.uint8)

                # Comprehensive Alpha intersection (S1 Alpha * S2 Alpha * Metadata Mask)
                win_geom_mask = geom_mask[window.row_off:window.row_off+window.height, window.col_off:window.col_off+window.width]
                alpha_final = (alpha.astype(float) * 
                               (win_geom_mask.astype(float) / 255.0) * 
                               (s1_alpha.astype(float) / 255.0)).astype(np.uint8)
                
                # Mask RGB bands with final alpha to prevent unmasked data leakage
                mask_norm = alpha_final.astype(float) / 255.0
                vh_boosted = (vh_boosted.astype(float) * mask_norm).astype(np.uint8)
                ndvi_scaled = (ndvi_scaled.astype(float) * mask_norm).astype(np.uint8)
                context_blue = (context_blue.astype(float) * mask_norm).astype(np.uint8)

                dst.write(np.stack([vh_boosted, ndvi_scaled, context_blue, alpha_final], axis=0), window=window)
            dst.build_overviews([2, 4, 8, 16], Resampling.average)

def run_correlation():
    matches = find_overlaps()
    if not matches: return
    print(f"Found {len(matches)} potential S1/S2 matches.")
    for match in matches:
        vh_path, tci_path, s2_name = get_processed_paths(match['s1'], match['s2'])
        fuse_radar_optical(vh_path, tci_path, s2_name, match['inter_geom'])
        fuse_life_machine(match['s1'], match['s2'], s2_name, match['inter_geom'])

    # Update HTML legends for frontend consumption
    legends.save_all_legends(os.path.join(c.DIRS['OUT'], "scripts"))

if __name__ == "__main__":
    run_correlation()
