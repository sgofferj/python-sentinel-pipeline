#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# rebuild_metadata.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Maintenance utility to regenerate all .json sidecar files for existing visual TIFFs.
This updates sidecars with new fields like precise footprints and resolution.
"""

import os
import re
import metadata_engine
import inventory_manager
import constants as c

def rebuild_all():
    print("--- Starting Bulk Metadata Regeneration ---", flush=True)
    visual_root = os.path.join(c.DIRS["OUT"], "visual")
    count = 0

    # Resolution mapping (Effective resolution in m/px)
    RES_MAP = {
        "S1-VV": 15.0,
        "S1-VH": 15.0,
        "S1-RATIO": 15.0,
        "S2-TCI": 10.0,
        "S2-NDVI": 10.0,
        "S2-NIRFC": 10.0,
        "S2-AP": 20.0,
        "S2-NDBI": 20.0,
        "S2-NDBI_CLEAN": 20.0,
        "S2-NDRE": 20.0,
        "S2-NBR": 20.0,
        "S2-CAMO": 20.0,
        "LIFE-MACHINE": 10.0,
        "RADAR-BURN": 10.0,
        "TARGET-PROBE-V2": 10.0
    }

    # Walk through all visual subdirectories
    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".tif"):
                tif_path = os.path.join(root, file)
                
                try:
                    # Determine product type and legend ID from directory structure
                    # Path is like: .../visual/s2/ndvi/T35VLG-...-NDVI.tif
                    # Fused: .../visual/fused/T34VFM-...-LIFE-MACHINE.tif
                    parts = tif_path.split(os.sep)
                    # Find index of 'visual'
                    idx = parts.index("visual")
                    sat = parts[idx + 1].upper() # S1, S2, FUSED
                    
                    if sat == "FUSED":
                        # For Fused, the product name is the suffix after the second dash in filename
                        # e.g. T34VFM-20260408T094031Z-LIFE-MACHINE.tif -> LIFE-MACHINE
                        # Or just use parts[idx+2] if it's organized by subdir
                        p_type = parts[idx + 2].upper() 
                        # If its flat in fused/ dir, extract from filename
                        if p_type == os.path.basename(tif_path).upper() or p_type == "FUSED":
                             m = re.search(r"-(LIFE-MACHINE|RADAR-BURN|TARGET-PROBE-V2)\.tif", file, re.I)
                             if m: 
                                 p_type = m.group(1).upper()
                        
                        product_id = f"FUSED-{p_type}"
                        legend_id = p_type
                    else:
                        p_type = parts[idx + 2].upper() # NDVI, VH, etc.
                        product_id = f"{sat}-{p_type}"
                        legend_id = product_id
                    
                    eff_res = RES_MAP.get(product_id)
                    print(f"Regenerating sidecar for: {file} ({product_id}) @ {eff_res}m", flush=True)
                    metadata_engine.generate_sidecar(tif_path, product_id, legend_id, effective_res=eff_res)
                    count += 1
                except (ValueError, IndexError) as e:
                    print(f"Skipping {file}: Could not determine product type from path. {e}", flush=True)

    print(f"\nSuccessfully regenerated {count} sidecar files.", flush=True)
    
    print("\nRebuilding global inventory...", flush=True)
    inventory_manager.rebuild_inventory()
    
    print("--- Regeneration Complete ---", flush=True)

if __name__ == "__main__":
    rebuild_all()
