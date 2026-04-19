#!/usr/bin/env python3
import os
import gc
import time
from concurrent.futures import ThreadPoolExecutor
import metadata_engine as meta
import constants as c

# Root of all visual products
visual_root = "/media/sgofferj/3F26D5F050FDC36C/Sat/output/visual"

print("Starting deep parallel repair of all sidecars (Extreme Compression + Correct Naming)...")

tasks = []

# We will walk through the directory and find every .tif file
for root, _, files in os.walk(visual_root):
    for f in files:
        if f.endswith(".tif"):
            tif_path = os.path.join(root, f)
            
            # Extract folder info
            parts = [p.lower() for p in root.split(os.sep)]
            filename_upper = f.upper()
            
            # --- Robust Product Identification ---
            full_p_type = "Unknown"
            legend_id = "Unknown"

            if "s1" in parts:
                if "ratio" in parts: 
                    full_p_type = "S1-RATIO"
                elif "vv" in parts: 
                    full_p_type = "S1-VV"
                elif "vh" in parts: 
                    full_p_type = "S1-VH"
                legend_id = full_p_type
            elif "s2" in parts:
                # Folder name is the product (ndvi, tci, etc)
                p_folder = root.split(os.sep)[-1].upper()
                full_p_type = f"S2-{p_folder}"
                legend_id = full_p_type
            elif "fused" in parts:
                # Detect which fusion product it is from the filename
                if "RADAR-BURN" in filename_upper:
                    full_p_type = "FUSED-RADAR-BURN"
                    legend_id = "RADAR-BURN"
                elif "LIFE-MACHINE" in filename_upper:
                    full_p_type = "FUSED-LIFE-MACHINE"
                    legend_id = "LIFE-MACHINE"
                elif "TARGET-PROBE-V2" in filename_upper:
                    full_p_type = "FUSED-TARGET-PROBE-V2"
                    legend_id = "TARGET-PROBE-V2"
            
            if full_p_type != "Unknown":
                tasks.append((tif_path, full_p_type, legend_id))

def repair_one(task):
    tif_path, full_p_type, legend_id = task
    json_path = tif_path.replace(".tif", ".json")
    print(f"Repairing: {os.path.basename(tif_path)} as {full_p_type}...", flush=True)
    
    # Remove the old sidecar first
    if os.path.exists(json_path):
        os.remove(json_path)
    
    try:
        # Call the optimized engine with correct naming
        # Full_p_type must be SAT-TYPE format for the viewer's splitter
        meta.generate_sidecar(tif_path, full_p_type, legend_id)
    except Exception as e:
        print(f"Failed to repair {os.path.basename(tif_path)}: {e}")

start_time = time.time()
# Use a pool of workers (max 8 for I/O and some CPU)
with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(repair_one, tasks)

elapsed = time.time() - start_time
print(f"\nRepair complete in {elapsed:.2f}s. Now rebuilding final inventory...")
os.system("./venv/bin/python3 inventory_manager.py")
print("\nDone! Your inventory should now be tiny and correctly grouped.")
