#!/usr/bin/env python3
import json
import os
from shapely.geometry import shape, mapping, MultiPolygon
from shapely.ops import unary_union

visual_root = "/media/sgofferj/3F26D5F050FDC36C/Sat/output/visual"

# Find all sidecars EXCEPT the global inventory
sidecars = []
for root, _, files in os.walk(visual_root):
    for f in files:
        if f.endswith(".json") and f != "inventory.json":
            sidecars.append(os.path.join(root, f))

print(f"Found {len(sidecars)} sidecar files to fix.")

for i, path in enumerate(sidecars):
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        if 'footprint' in data and data['footprint']:
            # Load as a Shapely object
            g = shape(data['footprint'])
            
            # 1. Fix geometry and merge islands
            # simplify(0.0001) is ~11 meters, perfect for S1 noise
            refined = g.buffer(0).simplify(0.0001, preserve_topology=True)
            
            # 2. Filter out tiny noise islands (important for S1-RATIO)
            # If it's a MultiPolygon, keep only parts larger than 0.001 deg^2 (~10km2 approx)
            # or just keep the top 10 largest pieces.
            if refined.geom_type == 'MultiPolygon':
                parts = sorted(refined.geoms, key=lambda p: p.area, reverse=True)
                # Keep significant chunks only. Noise is usually tiny.
                # Threshold of 1km2 (0.0001 deg2 approx)
                significant = [p for p in parts if p.area > 0.0001]
                if not significant:
                    # If everything is tiny, just keep the biggest piece
                    refined = parts[0]
                else:
                    refined = MultiPolygon(significant) if len(significant) > 1 else significant[0]
                
                # Re-simplify after reduction
                refined = refined.simplify(0.0001, preserve_topology=True)

            data['footprint'] = mapping(refined)
            
            # Count final points for logging
            final_points = 0
            if data['footprint']['type'] == 'Polygon':
                final_points = sum(len(r) for r in data['footprint']['coordinates'])
            else:
                for poly in data['footprint']['coordinates']:
                    final_points += sum(len(r) for r in poly)

            print(f"[{i+1}/{len(sidecars)}] {os.path.basename(path)} -> {final_points} points.", flush=True)
            
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        else:
            print(f"[{i+1}/{len(sidecars)}] Skipping {os.path.basename(path)} (no footprint).")
            
    except Exception as e:
        print(f"Error processing {path}: {e}")

print("\nDone! All sidecars are now properly compacted.")
