#!/usr/bin/env python3
import json
import os
from shapely.geometry import shape, mapping

inv_path = "/media/sgofferj/3F26D5F050FDC36C/Sat/output/visual/inventory.json"
out_path = "/media/sgofferj/3F26D5F050FDC36C/Sat/output/visual/inventory_new.json"

if not os.path.exists(inv_path):
    print(f"Error: {inv_path} not found")
    exit(1)

print(f"Loading {inv_path}...")
with open(inv_path, 'r') as f:
    data = json.load(f)

print(f"Loaded {len(data['layers'])} layers. Simplifying...")

for i, layer in enumerate(data['layers']):
    if 'footprint' in layer and layer['footprint']:
        try:
            g = shape(layer['footprint'])
            simplified = g.simplify(0.0001, preserve_topology=True)
            layer['footprint'] = mapping(simplified)
            print(f"Processed layer {i+1}/{len(data['layers'])}: {layer.get('product', 'Unknown')}", flush=True)
        except Exception as e:
            print(f"Error simplifying layer {i}: {e}")

print(f"Writing {out_path}...")
with open(out_path, 'w') as f:
    json.dump(data, f, indent=2)

print("Done! Check sizes:")
os.system(f"ls -lh {inv_path} {out_path}")
