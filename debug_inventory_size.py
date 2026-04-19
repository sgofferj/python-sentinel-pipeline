import json
import os

path = "/media/sgofferj/3F26D5F050FDC36C/Sat/output/visual/inventory.json"
with open(path, 'r') as f:
    data = json.load(f)

sizes = []
for layer in data['layers']:
    layer_str = json.dumps(layer)
    sizes.append((len(layer_str), layer['path'], layer.get('product')))

sizes.sort(key=lambda x: x[0], reverse=True)

print("Top 10 Largest Layers:")
for size, l_path, prod in sizes[:10]:
    print(f"{size/1024/1024:.2f} MB | {prod} | {l_path}")

# Inspect the very first one
largest = data['layers'][0]
if 'footprint' in largest and largest['footprint']:
    coords = largest['footprint']['coordinates']
    print(f"\nLargest layer coordinate count: {len(str(coords))}")
    if largest['footprint']['type'] == 'MultiPolygon':
        print(f"Number of parts: {len(coords)}")
    else:
        print(f"Number of rings: {len(coords)}")
