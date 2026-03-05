import rasterio as rio
import numpy as np
import os
import constants as c

def analyze_tif(path):
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return
    
    with rio.open(path) as src:
        data = src.read()
        # NDBI is often stored as RGB in the output, but let's see if we can find 
        # the raw values. Actually, the pipeline writes RGB.
        # We need to analyze the RAW NDBI before it's colorized.
        print(f"Analyzing {os.path.basename(path)}")
        print(f"Shape: {data.shape}, Dtype: {data.dtype}")
        for i in range(data.shape[0]):
            band = data[i]
            valid = band[band > 0]
            if valid.size > 0:
                print(f"Band {i+1} - Min: {np.min(valid)}, Max: {np.max(valid)}, Mean: {np.mean(valid)}")

if __name__ == "__main__":
    # Find the NDBI file
    ndbi_dir = c.DIRS['S2_NDBI']
    files = [f for f in os.listdir(ndbi_dir) if f.endswith(".tif")]
    if files:
        analyze_tif(os.path.join(ndbi_dir, files[0]))
    else:
        print("No NDBI files found.")
