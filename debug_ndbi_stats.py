import rasterio as rio
import numpy as np
import os

# Updated paths relative to Sat/python-sentinel-pipeline
b08_path = "temp/S2C_MSIL2A_20260213T100131_N0512_R122_T35VLJ_20260213T141213.SAFE/GRANULE/L2A_T35VLJ_A007528_20260213T100310/IMG_DATA/R10m/T35VLJ_20260213T100131_B08_10m.jp2"
b11_path = "temp/S2C_MSIL2A_20260213T100131_N0512_R122_T35VLJ_20260213T141213.SAFE/GRANULE/L2A_T35VLJ_A007528_20260213T100310/IMG_DATA/R20m/T35VLJ_20260213T100131_B11_20m.jp2"

def analyze():
    if not os.path.exists(b08_path):
        print(f"ERROR: {b08_path} not found")
        return
    if not os.path.exists(b11_path):
        print(f"ERROR: {b11_path} not found")
        return

    with rio.open(b08_path) as s8, rio.open(b11_path) as s11:
        w, h = s11.width, s11.height
        # Sample a 2000x2000 chunk from the middle
        win_11 = rio.windows.Window(w//4, h//4, 2000, 2000)
        # B08 is 10m, B11 is 20m. We need matching shapes.
        # B08 has 2x resolution.
        win_08 = rio.windows.Window(w//2, h//2, 4000, 4000)
        
        swir = s11.read(1, window=win_11).astype(float)
        nir = s8.read(1, window=win_08, out_shape=(2000, 2000)).astype(float)
        
        denom = swir + nir
        ndbi = np.zeros_like(swir)
        m = (denom != 0)
        ndbi[m] = (swir[m] - nir[m]) / denom[m]
        
        valid_ndbi = ndbi[m]
        print(f"NDBI Sample Stats:")
        print(f"Min: {np.min(valid_ndbi):.4f}")
        print(f"Max: {np.max(valid_ndbi):.4f}")
        print(f"Mean: {np.mean(valid_ndbi):.4f}")
        for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
            print(f"{p}th Percentile: {np.percentile(valid_ndbi, p):.4f}")

if __name__ == "__main__":
    analyze()
