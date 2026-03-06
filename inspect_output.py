#!/usr/bin/env python3
import rasterio as rio
import numpy as np

path = "output/S1A_VV_Sigma0_denoised.tif"

print(f"Inspecting: {path}")
with rio.open(path) as src:
    data = src.read(1)
    # Exclude zeros (likely border or noise floor clipping)
    valid_data = data[data > 0]

    print(f"Shape: {data.shape}")
    print(f"Dtype: {data.dtype}")
    print(f"Min: {np.min(data):.6f}")
    print(f"Max: {np.max(data):.6f}")
    print(f"Mean (valid): {np.mean(valid_data):.6f}")
    print(f"Std (valid): {np.std(valid_data):.6f}")

    # Calculate decibels: 10 * log10(sigma0)
    db = 10 * np.log10(valid_data)
    print(f"Mean dB: {np.mean(db):.2f} dB")
    print(f"Max dB: {np.max(db):.2f} dB")
    print(f"Min dB: {np.min(db):.2f} dB")
