import rasterio as rio
import numpy as np

def check_tmp():
    try:
        with rio.open("/tmp/vv.tif") as src:
            print(f"VV.tif - Width: {src.width}, Height: {src.height}, Count: {src.count}")
            # Read middle window
            w = src.width; h = src.height
            win = rio.windows.Window(w//2, h//2, 256, 256)
            data = src.read(1, window=win)
            alpha = src.read(2, window=win)
            print(f"Band 1 - Non-zero: {np.count_nonzero(data)}")
            print(f"Band 1 - Max: {np.max(data)}")
            print(f"Band 2 - Non-zero: {np.count_nonzero(alpha)}")
            print(f"Band 2 - Max: {np.max(alpha)}")
    except Exception as e:
        print(f"Error checking /tmp/vv.tif: {e}")

if __name__ == "__main__":
    check_tmp()
