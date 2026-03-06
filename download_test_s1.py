#!/usr/bin/env python3
import os
import zipfile
import time
from copernicus import connect
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
# Strip quotes if they were included in the .env file
USERNAME = USERNAME.strip('"')
PASSWORD = PASSWORD.strip('"')

# Bounding box for Upinniemi (small area)
BOX = "24.303818,59.984906,24.401321,60.041018"

print(f"Connecting to Copernicus as {USERNAME}...")
mycop = connect(USERNAME, PASSWORD)
if hasattr(mycop, "error"):
    print(f"Connection failed: {mycop.error}")
    exit(1)
print(f"Connection status: {mycop.status}")

print("Searching for the latest Sentinel-1 GRD IW product...")
status, search_result = mycop.productSearch(
    "Sentinel1", maxRecords=1, productType="GRD", sensorMode="IW"
)

if status != 200 or not search_result.get("features"):
    print(f"Search failed or no products found. Status: {status}")
    print(search_result)
    exit(1)

feature = search_result["features"][0]
uuid = feature["id"]
filename = feature["properties"]["title"]

print(f"Found product: {filename}")
print(f"UUID: {uuid}")

# Ensure we download to the data directory
dl_dir = "data"
zip_path = os.path.join(dl_dir, f"{filename}.zip")
safe_path = os.path.join(dl_dir, f"{filename}.SAFE")

if os.path.exists(safe_path):
    print(f"Product already exists at {safe_path}. Skipping download.")
elif os.path.exists(zip_path):
    print(f"ZIP file already exists at {zip_path}. Using existing ZIP.")
    print("Unzipping...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dl_dir)
    os.remove(zip_path)
    print(f"Extraction complete: {safe_path}")
else:
    print(f"Downloading to {zip_path}...")
    mycop.download(uuid, filename, dl_dir)

    print("Unzipping...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dl_dir)

    os.remove(zip_path)
    print(f"Download and extraction complete: {safe_path}")

# Run a quick check with the new calibrator
from s1_calibrator import S1Calibrator

print("\nTesting S1Calibrator...")
try:
    cal = S1Calibrator(safe_path)
    # We'll just try to calibrate a small block or just check the XML parsing for now
    # to avoid a massive processing run if not needed.
    # But let's try a full calibration of VV to see it in action!
    output_tif = os.path.join("output", f"{filename}_VV_sigma0.tif")
    os.makedirs("output", exist_ok=True)

    print(f"Starting calibration of VV to {output_tif}...")
    cal.calibrate("VV", output_tif, block_size=1024)
    print("Calibration test successful!")
except Exception as e:
    print(f"Calibration test failed: {e}")
    import traceback

    traceback.print_exc()
