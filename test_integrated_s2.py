#!/usr/bin/env python3
import os
import sys
import glob
from osgeo import gdal
import functions_s2 as s2
import constants as c
import copernicus as cop
from dotenv import load_dotenv
import zipfile

load_dotenv()

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)

# Pick the existing Finland S2 package from temp for testing
filename = "S2C_MSIL2A_20260220T095031_N0512_R079_T35VLJ_20260220T131719.SAFE"

# consolidated test directory
TEST_DIR = "output/test"
os.makedirs(TEST_DIR, exist_ok=True)

try:
    print(f"Regenerating images for Instagram from: {filename}")

    # 1. Process
    manifest = f"{c.DIRS['DL']}/{filename}/MTD_MSIL2A.xml"
    if not os.path.exists(manifest):
        manifest = f"{c.DIRS['DL']}/{filename}/MTD_MSIL1C.xml"

    ds = gdal.Open(manifest)
    if ds is None:
        print(f"Error: Could not open manifest at {manifest}")
        sys.exit(1)

    s2.prepare(ds)

    utm = s2.get_utm(filename)
    time_str = s2.get_time(filename)
    name = f"{utm}-{time_str}Z"

    # Render everything with overviews so they are fast to look at
    s2.render(
        f"{TEST_DIR}/{name}",
        ["TCI", "NIRFC", "AP", "NDVI", "NDBI", "NDRE", "NBR", "CAMO"],
    )
    s2.cleanup()

    print("\nREGENERATION SUCCESSFUL!")
    print(f"Images are ready in: {os.path.abspath(TEST_DIR)}")

except Exception as e:
    print(f"\nERROR during regeneration: {e}")
    import traceback

    traceback.print_exc()
finally:
    ds = None
