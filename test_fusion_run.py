import os
import copernicus as cop
import constants as c
import functions as func
import functions_s1 as s1
import functions_s2 as s2
import inventory_manager
from search import writelog
from correlate import run_correlation
from osgeo import gdal
from dotenv import load_dotenv
import zipfile

load_dotenv()

# Use existing datasets in temp
S1_ID = "7714f38e-585f-417d-9478-16a8e22d932c"
S2_ID = "2be06fc3-0fd3-400a-9993-df6cb6ae0dfc"

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
try:
    mycop = cop.connect(USERNAME, PASSWORD)
except Exception as e:
    print(f"Warning: Could not connect to Copernicus: {e}. Proceeding with local data.")
    mycop = None

def download_one(fileid, filename):
    out_dir = os.path.join(c.DIRS['DL'], filename)
    if os.path.exists(out_dir):
        print(f"Already have {filename}")
        return
    if mycop is None:
        raise ConnectionError(f"Local data missing for {filename} and no Copernicus connection available.")
    print(f"Downloading {filename}...")
    mycop.download(fileid, filename, c.DIRS["DL"])
    zip_path = os.path.join(c.DIRS['DL'], f"{filename}.zip")
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(c.DIRS['DL'])
    os.remove(zip_path)

def fetch_real_metadata(fileid):
    if mycop is None:
        return {"id": fileid, "properties": {"title": "Unknown", "startDate": "2026-02-13T10:00:00Z", "footprint": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"}}
    return mycop.getMetadata(fileid)

def test_fusion():
    # 1. Download
    s1_name = "S1A_IW_GRDH_1SDV_20260213T045812_20260213T045837_063196_07EEFE_B7AD.SAFE"
    s2_name = "S2C_MSIL2A_20260213T100131_N0512_R122_T35VLJ_20260213T141213.SAFE"
    
    download_one(S1_ID, s1_name)
    download_one(S2_ID, s2_name)

    # 2. Process S1
    print("\nProcessing S1...")
    ds1 = gdal.Open(os.path.join(c.DIRS['DL'], s1_name, "manifest.safe"))
    s1.run_pipeline(ds1, ["VV", "VH", "RATIOVVVH"])

    # 3. Process S2
    print("\nProcessing S2...")
    ds2 = gdal.Open(os.path.join(c.DIRS['DL'], s2_name, f"MTD_MSIL2A.xml"))
    s2.run_pipeline(ds2, ["TCI", "NIRFC", "NDVI", "NDBI", "NDRE", "NBR", "CAMO", "NDBI_CLEAN"])

    # 4. Generate Logs for correlation
    print("\nFetching official footprints from API...")
    s1_meta = fetch_real_metadata(S1_ID)
    s2_meta = fetch_real_metadata(S2_ID)
    
    # Overwrite titles with real SAFE names if fetched
    s1_meta['properties']['title'] = s1_name
    s2_meta['properties']['title'] = s2_name
    
    writelog("s1", [s1_meta])
    writelog("s2", [s2_meta])

    # 5. Correlate and Fuse
    print("\nRunning Fusion...")
    run_correlation()
    
    # 6. Finalize
    inventory_manager.rebuild_inventory()
    func.perf_logger.stop_run()

if __name__ == "__main__":
    func.perf_logger.start_run()
    test_fusion()
