# python-sentinel-pipeline

(C) 2025-2026 Stefan Gofferje

Licensed under the GNU General Public License V3 or later.

This is an automated pipeline designed to grab and process Sentinel-1 (Radar), Sentinel-2 (Optical), and Sentinel-3 (SLSTR Thermal) imagery from the [ESA Copernicus Dataspace Ecosystem (CDSE)](https://dataspace.copernicus.eu/).

The goal is to produce physically consistent, high-contrast imagery optimized for OSINT, change detection, and cross-sensor fusion without having to manually fiddle with SNAP or heavy GIS suites every time a new tile drops.

> [!WARNING]
> Sentinel-2 data is heavy. You need at least 16GB of RAM and ideally an SSD for the `temp/` directory.

## What it does

### Dual-Purpose Output

- **Visual (RGBA):** 8-bit Cloud Optimized GeoTIFFs (COGs). These are normalized for tile-to-tile consistency (fixed reflectance scaling) and include automated legends and compact JSON metadata sidecars for web viewers (like OpenLayers).
- **Analytic (Float32):** Single-band rasters preserving absolute physical units (dB for Radar, Reflectance for Optical). Essential for statistical analysis and automated change detection.

### Smart Processing

- **Single-Pass Rendering:** Indices and visual products are calculated in a single windowed loop to minimize Disk I/O.
- **Memory Safety:** Parallelism is constrained by `MAX_PARALLEL_FINALIZERS` and single-threaded GDAL sub-processes to prevent OOM kills on 16GB systems.
- **Lean Metadata:** Footprints are generated using 100m downsampling with recursive hole-filling and coordinate rounding. This makes sidecar JSONs ~100x smaller and faster to generate.
- **Automatic Dependencies:** If you ask for a fusion product (like `RADAR-BURN`), the pipeline automatically ensures all required analytic source products (VH, NDVI, etc.) are generated first.
- **Automated ROI Cropping:** Extracts crops for specific Regions of Interest (ROIs) defined in `roi_config.json`. Supports coverage-based filtering and automated social media/notification updates. [See README_ROI.md for details](./README_ROI.md).
- **GPU Acceleration:** If `cupy` is installed and a CUDA-capable GPU is found, multispectral index math is automatically offloaded to the GPU.

### OSINT & Specialty Products

- **RADAR-BURN:** Overlays S1-VH radar detections (metallic/dense signatures) onto S2-TCI imagery using a high-contrast ghost blend. Ideal for spotting vessels or infrastructure hidden in optical noise.
- **NDBI_CLEAN:** A vegetation-decoupled building index designed to spot infrastructure in dense environments.
- **CAMO:** Discovery composite for spotting anomalies in rural/forested terrain.
- **TARGET-PROBE-V2:** Sensor fusion gating building signatures with radar returns.
- **LIFE-MACHINE:** Combined SAR/Optical discovery composite for distinguishing natural terrain from man-made structures.
- **Guided Filter (TCI-GF / NIRFC-GF / AP-GF):** Edge-preserving smoothing and detail enhancement using Kaiming He's guided filter. TCI-GF uses the 10m NIR band (B08) as guidance to transfer high-frequency edge information into the RGB composite. NIRFC-GF uses self-guidance. AP-GF sharpens 20m SWIR (B11/B12) to 10m using a synthetic panchromatic guide (`0.1·B02+0.1·B03+0.4·B04+0.4·B08`) to avoid NIR-vs-SWIR vegetation halos. All apply detail enhancement (`output = base + strength × detail`) to recover sharpness lost during smoothing.
- **AIS Correlation:** Correlates satellite imagery with historical AIS vessel data from a [python-ais-recorder](https://github.com/sgofferj/python-ais-recorder) API. Circles and data blocks are plotted onto S1-Ratio or S2-TCI images to identify ships at the exact moment of acquisition.
- **Overflight Prediction:** Predicts the next pass of Sentinel-1, Sentinel-2, and Sentinel-3 satellites over your configured bounding boxes using high-precision Skyfield propagation and Celestrak TLEs. Generates GeoJSON swath overlays that can be toggled in the viewer.
- **SLSTR Fire Detection (S3):** Sentinel-3 SLSTR thermal band processing for active fire monitoring. Brightness Temperature (BT) composites map S8/S7/S9 bands to RGB false-colour. FIRE detection products use a hot-body colormap (transparent for cold backgrounds, dark red→yellow→white for fire intensity). Daily revisit enables fire progression tracking where S2's 5-day cycle falls short.

## Configuration

Settings are handled via a `.env` file. The pipeline supports dynamic variable expansion (e.g., `S1_BOX = ${BOX_GULF}`), allowing you to define your coordinates once and reuse them across different parameters.

### CDSE Credentials

| Variable              | Description                |
| :-------------------- | :------------------------- |
| `COPERNICUS_USERNAME` | Your CDSE account email    |
| `COPERNICUS_PASSWORD` | Your CDSE account password |

### Core Control

| Variable            | Description                                                            | Default |
| :------------------ | :--------------------------------------------------------------------- | :------ |
| `PIPELINES`         | `S1,S2,FUSION` (comma-separated list)                                  | `S1,S2` |
| `USE_LOG`           | Skip products already processed (uses `s1_last.json` / `s2_last.json`) | `True`  |
| `TARGET_DIR`        | Root directory for the `output/` folder                                | `.`     |
| `CLEANUP_AFTER_RUN` | Automatically delete old raw data after successful processing              | `False` |
| `CLEANUP_DAYS`      | Number of days to keep raw data                                             | `30`    |
| `CLEANUP_ROI_DAYS`  | Keep ROI crops for this many days (can be longer than `CLEANUP_DAYS`)       | `30`    |
| `CLEANUP_S1_DAYS`   | Override `CLEANUP_DAYS` for Sentinel-1 products only                       | —       |
| `CLEANUP_S2_DAYS`   | Override `CLEANUP_DAYS` for Sentinel-2 products only                       | —       |
| `CLEANUP_S3_DAYS`   | Override `CLEANUP_DAYS` for Sentinel-3 products only                       | —       |
| `CLEANUP_S2_VERSIONS` | Keep at most N S2 products per UTM tile (overrides `CLEANUP_S2_DAYS`)       | —       |
| `CLEANUP_FUSION_DAYS` | Override `CLEANUP_DAYS` for fusion products only (RADAR-BURN, etc.)      | —       |
| `CLEANUP_RAW`       | Delete source .SAFE/.SEN3 directories immediately after processing          | `True`  |
| `EXECUTE_AFTER_PIPELINE` | Optional command or script to run after pipeline (e.g., rsync)       | —       |
| `APPRISE_URLS`      | Optional [Apprise](https://github.com/caronc/apprise) URIs for alerts      | —       |

### Performance & Hardware

| Variable                  | Description                                          | Default            |
| :------------------------ | :--------------------------------------------------- | :----------------- |
| `PIPELINE_WORKERS`        | Concurrent threads for warping and index calculation | `2`                |
| `MAX_PARALLEL_FINALIZERS` | Concurrent threads for COG and Sidecar generation    | `2`                |
| `DISABLE_GPU`             | Force CPU mode even if CUDA/CuPy is available        | `False`            |
| `ENABLE_GPU_WARP`         | Use experimental CUDA-accelerated warping for S1     | `False`            |
| `GDAL_NUM_THREADS`        | Number of threads for GDAL internal operations       | `PIPELINE_WORKERS` |

### Sentinel-1 (Radar) Parameters

| Variable         | Description                                                                                                                                       | Default           |
| :--------------- | :------------------------------------------------------------------------------------------------------------------------------------------------ | :---------------- |
| `S1_BOX`         | Search area coordinates: `West,South,East,North`. Supports single box, semicolon-separated list (`box1;box2`), or JSON list (`["box1", "box2"]`). | -                 |
| `S1_STARTDATE`   | Earliest sensing date (YYYY-MM-DD)                                                                                                                | Yesterday         |
| `S1_MAXRECORDS`  | Maximum number of products to download per box                                                                                                    | `5`               |
| `S1_PRODUCTTYPE` | `GRD` (Ground Range Detected) is standard                                                                                                         | `GRD`             |
| `S1_SENSORMODE`  | `IW` (Interferometric Wide Swath) is standard                                                                                                     | `IW`              |
| `S1_SORTPARAM`   | CDSE sorting parameter (e.g., `startDate`)                                                                                                        | `startDate`       |
| `S1_SORTORDER`   | `descending` or `ascending`                                                                                                                       | `descending`      |
| `S1_PROCESSES`   | `VV, VH, RATIOVVVH, AIS`                                                                                                                         | `VV,VH,RATIOVVVH` |

### Sentinel-2 (Optical) Parameters

| Variable         | Description                                                                                                                                       | Default      |
| :--------------- | :------------------------------------------------------------------------------------------------------------------------------------------------ | :----------- |
| `S2_BOX`         | Search area coordinates: `West,South,East,North`. Supports single box, semicolon-separated list (`box1;box2`), or JSON list (`["box1", "box2"]`). | -            |
| `S2_STARTDATE`   | Earliest sensing date (YYYY-MM-DD). If omitted, resumes from the last run date in `s2_last.json`.                                                 | Yesterday    |
| `S2_MAXRECORDS`  | Maximum number of products to download per box                                                                                                    | `5`          |
| `S2_CLOUDCOVER`  | Maximum allowed cloud coverage percentage (0-100)                                                                                                 | `5`          |
| `S2_PRODUCTTYPE` | `L2A` (Bottom of Atmosphere) is recommended                                                                                                       | `L2A`        |
| `S2_SORTPARAM`   | CDSE sorting parameter (e.g., `startDate`)                                                                                                        | `startDate`  |
| `S2_SORTORDER`   | `descending` or `ascending`                                                                                                                       | `descending` |
| `S2_PROCESSES`   | `TCI, NIRFC, AP, NDVI, NDBI, NDBI_CLEAN, NDRE, NBR, CAMO, AIS, TCI-GF, NIRFC-GF, AP-GF`                                                           | (All)        |

### Sentinel-3 (SLSTR Fire Detection) Parameters

| Variable         | Description                                                                   | Default        |
| :--------------- | :---------------------------------------------------------------------------- | :------------- |
| `S3_BOX`         | Search area coordinates: `West,South,East,North` (multi-box via `;` or JSON). | -              |
| `S3_STARTDATE`   | Earliest sensing date (YYYY-MM-DD)                                            | Yesterday      |
| `S3_MAXRECORDS`  | Maximum products to download per box (final cap)                              | `5`            |
| `S3_PRODUCTTYPE` | `SL_1_RBT___` (SLSTR L1 radiances/BT), `SL_2_FRP___` (FRP), `OL_2_LFR___`     | `SL_1_RBT___` |
| `S3_SENSORMODE`  | `NT` (Non-Time Critical) or `NRT` (Near Real Time)                            | `NT`           |
| `S3_PROCESSES`   | `BT` (thermal composite), `FIRE` (hot-body anomaly)                           | `BT,FIRE`      |

> [!NOTE]
> The CDSE API returns all S3 product types mixed together (L2, OLCI, SR, etc.).
> The search fetches a larger sample (max 100 records) and filters for `SL_1_RBT___`
> + sensor mode by filename pattern, then trims to `S3_MAXRECORDS` per box.
> This ensures reliable RBT discovery even though ~85% of raw results are other
> product types.

### AIS Parameters

| Variable               | Description                                           | Default |
| :--------------------- | :---------------------------------------------------- | :------ |
| `AIS_RECORDER_URL`     | URL of the python-ais-recorder API                    | -       |
| `AIS_MAX_TIME_MINUTES` | Search window (+/- minutes) around sensing time       | `30`    |

### Guided Filter Parameters

Edge-preserving smoothing and detail enhancement for S2 visual products. Enabled per-product by adding `TCI-GF`, `NIRFC-GF`, or `AP-GF` to `S2_PROCESSES`.

| Variable                | Description                                                          | Default |
| :---------------------- | :------------------------------------------------------------------- | :------ |
| `GF_RADIUS`             | Local window radius in pixels (1–8; higher = stronger smoothing)     | `2`     |
| `GF_EPSILON`            | Edge-preservation threshold (higher = smoother, less edge detail)    | `0.01`  |
| `GF_GUIDANCE`           | Guidance image strategy: `B08` (NIR-guided) or `self` (self-guided)  | `B08`   |
| `GF_DETAIL_STRENGTH`    | Detail enhancement: `-1`=base only, `0`=unchanged, `1`=standard, `2`=aggressive | `2.0`   |
| `GF_AP_RADIUS`          | Window radius for AP-GF (defaults to `GF_RADIUS`)                    | `2`     |
| `GF_AP_EPSILON`         | Edge-preservation for AP-GF (lower = sharper SWIR)                  | `0.005` |
| `GF_AP_DETAIL_STRENGTH` | Detail enhancement for AP-GF                                         | `2.0`   |
| `GF_AP_W_B02`           | Blue weight in AP synthetic pan guide                                | `0.1`   |
| `GF_AP_W_B03`           | Green weight in AP synthetic pan guide                               | `0.1`   |
| `GF_AP_W_B04`           | Red weight in AP synthetic pan guide                                 | `0.4`   |
| `GF_AP_W_B08`           | NIR weight in AP synthetic pan guide (weights auto-normalise)        | `0.4`   |

TCI-GF uses B08 as guidance → applies B08 edge structure to the RGB composite. NIRFC-GF uses self-guidance → edge-preserving smoothing of the false-color composite. AP-GF builds a synthetic panchromatic guide `G = 0.1·B02+0.1·B03+0.4·B04+0.4·B08` to sharpen 20m SWIR bands (B11/B12) to 10m before forming the `AP` false-colour (`R=B12, G=B11, B=B08`). All use detail enhancement (subtract base, amplify residual, add back) to recover sharpness lost during smoothing.

### Fusion Parameters

| Variable           | Description                                 | Default |
| :----------------- | :------------------------------------------ | :------ |
| `FUSION_PROCESSES` | `RADAR-BURN, LIFE-MACHINE, TARGET-PROBE-V2` | (All)   |

## Usage

### 1. Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # And edit your CDSE credentials
```

### 2. Run

```bash
# Search, download, and process new data
python pipelines.py

python pipelines.py --downloaded # Process existing .SAFE folders without searching/downloading

# Run pipeline then shutdown the computer (after post-run hook and notifications)
python pipelines.py --shutdown

## Maintenance & Utilities

While the pipeline is highly automated, the following utility scripts are available for maintenance:

- **Cleanup**: `python cleanup.py --days 30 --force`  
  Removes products older than the specified number of days from `output/`, `temp/`, and the search logs. Defaults to 30 days and dry-run mode (remove `--force` to see what would be deleted).  
  The pipeline also runs an automatic **stale temp file scrub** at startup (`cleanup_temp_files()`) to remove intermediate files left by interrupted runs (`/tmp/vv.tif`, `*.grid.tif`, `*.tmp.tif` in output, etc.).
- **ROI Manager**: `python roi_manager.py --all`  
  Processes historical data to extract crops for all ROIs defined in `roi_config.json`. (Normally integrated into the main pipeline).
- **Overflight Predictor**: `python overflight_predictor.py`  
  Standalone script to check the next scheduled satellite passes. GeoJSON swath overlays are regenerated during inventory rebuild.
- **Metadata Rebuild**: `python rebuild_metadata.py`  
  Bulk regenerates all `.json` sidecar files for existing visual TIFFs. Useful if you've updated the metadata engine or manually moved files.
- **Inventory Rebuild**: `python inventory_manager.py`  
  Refreshes the global `inventory.json` used by the web viewer. (Automatically called by the main pipeline and other utilities).

## Viewer

The project includes a lightweight web viewer in the `viewer/` directory. It's designed to be served independently (e.g., via Nginx or `python -m http.server`) and reads the `output/` directory to display your products on an OpenLayers map.

- **Multi-language Support:** Choose between Finnish, Swedish, English, and German via the sidebar.
- **Smart Grouping:** Sort Sentinel-2 imagery by product type or by grid tile. Sentinel-3 thermal products are grouped separately.
- **Interactive Layers:** Hover to see footprints, click to jump to the sidebar entry.
- **Cloud Metadata:** Sentinel-2 products show the cloud coverage percentage (e.g., ☁️ 4.2%).
- **FIRE Opacity Slider:** When S3 FIRE layers are active, a transparency slider appears for blending thermal anomalies over basemap or S2 imagery. Value is persisted across sessions.
- **Advanced Tools:** Includes a fullscreen mode, a screenshot tool, and a BBOX coordinate widget for easier area definition.
- **Overflight HUD:** Displays the predicted time for the next Sentinel-1/2/3 passes at the bottom of the layer picker. Toggle buttons in the sidebar show GeoJSON swath overlays for the next overpass.

## Hardware Acceleration (GPU)

If you want to use your GPU:

1. Install [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) (if using Docker).
2. Ensure `cupy` is available in your environment.
3. The pipeline will detect it and switch to GPU kernels for index math.
4. Set `DISABLE_GPU=True` in `.env` if you need to force CPU mode.
if you need to force CPU mode.
