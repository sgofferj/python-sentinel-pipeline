# python-sentinel-pipeline

(C) 2025-2026 Stefan Gofferje

Licensed under the GNU General Public License V3 or later.

This is an automated pipeline designed to grab and process Sentinel-1 (Radar) and Sentinel-2 (Optical) imagery from the [ESA Copernicus Dataspace Ecosystem (CDSE)](https://dataspace.copernicus.eu/).

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
- **GPU Acceleration:** If `cupy` is installed and a CUDA-capable GPU is found, multispectral index math is automatically offloaded to the GPU.

### OSINT & Specialty Products

- **NDBI_CLEAN:** A vegetation-decoupled building index designed to spot infrastructure in dense environments.
- **CAMO:** Discovery composite for spotting anomalies in rural/forested terrain.
- **TARGET-PROBE-V2:** Advanced sensor fusion gating building signatures with radar returns.
- **LIFE-MACHINE:** Combined SAR/Optical discovery composite for distinguishing natural terrain from man-made structures.

## Configuration

Settings are handled via a `.env` file.

### CDSE Credentials

| Variable | Description |
| :--- | :--- |
| `COPERNICUS_USERNAME` | Your CDSE account email |
| `COPERNICUS_PASSWORD` | Your CDSE account password |

### Core Control

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PIPELINES` | `S1,S2,FUSION` (comma-separated list) | `S1,S2` |
| `USE_LOG` | Skip products already processed (uses `s1_last.json` / `s2_last.json`) | `True` |
| `TARGET_DIR` | Root directory for the `output/` folder | `.` |
| `CLEANUP_AFTER_RUN` | Automatically delete raw data after successful processing | `False` |
| `CLEANUP_DAYS` | Number of days to keep raw data | `30` |

### Performance & Hardware

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PIPELINE_WORKERS` | Concurrent threads for warping and index calculation | `2` |
| `MAX_PARALLEL_FINALIZERS` | Concurrent threads for COG and Sidecar generation | `2` |
| `DISABLE_GPU` | Force CPU mode even if CUDA/CuPy is available | `False` |
| `ENABLE_GPU_WARP` | Use experimental CUDA-accelerated warping for S1 | `False` |
| `GDAL_NUM_THREADS` | Number of threads for GDAL internal operations | `PIPELINE_WORKERS` |

### Sentinel-1 (Radar) Parameters

| Variable | Description | Default |
| :--- | :--- | :--- |
| `S1_BOX` | Search area coordinates: `East,South,West,North` | - |
| `S1_STARTDATE` | Earliest sensing date (YYYY-MM-DD) | Yesterday |
| `S1_MAXRECORDS` | Maximum number of products to download per box | `5` |
| `S1_PRODUCTTYPE` | `GRD` (Ground Range Detected) is standard | `GRD` |
| `S1_SENSORMODE` | `IW` (Interferometric Wide Swath) is standard | `IW` |
| `S1_SORTPARAM` | CDSE sorting parameter (e.g., `startDate`) | `startDate` |
| `S1_SORTORDER` | `descending` or `ascending` | `descending` |
| `S1_PROCESSES` | `VV, VH, RATIOVVVH` | `VV,VH,RATIOVVVH` |

### Sentinel-2 (Optical) Parameters

| Variable | Description | Default |
| :--- | :--- | :--- |
| `S2_BOX` | Search area coordinates: `East,South,West,North` | - |
| `S2_STARTDATE` | Earliest sensing date (YYYY-MM-DD) | Yesterday |
| `S2_MAXRECORDS` | Maximum number of products to download per box | `5` |
| `S2_CLOUDCOVER` | Maximum allowed cloud coverage percentage (0-100) | `5` |
| `S2_PRODUCTTYPE` | `L2A` (Bottom of Atmosphere) is recommended | `L2A` |
| `S2_SORTPARAM` | CDSE sorting parameter (e.g., `startDate`) | `startDate` |
| `S2_SORTORDER` | `descending` or `ascending` | `descending` |
| `S2_PROCESSES` | `TCI, NIRFC, AP, NDVI, NDBI, NDBI_CLEAN, NDRE, NBR, CAMO` | (All) |

### Fusion Parameters

| Variable | Description | Default |
| :--- | :--- | :--- |
| `FUSION_PROCESSES` | `RADAR-BURN, LIFE-MACHINE, TARGET-PROBE-V2` | (All) |

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

# Process existing .SAFE folders in temp/ without searching or downloading
python pipelines.py --downloaded
```

## Viewer

The project includes a lightweight web viewer in the `viewer/` directory. It's designed to be served independently (e.g., via Nginx or `python -m http.server`) and reads the `output/` directory to display your products on an OpenLayers map.

## Hardware Acceleration (GPU)

If you want to use your GPU:

1. Install [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) (if using Docker).
2. Ensure `cupy` is available in your environment.
3. The pipeline will detect it and switch to GPU kernels for index math.
4. Set `DISABLE_GPU=True` in `.env` if you need to force CPU mode.
