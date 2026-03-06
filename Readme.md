# python-sentinel-pipeline

(C) 2025-2026 Stefan Gofferje

Licensed under the GNU General Public License V3 or later.

> [!CAUTION]
> This project is under heavy development. While functional, it is currently in a "tech preview" phase.

> [!WARNING]
> Sentinel 2 datafiles are BIG. You will need at least 16GB of RAM. High-performance SSD storage is strongly recommended for temporary processing.

## Description

This project is a fully automatic, high-precision pipeline to download and process satellite imagery from the [ESA Copernicus Dataspace Ecosystem (CDSE)](https://dataspace.copernicus.eu/). It is designed for OSINT (Open Source Intelligence) purposes, providing physically consistent imagery for change detection and multi-sensor fusion.

## Features

### Dual-Output Architecture
The pipeline now produces two distinct classes of output:
- **Visual (RGBA):** 8-bit Cloud Optimized GeoTIFFs (COGs) with high-contrast non-linear stretching, optimized for instant web display (Leaflet/Cesium). Includes automated HTML/CSS legends and `.json` metadata sidecars.
- **Analytic (Float32):** Single-band high-fidelity rasters preserving absolute physical units (dB/Reflectance). These are the source of truth for automated change detection and statistical analysis.

### Performance & Scaling
- **Single-Pass Rendering:** S1 and S2 products are rendered in a single windowed loop to minimize Disk I/O.
- **Multi-threaded Calibration:** S1 radiometric calibration uses a high-concurrency `ThreadPoolExecutor`.
- **COG Integration:** Automatic conversion to COG format using `gdaladdo` and `gdal_translate` with multi-threaded DEFLATE compression.
- **Performance Tracking:** Integrated `PerformanceLogger` tracks execution time, peak RSS memory, and recursive child-process CPU usage.

### OSINT Specializations
- **Target Probe V2:** Advanced sensor fusion using NDBI_CLEAN (Vegetation-decoupled building index) gated by S1-VH metallic signatures.
- **Life-vs-Machine:** Combined SAR/Optical discovery composite.
- **Urban Heat Map:** High-contrast pseudo-coloring for infrastructure detection in snow/winter conditions.

## Configuration

The following values are supported in the `.env` file.

| Variable | Purpose | Default |
| :--- | :--- | :--- |
| `PIPELINES` | Which pipelines to run ("S1,S2"). | "S1,S2" |
| `PIPELINE_WORKERS` | Number of concurrent threads/workers for math and I/O. | 2 |
| `COPERNICUS_USERNAME` | CDSE Account Email. | - |
| `COPERNICUS_PASSWORD` | CDSE Account Password. | - |

## Deployment (Docker)

The project is designed to run as a scheduled container.

### Volume Strategy
- `/app/temp`: **Bind mount** to high-speed scratch space (SSD). Handles multi-GB raw downloads and intermediate warps.
- `/app/output`: **Bind mount** for persistent storage of visual and analytic products.

### Hardware Acceleration (GPU)
For future CUDA/OpenCL acceleration, the container requires the following runtime privileges:
- **NVIDIA:** Use `--gpus all` and ensure the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) is installed on the host.
- **OpenCL:** Mount the host DRI devices (e.g., `--device /dev/dri:/dev/dri`).

### Scheduling
The internal Cron daemon (default `04:00 UTC`) can be overridden via the `CRON_SCHEDULE` environment variable.

## Usage (Manual)

1. Create a virtual environment: `python -m venv venv`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure `.env` with your CDSE credentials.
4. Run the pipeline: `python pipelines.py`
