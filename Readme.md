# python-sentinel-pipeline

(C) 2025-2026 Stefan Gofferje

Licensed under the GNU General Public License V3 or later.

> [!CAUTION]
> This project is under heavy development and currently not ready to be used without Python knowledge.
> I will make this a docker container at a later point but for the moment the project should be
> considered more like a tech preview than a ready product.

> [!WARNING]
> Sentinel 2 datafiles are BIG. For Sentinel 2 processing you will need at least 16GB of RAM. Also make sure,
> you have ample disk space!

## Description

This project is a fully automatic, high-precision pipeline to download and process satellite imagery from the [ESA Copernicus Dataspace Ecosystem (CDSE)](https://dataspace.copernicus.eu/). It is designed for OSINT (Open Source Intelligence) purposes, providing physically consistent imagery for change detection and multi-sensor fusion.

## Features

### Radiometric Consistency
- **Absolute Physical Scaling:** Uses fixed physical units (dB for Sentinel-1, Reflectance for Sentinel-2) instead of dynamic stretching. This ensures that pixel values are consistent across different dates, a critical requirement for automated change detection.

### Sentinel 1 Pipeline (SAR)
- **Automatic Search & Download:** Supports GRD IW products.
- **Calibration & Denoising:** Custom `S1Calibrator` for radiometric calibration (Sigma0) and thermal noise removal.
- **Footprint Masking:** High-precision alpha masks derived from `manifest.safe`.
- **Products:**
  - **VV / VH:** Absolute dB scaled greyscale images (-30 to 0 dB).
  - **RATIOVVVH:** Pseudocolor composite (Red=VV, Green=VH, Blue=Ratio).
  - **NDPI:** Normalized Difference Polarization Index.

### Sentinel 2 Pipeline (Optical)
- **Level-2A Optimized:** Focuses on Bottom-of-Atmosphere (BOA) reflectance.
- **Winter/Snow Optimization:** Fixed physical clipping (0-0.3 reflectance) with Gamma 2.2 correction to preserve forest detail in high-latitude winter conditions.
- **OSINT Products (10m):**
  - **TCI / NIRFC / SWIR AP:** Standard and False Color composites.
  - **NDVI:** Normalized Difference Vegetation Index.
  - **NDRE (Red Edge):** High-sensitivity vegetation stress detection; peers through canopy to find "biological disturbances" (trails, hidden bases, camouflage).
  - **NDBI (Built-Up):** Highlights concrete, asphalt, and new infrastructure.
  - **NBR (Burn Ratio):** Identifies scorched earth or kinetic impacts.
  - **CAMO Composite:** Specialized Red(NDVI), Green(NDRE), Blue(TCI-Green) layer to reveal cut-foliage camouflage.

### Multi-Sensor Fusion
- **Spatial Matchmaker:** Automated logic to identify temporal (+/- 24h) and spatial overlaps between processed S1 and S2 products.
- **Surgical Fusion (Radar-over-Optical):** "Zero-Warp" high-precision burn-in of high-intensity S1 VH backscatter (>-10dB) into S2 TCI base. The output is surgically clipped to the exact overlap extent.
- **Life-vs-Machine:** Fuses S1-VH (Structure), S2-NDVI (Health), and S2-NDRE (Stress) into a single RGB product to distinguish human activity from natural forest.

## Configuration

The following values are supported in the `.env` file.

### General
| Variable  | Purpose |
| --------- | ------- |
| USE_LOG   | Enable search log to prevent re-downloading same products. |
| PIPELINES | Which pipelines to run ("S1,S2"). |

### Sentinel 1 / 2
Standard CDSE search parameters are supported: `_BOX`, `_STARTDATE`, `_MAXRECORDS`, `_PRODUCTTYPE`, `_SORTPARAM`, etc.

## Usage

1. Create a virtual environment: `python -m venv venv`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure `.env` with your CDSE credentials.
4. Run the pipeline: `python pipelines.py`

Fused products and OSINT indices will be generated automatically in the `output/` directory if overlaps are found.
