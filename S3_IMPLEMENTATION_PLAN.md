# Sentinel-3 Implementation Plan — Fire Detection & OSINT BDA

## 1. Objective

Add Sentinel-3 (SLSTR + OLCI) support to the pipeline, focused on:
- **Active fire detection** and fire radiative power (FRP) mapping via SLSTR thermal bands
- **Burn scar mapping** via OLCI/SLSTR multi-spectral indices
- **OSINT Battle Damage Assessment** via thermal anomaly overlays on high-res S2/S1 imagery
- **Daily revisit** capability (S3 covers the same area daily vs S2's ~5 days)

---

## 2. Architecture Overview

### 2.1 What Already Exists

- `copernicus/_class.py:123` — collection mapping `"Sentinel3": "SENTINEL-3"` is already in place
- OData search infrastructure supports `sensorMode` (used for S1 IW/NRT) and `productType` filters — reusable for S3

### 2.2 New / Modified Files

| File | Action | Purpose |
|------|--------|---------|
| `.env.example` | Modify | Add `S3_BOX`, `S3_PRODUCTTYPE`, `S3_PROCESSES`, `S3_SENSORMODE`, `S3_FORECAST_PASSES` |
| `constants.py` | Modify | Add `DIRS` entries for S3 visual/analytic, band index constants, rendering constraints |
| `search.py` | Modify | Add `search_s3()` function |
| `pipelines.py` | Modify | Add `S3` pipeline block (search → download → process) |
| `functions_s3.py` | **New** | Core S3 processing pipeline (modeled after `functions_s2.py`) |
| `correlate.py` | Modify | Add S3+S1/S2 fusion products (thermal overlays) |
| `metadata_engine.py` | Modify | Add S3 entries to `RES_MAP` |
| `overflight_predictor.py` | Modify | Add S3 NORAD IDs, swath width, and satellite group |
| `legends.py` | Modify | Add colour ramps for thermal/fire products |
| `cleanup.py` | Modify | Add S3 filename pattern |

---

## 3. Phase 1 — Foundation: Search, Download, Basic Rendering

### 3.1 CDSE Search Parameters

**SLSTR Level-1 Radiances and Brightness Temperatures (primary fire source):**
```
Collection:    SENTINEL-3
ProductType:   SL_1_RBT___
Timeliness:    NT (Non-Time Critical) or NRT (Near Real Time)
Resolution:    500m (S1-S6), 1km (S7-S9, F1, F2)
Swath:         1400 km nadir
```

**SLSTR Level-2 Fire Radiative Power (pre-processed active fire):**
```
Collection:    SENTINEL-3
ProductType:   SL_2_FRP___
Timeliness:    NT (ESA NTC products preferred)
Content:       FRP_in.nc (MWIR thermal fire detections)
               FRP_an.nc / FRP_bn.nc (SWIR night-time detections)
```

**OLCI Level-2 Land Full Resolution (vegetation/burn scar context):**
```
Collection:    SENTINEL-3
ProductType:   OL_2_LFR___
Timeliness:    NT
Resolution:    300m
Bands:         21 spectral bands (0.4–1.02 µm)
```

### 3.2 Search Function (`search.py`)

Model after `search_s2()`. Add `search_s3(boxes, product_type=None)` that:
- Reads `S3_PRODUCTTYPE` from env (default `SL_1_RBT___`)
- Reads `S3_SENSORMODE` for timeliness filter (`NT` or `NRT`)
- Reads `S3_MAXRECORDS` from env
- Queries `"Sentinel3"` collection with `productType` and optional `timeliness` filter
- Returns same `(num_files, results_per_box)` pattern

The S3 OData filter is:
```
Collection/Name eq 'SENTINEL-3'
and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/Value eq 'SL_1_RBT___')
and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'timeliness' and att/Value eq 'NT')
and ContentDate/Start gt {start_date}
and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')

For FRP products: productType eq 'SL_2_FRP___'
For OLCI products: productType eq 'OL_2_LFR___'
```

### 3.3 Product Naming Convention

S3 products follow the pattern:
```
S3{A/B}_SL_1_RBT____{datestart}_{dateend}_{...}.SEN3
S3{A/B}_OL_2_LFR____{datestart}_{dateend}_{...}.SEN3
```

### 3.4 GDAL Access

SLSTR products open via GDAL's SENTINEL3 driver. Subdataset naming:
```
S3_{instrument}_SL_1_RBT____{hash}.SEN3:
  SUBDATASET_1_NAME=...//S3_SL_1_RBT____{hash}.SEN3/radiance_{view}_{band}
  SUBDATASET_2_NAME=...//S3_SL_1_RBT____{hash}.SEN3/...quality...
```

For SLSTR, key GDAL subdatasets:
- `radiance_nadir_{s1..s6}` — VNIR/SWIR reflectance bands
- `BT_nadir_{s7,s8,s9}` — Thermal IR brightness temperatures (K)
- `BT_nadir_{f1,f2}` — Fire bands (high dynamic range, same λ as S7/S8)

OLCI opens as:
- `Oa01_radiance` through `Oa21_radiance` — 21 spectral bands

### 3.5 Directory Structure (`constants.py`)

Add `DIRS` entries:
```python
"DL": ...                           # shared temp (same as S1/S2)
"VIS_S3_RBT": .../visual/s3/rbt    # SLSTR brightness temp composite
"VIS_S3_FRP": .../visual/s3/frp    # FRP active fire overlay
"VIS_S3_TCI": .../visual/s3/tci    # OLCI true colour
"VIS_S3_NDVI": .../visual/s3/ndvi  # OLCI NDVI
"VIS_S3_NBR": .../visual/s3/nbr    # OLCI+SLSTR NBR
"ANA_S3_BT": .../analytic/s3/bt    # analytic brightness temp
"ANA_S3_NDVI": .../analytic/s3/ndvi
"ANA_S3_FRP": .../analytic/s3/frp  # FRP values
```

### 3.6 Constants for S3 Band Indices

```python
# SLSTR 500m band order (nadir view)
S3_BAND_S1: int = 1   # 0.555 µm
S3_BAND_S2: int = 2   # 0.659 µm
S3_BAND_S3: int = 3   # 0.865 µm
S3_BAND_S4: int = 4   # 1.375 µm
S3_BAND_S5: int = 5   # 1.610 µm
S3_BAND_S6: int = 6   # 2.255 µm

# SLSTR 1km band order
S3_BAND_S7: int = 1   # 3.74 µm (MIR, fire)
S3_BAND_S8: int = 2   # 10.85 µm (TIR, fire)
S3_BAND_S9: int = 3   # 12.0 µm (TIR)
S3_BAND_F1: int = 4   # 3.74 µm (fire band, high range)
S3_BAND_F2: int = 5   # 10.85 µm (fire band, high range)
```

### 3.7 Rendering Constraints

```python
S3_BT_MIN: float = 250.0    # Kelvin
S3_BT_MAX: float = 350.0    # Kelvin
S3_FRP_MIN: float = 0.0     # MW
S3_FRP_MAX: float = 5000.0  # MW
S3_REF_MIN: int = 0
S3_REF_MAX: int = 4000
```

### 3.8 Base Pipeline (`functions_s3.py`)

Create `functions_s3.py` with `run_pipeline(ds_obj, processes, fusion_processes)`:

**`prepare(ds_obj)` — Warp SLSTR/OLCI to EPSG:3857:**
- SLSTR: warp S7, S8, S9 (BT) + S5, S6 (SWIR) + S1-S3 (VIS/NIR) to common grid
  - 1km thermal bands warped to 500m to match native VIS/SWIR grid
  - Resampling: bilinear for continuous data (BT), nearest for quality flags
- OLCI: warp 300m bands to 300m or resample to matched SLSTR grid
- Output: separate GeoTIFFs for each warped band in a staging directory

**`_render_internal(processes, v_paths, a_paths, ...)` — Single-pass render:**
- Model after `functions_s2.py`'s block renderer
- Read warped bands, compute requested products per block
- Write 8-bit visual + float32 analytic outputs

**S3_PROCESSES product options:**

| Process Code | Description | Input Bands | Type |
|:---|:---|:---|:---|
| `TCI` | OLCI true colour (Oa08/Oa06/Oa04) | OLCI 300m | Visual |
| `BT` | SLSTR brightness temp composite (S8=red, S7=green, S9=blue) | S7, S8, S9 | Visual |
| `BT_HOT` | Hotspot emphasis (S7/S9 ratio) | S7, S9 | Visual |
| `NDVI` | Normalized Difference Vegetation Index | S3, S1 (or OLCI) | Both |
| `NBR` | Normalized Burn Ratio (using SLSTR S3+S6) | S3, S6 | Both |
| `dNBR` | Differenced NBR (multi-temporal, Phase 2) | S3, S6 | Both |
| `FRP` | Level-2 FRP overlay (requires SL_2_FRP___) | FRP_in.nc | Both |
| `FIRE` | Thermal anomaly classification (custom) | S7, S8 | Visual |

---

## 4. Phase 2 — Fire Detection Products

### 4.1 SLSTR Thermal Composite (`BT`)

False-colour RGB compositing of SLSTR thermal bands:
- **Red**: S8 (10.85 µm) — TIR, highlights hot surfaces
- **Green**: S7 (3.74 µm) — MIR, very sensitive to sub-pixel hotspots
- **Blue**: S9 (12.0 µm) — TIR, cloud discrimination

High brightness temperatures appear yellow/white (active fires).
Cold clouds appear blue/cyan.
Bare ground appears in mid-tones.

Stretch: 250–350 K range mapped to 0–255.

### 4.2 Hotspot Emphasis (`BT_HOT`)

Single-band ratio emphasizing sub-pixel hotspots:
```
BT_HOT = (S7 - S9) / (S7 + S9)
```
Active fires produce very high S7 (3.74 µm) relative to S9 (12 µm), creating strong positive values. Render with a hot-body colour ramp (black → red → yellow → white).

### 4.3 Level-2 FRP Overlay (`FRP`)

Process `SL_2_FRP___` Level-2 products when available:
- Parse NetCDF4 `FRP_in.nc` for fire detections
- Extract per-fire attributes: lat, lon, FRP (MW), confidence, classification
- Render as point overlay (circle markers sized by FRP, coloured by confidence)
- GeoJSON output that can be overlaid on any base map

Key FRP attributes from the Level-2 product:
- `latitude`, `longitude` — fire location
- `frp_mw` — fire radiative power in megawatts
- `confidence` — detection confidence flag
- `classification` — hotspot type flag
- `s5_confirm` — SWIR S5 confirmation flag
- `ratio_s56` — S5/S6 ratio (>0.9 indicates gas flare vs vegetation fire)

### 4.4 Custom Thermal Anomaly Detection (`FIRE`)

Pixel-level thermal anomaly detection for areas where SL_2_FRP is not available (NRT data):
- Compute background temperature window (10×10 km kernel, excluding potential fire pixels)
- Flag pixels where S7 BT exceeds background by >5 K (daytime) / >3 K (night-time)
- Apply S8 BT > 300 K threshold to filter false alarms (reflective hot surfaces)
- Classify:
  - **Low confidence**: S7 BT > background + 5K
  - **Medium confidence**: S7 BT > background + 10K, S8 BT > 310K
  - **High confidence (active fire)**: S7 BT > background + 15K, S8 BT > 320K
- Render as classified overlay with confidence-based colour coding

### 4.5 Burn Scar Mapping (`NBR`, `dNBR`)

Using SLSTR bands S3 (0.865 µm, NIR) and S6 (2.255 µm, SWIR):
```
NBR = (S3 - S6) / (S3 + S6)
```

dNBR (differenced NBR) for multi-temporal burn severity:
```
dNBR = NBR_pre_fire - NBR_post_fire
```
- Requires at least two acquisitions (before and after the fire event)
- dNBR thresholds (standard USGS classification):
  - < 0.1: Unburned
  - 0.1–0.27: Low severity
  - 0.27–0.44: Moderate-low severity
  - 0.44–0.66: Moderate-high severity
  - > 0.66: High severity

Alternative using OLCI bands (Oa08 665nm, Oa17 865nm) for higher resolution (300m):
```
NDVI = (Oa17 - Oa08) / (Oa17 + Oa08)
```

---

## 5. Phase 3 — OSINT BDA Fusion Products

### 5.1 Thermal Overlay on S2 Optical (`THERMAL-BURN`)

Model after `RADAR-BURN` in `correlate.py`:
- SLSTR thermal anomaly raster (500m native) warped to match S2 10m grid
- High-contrast ghost blend: thermal hotspots rendered in red/yellow with 50% transparency over S2-TCI
- Allows spotting active fires, industrial activity, and heat signatures at S2 resolution

**Detection overlap logic** (in `find_overlaps()`):
- S3 acquisition within ±6 hours of S2 acquisition
- S3 footprint intersects S2 footprint
- Thermal anomalies fall within the S2 tile bounds

### 5.2 S3+SAR Fusion

- SLSTR thermal anomalies overlaid on S1-VH backscatter (all-weather, day/night)
- Useful for monitoring persistent thermal activity (industrial sites, gas flares) regardless of cloud cover
- SAR backscatter can reveal structural changes (collapsed buildings, vehicle movements) while thermal reveals heat sources

### 5.3 Multi-Temporal Thermal Change

- Stack SLSTR thermal acquisitions over N days
- Compute ΔBT from running background (7-day median)
- Flag areas with BT > background + 3σ as "new thermal activity"
- Useful for detecting:
  - New fires
  - Industrial activity changes
  - Post-strike fires in BDA scenarios

### 5.4 BDA-Specific Products

| Product | Inputs | Purpose |
|:---|:---|:---|
| `BDA-THERMAL` | S3 BT_HOT + S2 TCI | Overlay thermal anomalies on high-res optical |
| `BDA-PERSISTENT` | S3 multi-temporal BT stack | 7-day persistent thermal activity mask |
| `BDA-SMOKE` | OLCI true colour | Smoke plume visualisation and tracking |
| `BDA-FIRE-PERIMETER` | S3 FRP + S2 NBR | Active fire front + burned area boundary |

---

## 6. Phase 4 — Infrastructure Integration

### 6.1 Pipeline Orchestrator (`pipelines.py`)

Add `"S3"` to `PIPELINES` env var. New block after S2 processing:

```python
processed_s3 = []
if "S3" in PIPELINES_LIST and s3_ready:
    for feat in s3_ready:
        filename = feat["properties"]["title"]
        # SLSTR L1 products open via GDAL's SENTINEL3 driver
        manifest = os.path.join(c.DIRS["DL"], filename, "xfdumanifest.xml")
        if os.path.exists(manifest):
            ds_obj = gdal.Open(manifest)
            s3.run_pipeline(ds_obj, S3_PROCESSES, FUSION_PROCESSES)
            processed_s3.append(feat)

if processed_s3 and not args.downloaded:
    search.update_last_run("s3", processed_s3)
```

### 6.2 Env Variables (`.env.example`)

```ini
# ----- Sentinel-3 (Fire Detection) Parameters
S3_BOX = "24.46,60.08,25.45,60.34"                 # Search area: West, South, East, North
S3_FORECAST_PASSES = 1                              # Number of orbital passes to forecast
S3_PRODUCTTYPE = "SL_1_RBT___"                     # SLSTR L1 radiances/BT (primary)
S3_SENSORMODE = "NT"                                # Timeliness: NT (NTC) or NRT
S3_MAXRECORDS = 5                                   # Max products to download per box
S3_PROCESSES = "BT,BT_HOT,FIRE,NDVI,NBR"           # Output products
# S3 Options:
# BT: Brightness temperature false-colour composite (S8/S7/S9)
# BT_HOT: Hotspot emphasis ratio
# FIRE: Custom thermal anomaly classification
# FRP: Level-2 FRP overlay (requires SL_2_FRP___ search)
# TCI: OLCI true colour (requires OL_2_LFR___)
# NDVI: Vegetation index (from OLCI or SLSTR)
# NBR: Burn ratio for scar mapping
# dNBR: Differenced NBR (multi-temporal)

# Fusion Options (requires matching S1/S2/S3 acquisitions):
# THERMAL-BURN: S3 thermal anomaly over S2-TCI
# BDA-PERSISTENT: 7-day persistent thermal activity
```

### 6.3 Overflight Prediction

Add S3 to `overflight_predictor.py`:

```python
SENTINELS = {
    "S1": {...},
    "S2": {...},
    "S3": {
        "Sentinel-3A": 43013,  # NORAD ID for S3A
        "Sentinel-3B": 43487,  # NORAD ID for S3B
    },
}

SWATH_WIDTH_M = {"S1": 250_000, "S2": 290_000, "S3": 1_400_000}
```

Env vars: `S3_FORECAST_PASSES = 1`

### 6.4 Resolution Map (`metadata_engine.py`)

```python
"S3-RBT": 500.0,
"S3-BT": 500.0,
"S3-BT_HOT": 500.0,
"S3-FIRE": 1000.0,
"S3-FRP": 1000.0,
"S3-TCI": 300.0,
"S3-NDVI": 300.0,
"S3-NBR": 500.0,
"S3-dNBR": 500.0,
"FUSED-THERMAL-BURN": 10.0,
"FUSED-BDA-PERSISTENT": 10.0,
```

### 6.5 ROI Manager

Add S3 product name patterns to `PRODUCT_NAMES` mapping for crop generation.

### 6.6 Cleanup

Add `S3` prefix to filename parsing in `cleanup.py` for automatic cleanup.

---

## 7. Implementation Order

| Step | Description | Effort | Dependencies |
|:---|:---|:---|:---|
| **1** | Add `search_s3()` to `search.py` | Small | None |
| **2** | Add `DIRS`, band constants, rendering constraints to `constants.py` | Small | None |
| **3** | Create `functions_s3.py` with `prepare()` (warp SLSTR/OLCI bands) | Medium | NetCDF4, GDAL SENTINEL3 driver |
| **4** | Implement BT composite + BT_HOT in `_render_internal()` | Medium | Step 3 |
| **5** | Implement OLCI TCI + NDVI in `_render_internal()` | Medium | Step 3 |
| **6** | Wire up `pipelines.py` orchestrator | Small | Steps 1–5 |
| **7** | Implement custom thermal anomaly detection (`FIRE`) | Medium | Step 4 |
| **8** | Implement NBR/dNBR burn scar mapping | Medium | Step 5 |
| **9** | Add FRP Level-2 overlay parsing (NetCDF4 FRP_in.nc) | Medium | None |
| **10** | Implement `THERMAL-BURN` fusion in `correlate.py` | Medium | Step 7 + S2 pipeline |
| **11** | Implement multi-temporal thermal change stack | Large | Step 7 |
| **12** | Add S3 to `overflight_predictor.py` | Small | None |
| **13** | Add S3 to `metadata_engine.py`, `legends.py`, `cleanup.py` | Small | None |
| **14** | Add S3 legend definitions to `legends.py` | Small | None |
| **15** | Update `.env.example`, `Readme.md`, `CHANGELOG.md` | Small | All above |

### Quick Win Path (Steps 1–6)

Steps 1–6 get SLSTR BT composites and OLCI TCI/NDVI rendering working end-to-end. This is the minimum viable integration and should be done first. Fire detection (steps 7–11) builds on this foundation.

---

## 8. Technical Notes

### 8.1 GDAL SENTINEL3 Driver

The SENTINEL3 GDAL driver (built into GDAL ≥ 3.0) opens `xfdumanifest.xml` and exposes subdatasets. Available bands depend on the specific product type. For SL_1_RBT___:
- 500m bands (S1–S6) in `radiance_nadir_*.tif` and `radiance_oblique_*.tif`
- 1km bands (S7–S9, F1, F2) in `BT_nadir_*.tif` and `BT_oblique_*.tif`

The driver does NOT provide easy band index mapping — need to enumerate subdatasets:
```python
ds = gdal.Open("S3A_SL_1_RBT____2025...SEN3/xfdumanifest.xml")
subdatasets = ds.GetSubDatasets()
for name, desc in subdatasets:
    print(name, desc)
```

### 8.2 NetCDF4 for FRP

`SL_2_FRP___` products use NetCDF4 format, not directly openable via GDAL. Use `netCDF4` or `xarray` library:
```python
import netCDF4
nc = netCDF4.Dataset("FRP_in.nc")
fires = {
    "lat": nc.variables["latitude"][:],
    "lon": nc.variables["longitude"][:],
    "frp": nc.variables["frp_mw"][:],
    "confidence": nc.variables["confidence"][:],
}
```

### 8.3 Dual-View Processing

SLSTR has both nadir and oblique views. For land-based fire detection, the nadir view is primary. The oblique view provides additional atmospheric path length useful for aerosol correction but can be ignored in initial implementation.

### 8.4 NRT vs NTC Timeliness

- **NT** (Non-Time Critical): Products available ~48h after acquisition, ESA-processed, higher quality. Preferred for historical fire analysis and burn scar mapping.
- **NRT** (Near Real Time): Products available ~3h after acquisition, EUMETSAT-processed. Preferred for active fire monitoring and BDA use cases.
- Recommend defaulting to `NT` with `NRT` override option for time-sensitive BDA applications.

### 8.5 Daily Revisit Advantage

S3A + S3B together provide daily global coverage at the equator (vs S2's ~5 days). This enables:
- Daily fire progression tracking (not possible with S2 alone)
- Thermal persistence analysis (is a hotspot still active?)
- Cloud gap-filling: if S2 is cloudy, S3 SLSTR's thermal bands see through thin cloud

### 8.6 Data Volume Considerations

- SL_1_RBT___: ~800 MB per 3-minute granule
- OL_2_LFR___: ~600 MB per scene
- Daily coverage produces many granules for a given area of interest
- Recommend keeping `S3_MAXRECORDS` conservative (default 3–5)
- 1km thermal data is lightweight compared to S2 10m — processing is fast

---

## 9. Open Questions / Future Work

1. **SLSTR-only fire detection vs Level-2 FRP**: Should the pipeline rely on the ESA Level-2 FRP product (pre-processed, authoritative) or implement custom detection from L1 RBT? Best approach: support both — FRP when available, custom FIRE detection as fallback.

2. **OLCI vs SLSTR for NDVI/NBR**: SLSTR S1–S6 bands provide 500m NDVI and NBR; OLCI provides 300m multispectral with higher spectral resolution. Recommend OLCI for NDVI (better vegetation monitoring) and SLSTR for NBR (SLSTR S6 at 2.25 µm is better for burn scar detection than OLCI's maximum 1.02 µm).

3. **Thermal band saturation**: SLSTR F1/F2 bands exist specifically because S7/S8 saturate over high-temperature fires (>350 K for S7). The pipeline should detect saturation and fall back to F1/F2 radiometry when available.

4. **Cloud masking**: Thermal bands partially penetrate thin cloud, but thick cloud blocks all bands. Consider implementing a cloud mask from S4 (1.375 µm cirrus band) or S9 (12 µm) for quality filtering.
