# Changelog

All notable changes since 01MAR2026.

---

## 2026-07-07 — Per-Satellite Cleanup TTL, --shutdown Flag, CLEANUP_RAW Rework & S2 Versions

### Added
- **Per-satellite cleanup days** (`pipelines.py`, `cleanup.py`): `CLEANUP_S1_DAYS`, `CLEANUP_S2_DAYS`, `CLEANUP_S3_DAYS` env vars allow independent retention periods per sensor. `CLEANUP_FUSION_DAYS` controls fusion products (RADAR-BURN, LIFE-MACHINE, TARGET-PROBE-V2). All fall back to `CLEANUP_DAYS` when unset.
- **`--shutdown` CLI flag** (`pipelines.py`): Shuts down the computer after the pipeline, post-run hook, and notifications complete. Uses KDE D-Bus, `systemctl poweroff`, or `shutdown -h now` as fallback.
- **`CLEANUP_S2_VERSIONS`** (`pipelines.py`, `cleanup.py`): Per-tile version retention for Sentinel-2. Groups products by UTM grid tile (e.g. T35VPK) and keeps only the N most recent per tile, discarding older ones regardless of age. Overrides day-based S2 cleanup when set. Implemented via `find_s2_excess_versions()`.

### Changed
- **`CLEANUP_RAW` now removes ALL source directories** (`pipelines.py`, `cleanup.py`): No longer filters by just-processed products. New `cleanup_all_source_data()` removes every `.SAFE`/`.SEN3` directory in `temp/` unconditionally — ensures stale source dirs from previous failed runs are cleaned up too.

---

## 2026-06-25 — Skip Empty FIRE Files

### Changed
- **FIRE files not saved when no fire detected** (`functions_s3.py`): `_render_internal()` tracks `fire_detected` during block rendering. If no alpha > 0 after cold-cloud masking, the FIRE `.tif` is deleted before COG/sidecar finalization — saves disk space for cold/cloudy/nighttime scenes where the overlay would be fully transparent.
- **Stale sidecar-patching removed** (`functions_s3.py`): The post-finalization block that patched FIRE sidecars with BT's footprint was removed. Since empty FIRE files are now deleted entirely, no patching is needed.
- **ROI FIRE crops deleted on no-anomaly** (`roi_manager.py`): FIRE ROI crops are removed when `thermal_monitor` was enabled and confirmed no thermal anomaly, gated on `thermal_checked` to preserve crops for ROIs without monitoring.
- **Fixed pre-existing bug** (`roi_manager.py`): `apprise_url` was referenced before assignment in `check_thermal_anomaly()` call (line 723), causing `UnboundLocalError` when thermal monitoring was enabled.

## 2026-06-14 — S3 Swath-Edge Clipping, Cloud Mask & Clean Contours

### Changed
- **Swath-edge clipping**: `_build_kdtree_mapping_3857()` now passes `distance_upper_bound=0.03°` (~3 km) to KD-tree query. Pixels beyond the curved swath boundary are marked invalid → set to 0 K (black in BT, transparent in FIRE). Prevents the previous straight-edge smearing from pixels pulled from the wrong tie-point.
- **BT visual fully opaque**: BT alpha forced to swath mask (255 inside, 0 outside) — removes the colormap's value-based transparency that made cold-water pixels see-through.
- **FIRE visual contour**: When no fires detected, FIRE sidecar footprint is patched from the BT sibling's swath contour (was falling back to bounding-box rectangle).
- **FIRE cloud mask**: S8 (10.85µm) cold-cloud filter (`S3_CLOUD_TEMP_THRESHOLD=275K`): pixels where S8 < 275K are set alpha=0 in FIRE, suppressing false fire detections from solar reflection in S7 (3.74µm) off cirrus/cloud tops. No S8 threshold change needed — real fires at 600-1200K are unaffected.
- **Sidecar contour quality**: Downsampling factor now resolution-aware (`factor = max(1, min(10, eff_res/500))` → ~2 for S3, ~1 for S2/S1). Simplify tolerance scaled to `mask_pixel * 3.0` → removes stair-step aliasing from raster-to-vector conversion. S3 contours reduced from 845 stair-stepped vertices to 54 clean vertices.
- **Overpass text removed** from sidebar (`#next-overflight` + `updateNextOverflight()`). Graphical GeoJSON swath overlays now carry the information.
- **Layer group order**: Sidebar groups reordered to S2 (optical) → S1 (SAR) → S3 (thermal) → FUSED → ROI.
- **cleanup.py**: `cleanup_source_data()` now also searches for `.SEN3` directories alongside `.SAFE`.
- `metadata_engine.py::generate_sidecar()`: resolution-aware downsampling factor and simplify tolerance; added `buffer(0)` + `fill_holes()` after simplify.
- `constants.py`: Added `S3_CLOUD_TEMP_THRESHOLD = 275.0`.

### Fixed
- `_bt_colormap()` NaN handling: wrapped interpolation input with `np.nan_to_num(data, nan=0.0)` — was causing NaN propagation through the pipeline.
- Alpha reshape in `_bt_colormap()`: colormap's alpha was 1D while R/G/B were 2D, causing `np.stack` shape mismatch (`ValueError: all input arrays must have the same shape`).
- `_fire_colormap()`: also wrapped with `np.nan_to_num()` for NaN safety.

## 2026-06-14 — S3 Per-Pixel KD-Tree Geolocation (Fixes ~250 km Warp Error)

### Changed
- **S3 geolocation completely rewritten** in `functions_s3.py`:
  - Replaced GCP+TPS gdal.Warp (~250 km center error, visible as shifted fjords) with per-pixel KD-tree warp from tie-point arrays
  - New `_build_kdtree_mapping_3857()` builds a cKDTree from (lon, lat) → (px, py), creates a regular output grid in EPSG:3857 at 1000m, batch-converts each pixel center to (lon, lat) via GDAL `TransformPoints`, and queries the KD-tree for the nearest input pixel — **0m geolocation error at all tie-point locations**, 0.00K BT difference
  - Removed unused `_s3_geodetic_paths()` and old `_s3_warp_band()` with VRT/GCP/TPS logic
  - `prepare()` now calls `_s3_warp_bands_kdtree()` once with all 3 bands, producing a single multi-band GeoTIFF directly in EPSG:3857 (no intermediate 4326→3857 warp cascade)
  - `cleanup()` simplified to match new single-output approach
  - Runtime: ~70s per 3-band full-swath scene at 1000m resolution

### Fixed
- **S3 geolocation accuracy**: GCP+TPS warp had ~250 km center error due to SLSTR swath curvature exceeding polynomial/TPS fitting capability. KD-tree per-pixel lookup from tie-point arrays gives 0m error at all 1.8M tie-point locations with pixel-perfect BT values.

## 2026-06-14 — S3 Search Fetch-Limit & Finland Box

### Changed
- **S3 search logic** in `search.py::search_s3()`: API is called with a fetch limit of `max(100, max_records * 10)` to get enough raw results through the mixed-product-type noise, then filtered results are trimmed to `S3_MAXRECORDS` per box. Previously used `S3_MAXRECORDS` directly as the API fetch limit, which often returned 0 RBT products since ~85% of raw CDSE results are non-RBT types (L2, OLCI, SR).
- `README.md`: Added note explaining the fetch-limit + trim logic for S3 search.

### Added
- Added a Finland-wide search box (`BOX_Finland`) in `.env`; `S3_BOX` now includes it in addition to existing boxes.

## 2026-06-14 — Thermal Monitoring & Viewer Orbit Filters

### Added
- **Thermal anomaly monitoring** in `roi_manager.py`: `check_thermal_anomaly()` crops analytic S3 BT Float32 to ROI, reads max BT, sends Apprise alert if threshold exceeded. Enabled per-ROI via `thermal_monitor: true` / `thermal_threshold` (default 310K).
- **Orbit direction (night-pass) filter buttons** in viewer: `[A] [↑] [↓]` per satellite group (S1/S2/S3/FUSED/ROI), filters visible products in layer picker. State persisted to localStorage.
- **S3 orbit_direction** support in `functions_s3.py`: reads `PASS_DIRECTION` from SENTINEL3 GDAL metadata, written as TIFF tag and sidecar field.
- **S1 (RADAR) opacity slider** in viewer: parallels the FIRE slider for S1 radar layers, cyan accent, localStorage persistence.

### Changed
- **Z-index grouping** in viewer: products now stacked by constellation (S2=bottom, S1=middle, S3=top) with newest-first within each group, rather than interleaved by timestamp.
- `README.md`: Added S3 env var table, SLSTR fire detection description, viewer FIRE/RADAR opacity slider docs.

## 2026-06-14 — Sentinel-3 SLSTR Fire Detection

### Added
- **Sentinel-3 pipeline** (`functions_s3.py`): SLSTR thermal band processing engine
  - `prepare()`: Enumerates GDAL SENTINEL3 driver subdatasets, warps BT_nadir bands (S7/S8/S9/F1/F2) to EPSG:3857 at 1km resolution
  - `_render_internal()`: Single-pass block renderer producing BT composite and FIRE detection products with COG finalization
  - `run_pipeline()`: Entry point with product URI parsing, platform extraction, and cleanup
- **BT (Brightness Temperature) composite**: S8 (10.85µm) = red, S7 (3.74µm) = green, S9 (12µm) = blue, scaled 250–350K to 0–255
- **FIRE (Thermal Anomaly) detection**: S7-based hot-body colormap (transparent → dark red → yellow → white) with `S3_FIRE_THRESHOLD` (300K) controlling transparency onset
- **`search_s3()`** in `search.py`: Searches Copernicus CDSE for Sentinel-3 products with `S3_PRODUCTTYPE`, `S3_SENSORMODE`, `S3_MAXRECORDS` envvars; log support via `s3_last.json`
- **Viewer: S3 layer group** ("Thermal" / "Lämpö" / "Termisk" / "Thermisch") with BT and FIRE product translations in all 4 languages
- **Viewer: Transparency slider** for FIRE detection layers — range input with hot-body theming, opacity applied in real-time, value persisted to `localStorage`
- **Viewer: S3 overpass toggle** button (red theme) with `Z_INDEX_PREDICTIONS` slot
- S3 legends (`S3-BT`, `S3-FIRE`) in `legends.py` with thermal composite and hot-body gradient HTML
- `S3-BT` (1000m) and `S3-FIRE` (1000m) entries in `RES_MAP` and `DIRS` constants

### Changed
- **Z-index re-ordering** in viewer (bottom to top): main products & ROIs (time-of-flight) → identify tiles (100M) → prediction GeoJSONs (150M) → user GeoJSON overlays (200M). Overpass layers moved from `Z_INDEX_OVERLAYS+100` to dedicated `Z_INDEX_PREDICTIONS` band.
- `pipelines.py`: Integrated S3 search/download/process blocks, `S3_PROCESSES` and `S3_BOX` envvar support, cleanup & notification integration
- `cleanup.py`: S3 filename patterns (`.SEN3`, `S3A/B_` prefix) for source data cleanup and analytic time parsing
- `metadata_engine.py`: S3 timestamp parsing in `generate_sidecar()`
- `.env.example`: Added `S3_BOX`, `S3_STARTDATE`, `S3_MAXRECORDS`, `S3_PRODUCTTYPE`, `S3_SENSORMODE`, `S3_PROCESSES`
- `notifications.py`: Fixed mypy type hints (`Optional[str]` for `urls`/`attachment` params)
- `overflight_predictor.py`: Fixed mypy `tles` redefinition and `current_pass` type annotation

## 2026-06-13 — Overpass GeoJSON & Performance Optimisation

### Added
- **Swath GeoJSON generation** in `overflight_predictor.py`: full pass propagation (rise→set at 1-minute steps), swath rectangles perpendicular to satellite heading (250 km S1, 290 km S2), written to `output/visual/overpass_s1.geojson` and `overpass_s2.geojson`
- Empty FeatureCollection written when no pass in 5-day window (prevents stale overlay display)
- Viewer: two toggle buttons "S1 Overpass" and "S2 Overpass" (default off) with Zulu timestamp labels; S1 in yellow, S2 in cyan; state persisted across reloads
- **TLE disk cache** (`output/.tle_cache.json`, 2-hour TTL) avoids redundant Celestrak requests on warm runs. (`overflight_predictor.py:140-206`)

### Changed
- `overflight_predictor.py`: complete rewrite — `_bearing()`, `_offset()`, `_swath_segment()` geometry helpers; `get_pass_track()` replaces peak-only logic; `predict_all()` generates GeoJSONs as side effect
- `viewer/index.html`: added `#overpass-s1` and `#overpass-s2` toggle buttons
- `viewer/js/app.js`: `toggleOverpassS1()`/`toggleOverpassS2()` functions with `ol.layer.Vector` loading from `imagery/visual/overpass_s*.geojson`
- `viewer/js/translations.js`: `overpass_s1`/`overpass_s2` keys in all 4 languages (fi/sv/en/de)
- **Step resolution**: propagation changed from 5-minute to 1-minute steps (450 km → 90 km step spacing) with neighbor expansion (±1 step) for reliable box-edge detection. (`overflight_predictor.py`)
- **TLE fetch**: 7 individual HTTP requests replaced with concurrent `ThreadPoolExecutor(max_workers=8)`, cutting fetch time from ~8s to ~1s. (`overflight_predictor.py:166-176`)
- **Removed**: `test_gf_gpu.py` (one-off diagnostic, deleted)

### Fixed
- **Overpass forecast filtering**: `predict_all()` no longer uses observer-based `find_events(altitude_degrees=30)` to find passes. Instead computes the full satellite ground track at 1-minute intervals and selects only passes whose ground track actually crosses (or passes within swath half-width of) the configured bounding boxes. Swaths are clipped to the relevant portion near the boxes. `_FORECAST_PASSES=N` now correctly shows the next N passes that cross the search areas. (`overflight_predictor.py`)
- Removed unused `boxes_union` parameter from `build_swath_geojson()`. (`overflight_predictor.py:241`)
- **NORAD IDs**: fixed S1C (`62235`→`62261`), added S1B (`41456`), added S1D (`66315`), fixed S2C (`61005`→`60989`). (`constants.py`)
- **Unconditional `predict_all()`** in `pipelines.py`: overpass forecast now updates on every pipeline run, not only when new products are found. (`pipelines.py:268`)

## 2026-06-12 — Guided Filter Finalized

### Added
- **Guided Filter** post-processing for S2 visual products (TCI-GF, NIRFC-GF) using Kaiming He's guided filter
  - CPU path via `scipy.ndimage.uniform_filter`, GPU path via CuPy
  - TCI-GF uses B08 (NIR) band for guidance; NIRFC-GF uses self-guidance
  - Detail enhancement step with `detail_strength` parameter
  - Edge-padding with replication to eliminate tile seam artifacts
- New product codes: `TCI-GF`, `NIRFC-GF` in `S2_PROCESSES`

### Changed
- GF defaults tuned: `GF_RADIUS=2`, `GF_DETAIL_STRENGTH=2.0`, `GF_EPSILON=0.01`
- Memory management: explicit `del` + `gc.collect()` in CPU guided filter path
- Guidance float32 conversion moved outside per-channel loop

### Configuration (Breaking)
- **New `.env` variables**: `GF_RADIUS`, `GF_EPSILON`, `GF_GUIDANCE`, `GF_DETAIL_STRENGTH`
- **New directory constants**: `VIS_S2_TCI_GF`, `VIS_S2_NIRFC_GF` in `DIRS`
- **New RES_MAP entries**: `S2-TCI-GF` and `S2-NIRFC-GF` at 10m resolution

---

## 2026-05-30 — Viewer & ROI Refinements

### Added
- Viewer: Sentinel-2 product sorting by acquisition time
- Viewer: Complete Finnish translations
- ROIs: Multi-tile mosaicing support (auto-fallback from single-tile to combined coverage)

### Fixed
- ROI crops: Fixed alpha transparency regression in cropped output
- ROI crops: Resolved sidecar absence and filename inconsistencies

---

## 2026-05-24 — ROI Enhancement & Apprise Integration

### Added
- **Apprise notifications** per-ROI: full-size JPEG sent via configurable `apprise_url`
- Full-size image generation (`_full.jpg`) alongside social media HEIC crops
- `roi_config.json` now supports per-ROI `apprise_url` field
- Universal product support in ROI cropping (all S1, S2, FUSED product types)

### Changed
- ROI grouping improved: now groups by (Date, Orbit, Direction, Product) for robust multi-tile handling
- Coverage calculation uses Shapely polygon union for accurate mosaicing decisions

---

## 2026-05-17 — AIS Integration

### Added
- **AIS Correlator** (`ais-correlator/ais_correlator.py`): Overlays AIS vessel metadata onto satellite imagery
  - **Primary target**: S1-RATIO products (RATIO-AIS) — detects dark vessels and false-coordinate transmitters
  - **Optional**: S2-TCI products (TCI-AIS)
  - Post-pipeline hook integration, retry logic for AIS API calls
  - New product types: `AIS` in both `S1_PROCESSES` and `S2_PROCESSES`
- Viewer: Fullscreen mode, screenshot tool, BBOX widget

### Configuration (Breaking)
- **New `.env` variables**: `AIS_RECORDER_URL`, `AIS_MAX_TIME_MINUTES`

---

## 2026-05-15 — Viewer & Metadata Enhancements

### Added
- Viewer: Multi-language support (Finnish translations for button tooltips)
- Viewer: Interactive layer identification, zoom controls, sidebar navigation
- Viewer: Configurable GeoJSON overlay support via `config.json`
- Viewer: Robust z-index management for overlay layering
- `CLEANUP_RAW` env var for immediate source `.SAFE` directory cleanup after processing

### Changed
- Viewer overflight text localized with line break separator

### Configuration
- **New `.env` variable**: `CLEANUP_RAW`

---

## 2026-05-13 — Cleanup & ROI Fixes

### Fixed
- Cleanup: Regex bugs in product filename matching resolved
- ROI: Sidecar metadata now correctly generated for all crop types

---

## 2026-05-11 — Bluesky Autoposter & Viewer i18n

### Added
- **Bluesky autoposter**: ROI crops can be automatically posted to Bluesky
  - `roi_bsky_username`, `roi_bsky_pw`, `roi_bsky_post`, `roi_bsky_names` in config
  - HEIC encoding with iterative compression to stay under 2MB limit
  - Rich text with functional hashtags
- Viewer: Multi-language support infrastructure
- `roi_config.json` now supports `.env` variable expansion in bbox/name fields

### Configuration (Breaking)
- `roi_config.json` format updated: supports dict with `"config"` and `"rois"` keys (backward compatible with old list-only format)

---

## 2026-05-10 — Overflight Prediction & Metadata Expansion

### Added
- **Overflight prediction** (`overflight_predictor.py`): TLE-based next pass prediction for S1/S2
  - Integrated into `inventory.json` as `next_overflights`
  - Multi-satellite support (S1A, S1C, S2A, S2B, S2C)
  - Multi-BBOX support with `.env` variable name labeling
- **Cloud cover extraction**: S2 metadata now includes `cloud_cover` percentage
- **`.env` variable expansion**: ROI config and BBOX strings support `${VAR}` references
- **Multi-BBOX support**: S1_BOX and S2_BOX accept semicolon-separated or JSON list of boxes

### Fixed
- Overflight: Fixed datetime comparison logic and TLE fetching for multiple satellites
- Env: Centralized variable expansion in `functions.py::resolve_env_variable()`
- Metadata: Corrupted TIFF files handled gracefully during regeneration
- Metadata: Fixed indentation error in metadata_engine.py

### Configuration
- **New `.env` variables**: (none new — existing `S1_BOX`, `S2_BOX` now support multi-BBOX syntax)
- **inventory.json format change**: Now includes `next_overflights` array

---

## 2026-05-07 — Analytic Cleanup

### Added
- **36-hour analytic file cleanup**: `cleanup.py` now removes analytic (float32) outputs older than 36 hours
- Separate `CLEANUP_ROI_DAYS` env var for ROI retention (independent of visual product retention)

### Configuration
- **New `.env` variable**: `CLEANUP_ROI_DAYS`

---

## 2026-05-03 — Viewer Overlays & Script Cleanup

### Added
- Viewer: Configurable GeoJSON overlay support via `config.json`
- Viewer: Robust z-index management for overlays

### Removed
- Obsolete/unused scripts cleaned from repository

---

## 2026-05-01 — DATA_DIR & Viewer Time Range

### Added
- `DATA_DIR` env var to relocate both `temp/` and `output/` to another drive
- Viewer: Time range filtering for layer display

### Configuration (Breaking)
- **New `.env` variable**: `DATA_DIR`
- `TARGET_DIR` still overrides output location if provided; defaults to `DATA_DIR/output`

---

## 2026-04-28 — Viewer Interactive Features

### Added
- Viewer: Interactive identify (click to inspect layers)
- Viewer: Zoom controls, sidebar navigation
- Viewer: Finnish translations for button tooltips

---

## 2026-04-21 — Sentinel-2 Band Mapping Fix

### Fixed
- **S2 band mapping correction**: Red/Blue bands were swapped in TCI/NIRFC rendering
  - Affects all S2 visual products using 10m bands (TCI, NIRFC)
  - Prior output had incorrect Red/Blue channel assignment

### Configuration (Breaking)
- No config change, but visual output for all existing S2 products will differ after this fix

---

## 2026-04-19 — Notification System & Search Statistics

### Added
- **Notifications module** (`notifications.py`): Sends pipeline run summaries via Apprise
- **Search statistics**: Pipeline summary now includes counts of processed S1, S2, Fusion, and ROI products
- Fusion product count included in Apprise notification

### Configuration
- **New `.env` variable**: `APPRISE_URLS`

---

## 2026-04-19 — Industrial-Strength OSINT Pipeline Overhaul

### Added
- Multiple fusion products: RADAR-BURN, LIFE-MACHINE, TARGET-PROBE-V2
- CUDA-accelerated fusion computation
- Product-specific error handling with graceful skips on missing dependencies
- Improved Fusion logging with per-product metadata propagation

### Changed
- Major fusion engine rewrite: block-wise processing with ghost masking
- Orbit metadata (relative orbit, pass direction) propagated through fusion products

---

## 2026-03-07 — PEP-8 & Type Safety Refactoring

### Changed
- Achieved full PEP-8 compliance across the codebase
- Added comprehensive type hints
- Fixed fusion striping artifacts in S1/S2 composite products
- Fixed S1 alpha transparency rendering

### Fixed
- GPU math verified for correctness
- COG/Metadata engine verified for consistency

---

## 2026-03-06 — CUDA Integration & Macro-Block Rendering

### Added
- CUDA/CuPy integration for GPU-accelerated index computation
- Dual-output paths (visual 8-bit + analytic float32)
- Macro-block rendering (2048x2048 blocks) for memory efficiency

### Fixed
- I/O bottleneck identified in single-pass render (ongoing)

---

## 2026-06-17 — Cleanup Fixes: ROI Retention, S3 .SEN3 Cleanup & Pre-Run Temp Scrub

### Fixed
- **ROI cleanup retention**: `cleanup_outputs()` no longer operates on `VIS_ROI` — ROI files are exclusively managed by `cleanup_roi_outputs()` with the correct `CLEANUP_ROI_DAYS` window. Previously they were also matched by `find_outdated_products(days=CLEANUP_DAYS)` and removed prematurely.
- **S3 .SEN3 source cleanup**: Restructured `cleanup_source_data()` to use exclusive `if/elif/elif` matching (S1 → S3 → S2 catch-all). Previously the S2 regex `(\d{8}T\d{6})` caught S3 products first and removed their `.SEN3` dirs under the wrong label.
- **S3 sidecar format matching**: Added `S3-(\d{8}T\d{6})Z-` regex to handle visual sidecar base names (e.g. `S3-20260612T212421Z-BT`) — the old `S3[AB]_` pattern only matched CDSE titles.
- **Log message now says** `source (.SAFE/.SEN3) directories` instead of just `.SAFE`.

### Added
- **`cleanup_temp_files()`** in `cleanup.py` — removes known temp/intermediate files left by interrupted runs before processing starts. Called automatically at the top of `pipelines.py`. Cleans:
  - `/tmp/` named intermediates (`vv_raw.tif`, `vv.tif`, `s2_10m.tif`, `s3_bt.tif`, etc.)
  - `*.grid.tif` GPU warp coordinate grids (never explicitly deleted until now)
  - `*.tmp.tif` COG finalizer orphans in the output tree
- **`glob_remove()`** helper for recursive pattern-matched file deletion.

## 2026-03-05 — Advanced SAR Denoising & Automated Legends

### Added
- Advanced SAR denoising: Lee, Frost, and Gamma Map filters (CPU + CUDA)
- Automated legend generation via `legends.py`
- 3-way alpha masking for improved product compositing
- Refined OSINT indices: CAMO, NDBI_CLEAN, AP
