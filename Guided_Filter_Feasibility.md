# Guided Filter (Kaiming He) — Feasibility & Effort Analysis

**Date:** 2026-06-11  
**Goal:** Add optional Kaiming He guided filter postprocessing for True Color (TCI) and NIRFC visual products.

---

## 1. What Is the Guided Filter?

Introduced in *"Guided Image Filtering"* by He, Sun & Tang (ECCV 2010, TPAMI 2013).  
Key properties:

- **Edge-preserving smoothing** — smooths flat areas while keeping sharp edges
- **Gradient reversal-free** — avoids artifacts common with bilateral filters
- **O(N) time complexity** — linear in pixel count via box filters / integral images
- **Guidance image** — can be the input itself (self-guided denoising) or a different image (structure transfer / sharpening)

---

## 2. Architectural Context

The S2 rendering pipeline in `functions_s2.py:_render_internal()`:

```
prepare()  →  gdal.Warp to EPSG:3857 @ 10m
                    ↓
            Block reader thread (2048×2048 tiles)
                    ↓
            Per-block processing:
              - Fixed-reflectance scaling (gamma 2.2)
              - Stack RGBA for TCI → [B04, B03, B02, α]
              - Stack RGBA for NIRFC → [B08, B04, B03, α]
              - Index math (NDVI, NDRE, …)
                    ↓
            Block writer thread → GeoTIFF
                    ↓
            COG conversion + overviews + sidecar JSON
```

**There is currently no postprocessing filter stage.** All filtering is SAR-specific (Lee, Frost, Gamma MAP in `denoise.py`).

---

## 3. Feasibility: HIGH

### What makes it work

| Factor | Assessment |
|--------|------------|
| **Block architecture** | 2048×2048 tiles are large; guided filter radii are small (r=2–16). Per-block application is natural. |
| **Dependencies** | No new packages needed. `scipy.ndimage.uniform_filter` is already used in `denoise.py`. CuPy provides `cupyx.scipy.ndimage.uniform_filter` for GPU. |
| **CUDA infrastructure** | `HAS_CUDA` detection exists in both `functions.py` and `functions_s2.py`. GPU stream management already implemented for spectral indices. |
| **Data flow** | Filter fits between scaling/stack and the write queue — no architectural changes needed. |

### Main technical risk: Block boundary artifacts

The guided filter computes local statistics (mean, variance, covariance) over a window of radius `r`. Applied independently per block, pixels near block edges will have truncated neighborhoods, causing visible seams.

**Mitigation options:**

| Option | Pros | Cons |
|--------|------|------|
| **A — Overlapping tiles** | Artifact-free; no extra I/O pass | Reader must fetch `r`-pixel margins; minor complexity increase in reader thread |
| **B — Post-render pass** | Simple; works on full image; no reader changes | Extra full-image read/write I/O; doubles disk traffic for TCI+NIRFC |
| **C — Accept small artifacts** | Zero code overhead | Only viable at r ≤ 2–4; visible on high-contrast edges at larger radii |

**Recommendation:** Option A (overlapping tiles) for production quality. The reader already uses `rio.windows.Window` — expanding each window by `r` pixels and cropping after filtering is ~20 lines of code.

### Guidance strategy

| Product | Guidance | Effect |
|---------|----------|--------|
| **TCI** | B08 (NIR, 10m) | Transfers NIR high-frequency edge information → sharper RGB, smoother flats |
| **TCI** | Self (RGB) | Smoothing-only; no structure transfer |
| **NIRFC** | Self (RGB) or B08 | B08 is already in the composite; self-guided is simpler |

B08-guidance for TCI is the most valuable — it leverages the 10m NIR edge information to enhance the RGB composite perceptually.

---

## 4. Implementation Plan

### 4.1 Guided filter kernel (`functions_s2.py` or new `guided_filter.py`)

```python
def guided_filter(I, p, radius=4, eps=0.01):
    """I: guidance (H×W), p: input (H×W), returns filtered p."""
    # box filter → mean_I, mean_p, mean_Ip, mean_II
    # cov = mean_Ip - mean_I * mean_p
    # var = mean_II - mean_I * mean_I
    # a = cov / (var + eps)
    # b = mean_p - a * mean_I
    # return mean_a * I + mean_b
```

- ~40 lines numpy + scipy path
- ~40 lines CuPy path
- Box filter via `scipy.ndimage.uniform_filter` / `cupyx.scipy.ndimage.uniform_filter`

### 4.2 Integration into render loop

- If `S2_GUIDED_FILTER` env var is set, apply after `np.stack()` and before `write_queue.put()`
- For overlapping tiles: expand reader window → filter → crop back to original bounds
- For TCI: stack RGBA → split → filter each RGB channel with B08 guidance → restack
- For NIRFC: stack RGBA → split → filter each RGB channel (self-guided) → restack

### 4.3 Configuration

New env vars (documented in `.env.example` and read in `constants.py`):

```ini
S2_GUIDED_FILTER = "TCI,NIRFC"       # Which products to filter (comma-separated, empty=disabled)
S2_GUIDED_FILTER_RADIUS = 4          # Local window radius (pixels)
S2_GUIDED_FILTER_EPSILON = 0.01      # Edge-preservation threshold
S2_GUIDED_FILTER_GUIDANCE = "B08"    # "B08" (NIR-guided) or "self" (self-guided)
```

### 4.4 Filter application point in `_render_internal`

After line ~381 (after stacking TCI and NIRFC), before `write_queue.put()`:

```python
if guided_filter_enabled:
    # TCI: filter RGB channels with B08 guidance
    results["TCI_VIS"] = apply_guided_filter_rgb(results["TCI_VIS"], guidance_b08)
    # NIRFC: self-guided filter
    results["NIRFC_VIS"] = apply_guided_filter_rgb(results["NIRFC_VIS"], guidance=None)
```

---

## 5. Effort Breakdown

| Task | Days | Details |
|------|------|---------|
| Guided filter kernel (numpy + cupy) | 0.5–1 | ~80 lines, unit-testable independently |
| Overlapping tile reader | 0.5–1 | Expand window by `r`, crop after filter |
| Integration into `_render_internal` | 0.5–1 | Wire filter call into block loop |
| Env config + constants wiring | 0.25 | `constants.py`, `.env.example`, `functions_s2.py` |
| B08 guidance for TCI (per-channel) | 0.25 | Transform 3×1D guided filter calls per block |
| Testing with real S2 data | 1–2 | Tune radius/epsilon, verify no artifacts at boundaries, compare CPU vs GPU |
| Edge cases | 0.25 | Small edge blocks (partial tiles), nodata handling, zero-area alpha |
| **Total** | **3–6** | |

---

## 6. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Block boundary artifacts | Medium | Medium | Overlapping tiles with `r`-pixel margin |
| GPU memory spike (3× float32 copies) | Low | Low | Already managed by CuPy memory pool & `free_all_blocks()` |
| Performance regression on CPU | Medium | Low | Only enabled when env var is set; default is off |
| Visible smoothing of fine detail | Low | Medium | `epsilon` parameter controls edge sensitivity; default tuned on real data |
| B08 guidance introduces NIR-specific artifacts | Low | Low | Test on diverse scenes; fall back to self-guidance |

---

## 7. Recommendation

**Proceed with implementation.** Low risk, moderate effort, high value for visual product quality. The guided filter is well-understood, the integration points are clean, and the existing CUDA infrastructure makes GPU-accelerated filtering essentially free.

Start with:
1. Kernel in pure numpy + scipy (testable in isolation)
2. CuPy variant (plugs into existing `HAS_CUDA` pattern)
3. Per-block integration with overlapping tiles (Option A)
4. B08-guided TCI + self-guided NIRFC
