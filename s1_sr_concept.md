# Concept: Sentinel-1 Super-Resolution & Despeckling Integration

## Executive Summary
Unlike Sentinel-2, where super-resolution is primarily an optical upscaling task, Sentinel-1 (SAR) super-resolution is intrinsically linked with **Speckle Reduction**. This concept proposes integrating a deep-learning-based Super-Resolution (SR) stage into the S1 pipeline to enhance Ground Range Detected (GRD) products from 10m to **2.5m or 5m**.

## 1. Technical Approach: Joint SR and Despeckling

Standard interpolation (Bilinear/Lanczos) in SAR data amplifies speckle noise, leading to "grainy" high-res images. The optimal integration uses models that perform **Joint Super-Resolution and Despeckling**.

### Potential Libraries
*   **Solafune-Tools:** Provides a 5x SR model for Sentinel imagery.
*   **THREASURE-Net:** Uses EDSR (Enhanced Deep Residual Networks) to reach 2.5m resolution, specifically optimized for Sentinel-1/2 time series.
*   **Real-ESRGAN / Custom CNNs:** Recent research (e.g., Ayala et al. 2024) suggests using S1 Stripmap (SM) mode data as a training target to upscale standard Interferometric Wide (IW) mode data.

## 2. Integration Architecture

### A. Strategic Placement in `functions_s1.py`
The SR stage must occur **after calibration but before denoising/warping**, or as a replacement for the current `denoise.py` stage.

1.  **Calibration:** Perform radiometric calibration as usual (`s1_calibrator.py`).
2.  **SR Stage (New):** Apply the SR model to the calibrated (but still in slant/ground range) data.
3.  **Warping:** Use `gpu_warp.py` to project the high-res 2.5m data to EPSG:3857.

### B. Impact on Products

| Product | Impact |
| :--- | :--- |
| **VV / VH (Visual)** | Significant improvement in feature edge definition (coastlines, large vessels, urban structures). |
| **RATIO VV/VH** | Cleaner ratio maps with less "salt and pepper" noise, improving target detection. |
| **Fusion (TARGET-PROBE)** | 2.5m SAR data aligned with 2.5m S2 data (from SEN2SR) creates an ultra-high-resolution multi-sensor composite. |

## 3. Implementation Challenges

*   **Compute Intensity:** SAR SR is computationally heavier than optical SR due to the need to handle complex signal characteristics or high-dynamic-range float data.
*   **Artifacts:** Over-smoothing can occur with GAN-based models, potentially hiding small tactical targets (e.g., small vehicles or masts) while making the overall image look "prettier."
*   **Data Size:** A 2.5m S1 tile is 16x larger than a 10m tile.

## 4. Proposed Strategy: "The High-Res Fusion"

The most powerful use case for S1 SR in this pipeline is to match the resolution of the proposed S2 SEN2SR integration.

1.  Enable `S1_SUPER_RESOLVE=true` in `.env`.
2.  Use a model like **EDSR** or **Swin-Transformer** to reach 2.5m.
3.  Directly feed the 2.5m SAR data into the `correlate.py` engine to create **2.5m Fused Products**.

## 5. Conclusion
While more scientifically complex than S2 upscaling, S1 Super-Resolution is the "missing link" for a true high-resolution monitoring pipeline. Integrating a joint despeckling/SR model would elevate the pipeline's output to a level comparable with commercial high-res SAR providers.
