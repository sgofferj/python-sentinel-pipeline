# Concept: SEN2SR Integration for High-Resolution Sentinel-2 Products

## Executive Summary
This concept outlines the integration of the **ESA SEN2SR** (Super-Resolution) library into the `python-sentinel-pipeline`. The goal is to upscale Sentinel-2 imagery from its native 10m/20m resolution to **2.5m**. This enhancement significantly improves visual clarity for True Color (TCI) and provides high-precision analytic data for vegetation (NDVI) and urban (NDBI) monitoring.

## 1. Feasibility Analysis

### Technical Feasibility
*   **Library:** The `sen2sr` Python library is available via PyPI and GitHub. It supports modern CNN and Transformer-based models.
*   **Hardware:** The pipeline already implements CUDA/GPU acceleration. SEN2SR requires a GPU (ideally 8GB+ VRAM) for practical processing times.
*   **Memory Management:** A 2.5m S2 tile (~44,000 x 44,000 pixels) would consume massive amounts of RAM if loaded at once. However, our existing **Block-Based Renderer** in `functions_s2.py` handles images in windows (default 2048px), making it perfectly suited for processing these high-res products without OOM crashes.

### Scaling Impact
*   **Pixel Count:** 10m -> 2.5m represents a **16x increase** in total pixels.
*   **Processing Time:** Expect a 5-10x increase in S2 processing time per tile due to the neural network inference overhead.
*   **Storage:** Output COGs will be significantly larger (approx. 800MB - 1.5GB per product).

## 2. Integration Architecture

### A. Environment Configuration
Add new variables to `.env`:
*   `S2_SUPER_RESOLVE=true`: Toggle for the SR stage.
*   `S2_SR_MODEL=SEN2SRLite`: Choice of model (Lite for speed, standard for quality).
*   `S2_SR_BANDS=B02,B03,B04,B08,B05,B11,B12`: Which bands to upscale.

### B. New Module: `sen2sr_engine.py`
A dedicated wrapper for the `sen2sr` library.
*   **Input:** `.SAFE` folder path.
*   **Action:** Runs the inference on selected bands.
*   **Output:** Temporary high-res GeoTIFFs (2.5m) in `temp/sr/`.

### C. Pipeline Modifications

1.  **`pipelines.py`**:
    *   Before calling `s2.run_pipeline()`, check if `S2_SUPER_RESOLVE` is enabled.
    *   Invoke `sen2sr_engine` to generate high-res intermediate bands.

2.  **`functions_s2.py`**:
    *   Modify `prepare()`: If SR bands exist in `temp/sr/`, use them as the source for `gdal.Warp` at `xRes=2.5, yRes=2.5`.
    *   Update `_render_internal()`: The block logic remains the same, but the coordinate space and `out_shape` for 20m bands will automatically scale to the 2.5m master resolution.
    *   Update `metadata_engine.py`: Ensure `effective_res` in sidecars is set to `2.5` for SR products.

## 4. Hallucination Mitigation & Data Integrity

In the context of satellite monitoring, "Visual Plausibility" is secondary to "Radiometric Integrity." Super-resolution models (especially GANs) can "hallucinate" high-frequency details (e.g., textures, building edges) that are not physically present. To ensure the 2.5m products are reliable for analysis, the following mitigation strategies will be implemented:

### A. Radiometric Cycle-Consistency
The most fundamental check is the **Downsampling Constraint**.
*   **Method:** The SR output (2.5m), when downsampled back to 10m using a standard kernels (e.g., Average), must match the original input pixels within a strict tolerance (e.g., <1% deviation).
*   **Pipeline Integration:** The `sen2sr_engine.py` will run a post-inference verification pass. If a tile's radiometric drift exceeds the threshold, the system will flag the product in the `inventory.json` with a "Low Confidence" metadata tag.

### B. Model Selection (CNN over GAN)
While Generative Adversarial Networks (GANs) produce the sharpest-looking images, they are the most prone to creative hallucinations.
*   **Decision:** The default model will be a **Residual CNN (like EDSR)** or a **Swin Transformer** with a pixel-shuffle upscaler. These architectures are mathematically constrained to prioritize reconstruction accuracy over "texture synthesis," making them inherently safer for analytic products like NDVI.

### C. Spectral Consistency Masks
Hallucinations often break the spectral relationship between bands.
*   **Check:** Calculating NDVI on the 2.5m SR output should not produce "impossible" values (e.g., high vegetation noise in the middle of a concrete roof) that weren't present in the 10m baseline.
*   **Mitigation:** We will implement a "Spectral Anchor" logic where the 10m RGB bands act as a spatial constraint for the upscaling of the 20m bands, ensuring that the 2.5m analytic results stay within the "structural envelope" of the highest-resolution input.

### D. Uncertainty Mapping (Optional)
If supported by the selected model (e.g., Bayesian SR or Diffusion-based SR), the pipeline will generate an **Uncertainty Layer**. This 1-band TIF highlights areas where the model had to "guess" the most, allowing users to distinguish between confirmed high-res features and speculative upscaling.

## 5. Beyond TCI: Multi-Product Benefits

While True Color (TCI) is the primary target for 2.5m resolution, the following products would see significant improvements:

| Product | Benefit of 2.5m Resolution |
| :--- | :--- |
| **NDVI** | Detection of individual tree crowns, narrow hedgerows, and fine-scale crop health variations. |
| **NDBI / NDBI_CLEAN** | Much sharper building footprints and road network delineation. Highly beneficial for "Urban Heat" monitoring. |
| **NIRFC** | Better discrimination between different types of vegetation and water edges. |
| **Sensor Fusion** | Higher spatial context for S1-S2 composites (e.g., TARGET-PROBE), allowing for better pinpointing of radar-reflective objects within visual structures. |

## 4. Resource Safety Strategy
To maintain the "16GB system" compatibility:
1.  **Sequential Inference**: Process bands one by one in the SR engine to minimize VRAM usage.
2.  **Disk-to-Disk**: Use temporary files liberally instead of keeping high-res arrays in memory.
3.  **Restricted Parallelism**: Limit `MAX_PARALLEL_FINALIZERS` to 1 when `SUPER_RESOLVE` is active to avoid multiple concurrent GDAL processes on massive 2.5m files.

## 5. Conclusion
Integrating SEN2SR is not only feasible but leverages the existing strengths of the pipeline's architecture (CUDA support and block-based rendering). It transforms the pipeline from a standard monitoring tool into a high-precision spatial analysis platform.
