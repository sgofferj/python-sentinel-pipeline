# ToDo

## Cleanup

I noticed that every once in a while, mostly when downloads or pipeline steps fail, get interrupted and such, we get .tif.tmp files which then on further runs become .tif.tmp.tif files with their own sidecars which mess with the viewer and take unnecessarily space. We should look for those in the cleanup routine and get rid of them.

## Delta

Gemini suggests the following changes to the Delta products. Opinion?

### Optimal Delta Architecture Recommendation

If limited to a single channel, **use VV**. However, for a robust operational pipeline, implement a **Dual-Pol (VV + VH) Delta Strategy**:

```
Pass T1 (VV, VH) ──┐
                  ├──> Coregistration & Calibration ──> Log-Ratio / Anomaly Delta
Pass T2 (VV, VH) ──┘
```

1. **Primary Change Mask (VV Delta):** Run thresholding on $\text{VV}_{T2} / \text{VV}_{T1}$ to capture high-confidence additions or removals of high-RCS metallic objects (vehicles, containers, launchers).
2. **Clutter/Moisture Filter (VH Ratio):** Use the $\text{VH}/\text{VV}$ ratio across passes to filter out false alarms caused by surface moisture or wind-driven vegetation changes.
3. **Combined RGB Delta Visualization:**
* **Red:** $\text{VV}_{\text{change}}$ (Heavy target additions/removals)
* **Green:** $\text{VH}_{\text{change}}$ (Structural/canopy/depolarization changes)
* **Blue:** $\text{VV}_{T1}$ or baseline mean (Contextual terrain background)

## Search algorithm

I noticed another oddity. In the old days, when this pipeline was running on bash, I always found a fitting S2 image for the southern part of Valkeakoski (T35VLH) when the engine found an image for the Northern part (T35VLJ). Since the new Python engine, I only ever got stripes for T35VLH, usually on the Eastern edge. I'm wondering if there's a bug in the search engine or possibly in the crop engine.
