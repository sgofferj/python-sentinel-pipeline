#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# legends.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

import os
import json
import constants as c


def get_radar_burn_legend():
    """Returns HTML for the Turbo-mapped SAR highlight legend."""
    return """
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">TARGET PROBE (S1-VH > -15dB)</div>
        <div style="height: 12px; width: 200px; background: linear-gradient(to right, #30123b, #4662d8, #36aaf9, #1ae4b6, #a4fc3c, #fbb318, #e4460a, #7a0403); border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>Weak</span>
            <span>Strong</span>
            <span>Metal</span>
        </div>
        <div style="margin-top: 8px; border-top: 1px solid #444; padding-top: 5px; font-size: 10px; color: #aaa;">
            SAR backscatter highlight. Red/Orange = Metallic or High-Density Structure.
        </div>
    </div>
    """


def get_target_probe_v2_legend():
    """Returns HTML for the Advanced Target Probe legend."""
    return """
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">TARGET PROBE V2 (Sensor Fusion)</div>
        <div style="height: 12px; width: 200px; background: linear-gradient(to right, #141428, #00c800, #00ffff, #ffff00, #ff0000); border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>Nature</span>
            <span>Safe</span>
            <span>Unusual</span>
            <span>Built</span>
        </div>
        <div style="margin-top: 8px; color: #aaa; font-size: 10px;">Logic: (NDBI-NDRE) gated by S1-VH</div>
    </div>
    """


def get_life_machine_legend():
    """Returns HTML for the Discovery Composite (Life/Machine) legend."""
    return """
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 8px; color: #ffeb3b;">DISCOVERY COMPOSITE</div>
        <div style="display: flex; flex-direction: column; gap: 5px;">
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #f00; border: 1px solid #500;"></div>
                <span>MACHINE: S1-VH Intensity</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #0f0; border: 1px solid #050;"></div>
                <span>LIFE: S2-NDVI (Vegetation)</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #00f; border: 1px solid #005;"></div>
                <span>CONTEXT: S2-Blue (Terrain)</span>
            </div>
        </div>
        <div style="margin-top: 8px; border-top: 1px solid #444; padding-top: 5px; font-size: 10px; color: #aaa;">
            Yellow = Machinery in Veg, Cyan = Wetland/Water Context.
        </div>
    </div>
    """


def get_standard_sar_legend(pol="VH"):
    """Returns HTML for standard grayscale dB products."""
    return f"""
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">SAR {pol} (Sigma0 dB)</div>
        <div style="height: 12px; width: 200px; background: linear-gradient(to right, #000, #fff); border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>{c.S1_DB_MIN}dB</span>
            <span>{c.S1_DB_MAX}dB</span>
        </div>
    </div>
    """


def get_ratio_sar_legend():
    """Returns HTML for SAR VV/VH Ratio composite legend."""
    return """
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 8px; color: #ffeb3b;">SAR RATIO COMPOSITE</div>
        <div style="display: flex; flex-direction: column; gap: 5px;">
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #f00; border: 1px solid #444;"></div>
                <span>RED: VV Intensity</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #0f0; border: 1px solid #444;"></div>
                <span>GRN: VH Intensity</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #00f; border: 1px solid #444;"></div>
                <span>BLU: VV/VH Ratio</span>
            </div>
        </div>
        <div style="margin-top: 8px; border-top: 1px solid #444; padding-top: 5px; font-size: 10px; color: #aaa;">
            Yellow=Built/Rough, Magenta=Smooth/Water, Green=Veg/Canopy.
        </div>
    </div>
    """


def get_s2_index_legend(name, unit, vmin, vmax, colormap="RdYlGn", labels=None):
    """Returns HTML for standard S2 index products."""
    gradient = "linear-gradient(to right, #a50026, #d73027, #f46d43, #fdae61, #fee08b, #ffffbf, #d9ef8b, #a6d96a, #66bd63, #1a9850, #006837)"
    if colormap == "grayscale":
        gradient = "linear-gradient(to right, #000, #fff)"
    if colormap == "urban":
        gradient = "linear-gradient(to right, #141428, #3c3c3c, #ffff00, #ff0000)"
    if colormap == "osint":
        gradient = (
            "linear-gradient(to right, #141428, #00c800, #00ffff, #ffff00, #ff0000)"
        )

    labels_html = ""
    if labels:
        labels_html = f"""
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            {"".join([f"<span>{l}</span>" for l in labels])}
        </div>
        """
    else:
        labels_html = f"""
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>{vmin}</span>
            <span>{vmax}</span>
        </div>
        """

    return f"""
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">S2 {name} ({unit})</div>
        <div style="height: 12px; width: 200px; background: {gradient}; border: 1px solid #444;"></div>
        {labels_html}
    </div>
    """


def get_s2_composite_legend(name, r_desc, g_desc, b_desc, extra_info=None):
    """Returns HTML for S2 multi-band composites (NIRFC, AP, CAMO)."""
    extra_html = ""
    if extra_info:
        extra_html = f"""
        <div style="margin-top: 8px; border-top: 1px solid #444; padding-top: 5px; font-size: 10px; color: #aaa;">
            {extra_info}
        </div>
        """

    return f"""
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 8px; color: #ffeb3b;">S2 {name}</div>
        <div style="display: flex; flex-direction: column; gap: 5px;">
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #f00; border: 1px solid #444;"></div>
                <span>RED: {r_desc}</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #0f0; border: 1px solid #444;"></div>
                <span>GRN: {g_desc}</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
                <div style="width: 12px; height: 12px; background: #00f; border: 1px solid #444;"></div>
                <span>BLU: {b_desc}</span>
            </div>
        </div>
        {extra_html}
    </div>
    """


def get_s3_bt_legend():
    """Returns HTML for S3 Brightness Temperature composite legend with colorbar."""
    return """
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">S3 BT (Brightness Temperature)</div>
        <div style="height: 16px; width: 200px; background: linear-gradient(to right, #000, #001a4d, #0033cc, #6699ff, #fff, #ffddcc, #ff9933, #cc4400, #881100); border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>250K</span>
            <span>300K</span>
            <span>350K</span>
        </div>
        <div style="margin-top: 5px; font-size: 10px; color: #aaa;">
            Mean S7/S8/S9 BT. Cold = dark/blue, moderate = white, hot = orange/red.
        </div>
    </div>
    """


def get_s3_fire_legend():
    """Returns HTML for S3 Fire Detection legend with colorbar."""
    return """
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">S3 FIRE (Thermal Anomaly)</div>
        <div style="height: 16px; width: 200px; background: linear-gradient(to right, rgba(0,0,0,0), #500, #c00, #ff4400, #ffaa00, #ffee00, #ffffff); border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>300K</span>
            <span>340K</span>
            <span>380K+</span>
        </div>
        <div style="margin-top: 5px; font-size: 10px; color: #aaa;">
            S7 (3.74 µm) hot-body detection. Cold backgrounds are transparent.
        </div>
    </div>
    """


def get_s1_delta_legend():
    """Returns HTML for S1 Delta (change) legend — magnitude 0→max, gate at 2.0 dB."""
    pal = c.S1_DELTA_PALETTE.lower()
    if pal == "turbo":
        palette = "linear-gradient(to right, #30123b, #4662d8, #36aaf9, #1ae4b6, #a4fc3c, #fbb318, #e4460a, #7a0403)"
        labels = ("Removed", "Stable", "New")
    elif pal == "viridis":
        palette = "linear-gradient(to right, #440154, #482777, #3e4989, #31688e, #26828e, #35b779, #6ece58, #b5de2b, #fde725)"
        labels = ("Removed", "Stable", "New")
    elif pal in ("grey-red", "grey_red"):
        palette = "linear-gradient(to right, #232323, #5a1a1a, #b41e1e, #e05a33, #b41e1e)"
        # grey 35,35,35 at 0 -> red 180,30,30 at max, via 80,35,35 etc.
        palette = "linear-gradient(to right, #232323, #501e1e, #8a2323, #c03030, #e05a33, #b41e1e)"
        labels = ("Stable", "Change", "Strong")
    elif pal == "grey-rdbu":
        palette = "linear-gradient(to right, #232323, #3a4a6b, #4a6bb8, #8a2323, #b41e1e)"
        labels = ("Stable", "Change", "Strong")
    else:  # rdylgn
        palette = "linear-gradient(to right, #a50026, #d73027, #f46d43, #fdae61, #fee08b, #ffffbf, #d9ef8b, #a6d96a, #66bd63, #1a9850, #006837)"
        labels = ("Removed", "Stable", "New")
    # For magnitude |Δ| the legend is 0 -> max, otherwise -max -> +max
    is_mag = pal in ("grey-red", "grey_red", "grey-rdbu")
    if is_mag:
        return f"""
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">S1 DELTA (|VV| Change dB)</div>
        <div style="height: 12px; width: 200px; background: {palette}; border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>0</span>
            <span>{c.S1_DELTA_GATE_DB:.1f}</span>
            <span>{c.S1_DELTA_MAX:.1f}</span>
        </div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px; font-size: 10px; color: #aaa;">
            <span>Stable</span>
            <span>Gate</span>
            <span>Strong</span>
        </div>
        <div style="margin-top: 5px; font-size: 10px; color: #aaa;">
            |VV<sub>t</sub> − VV<sub>t-1</sub>| (same orbit, {c.S1_DELTA_PALETTE}, gate {c.S1_DELTA_GATE_DB:.1f}dB). Grey = no change, red = strong change.
        </div>
    </div>
    """
    return f"""
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">S1 DELTA (VV Change dB)</div>
        <div style="height: 12px; width: 200px; background: {palette}; border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>{c.S1_DELTA_MIN:.1f}</span>
            <span>0</span>
            <span>+{c.S1_DELTA_MAX:.1f}</span>
        </div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px; font-size: 10px; color: #aaa;">
            <span>{labels[0]}</span>
            <span>{labels[1]}</span>
            <span>{labels[2]}</span>
        </div>
        <div style="margin-top: 5px; font-size: 10px; color: #aaa;">
            VV<sub>t</sub> − VV<sub>t-1</sub> (same orbit). Red = loss, Green = new backscatter.
        </div>
    </div>
    """


def get_ais_legend(base_legend_html, product_name):
    """Wraps an existing legend with an AIS overlay indicator."""
    ais_part = """
    <div style="margin-top: 10px; border-top: 1px solid #ffeb3b; padding-top: 5px; display: flex; align-items: center; gap: 8px;">
        <div style="width: 14px; height: 14px; border: 2px solid #ffeb3b; border-radius: 50%; background: rgba(255, 235, 59, 0.2);"></div>
        <span style="color: #ffeb3b; font-weight: bold;">AIS VESSEL DATA ACTIVE</span>
    </div>
    <div style="font-size: 10px; color: #aaa; margin-top: 3px;">
        Circles indicate interpolated vessel positions at time of acquisition.
    </div>
    """
    # Insert before the last closing div of the legend-box
    return base_legend_html.strip().replace("</div>\n    </div>", ais_part + "</div>")


def save_all_legends(output_dir):
    """Saves all legends as a JSON dictionary for frontend consumption."""
    os.makedirs(output_dir, exist_ok=True)

    # Base legends
    ratio_sar = get_ratio_sar_legend()
    tci_s2 = """<div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;"><div style="font-weight: bold; color: #ffeb3b;">S2 TCI (Natural Color)</div></div>"""

    legends = {
        # Fusion
        "RADAR-BURN": get_radar_burn_legend(),
        "TARGET-PROBE-V2": get_target_probe_v2_legend(),
        "LIFE-MACHINE": get_life_machine_legend(),
        # S1
        "S1-VH": get_standard_sar_legend("VH"),
        "S1-VV": get_standard_sar_legend("VV"),
        "S1-RATIO": ratio_sar,
        "S1-RATIO-AIS": get_ais_legend(ratio_sar, "S1 RATIO"),
        # S2 Indices
        "S2-NDVI": get_s2_index_legend(
            "NDVI", "Veg Index", -0.1, 0.9, labels=["No Veg", "Stressed", "Dense"]
        ),
        "S2-NDRE": get_s2_index_legend(
            "NDRE", "Red-Edge", -0.1, 0.5, labels=["Sparse", "Healthy", "Vibrant"]
        ),
        "S2-NDBI": get_s2_index_legend(
            "NDBI",
            "Building Index",
            -0.6,
            0.3,
            "urban",
            labels=["Nature", "Suburban", "Built"],
        ),
        "S2-NDBI_CLEAN": get_s2_index_legend(
            "NDBI_CLEAN",
            "OSINT Detect",
            -0.6,
            0.2,
            "osint",
            labels=["Nature", "Possible", "Detect"],
        ),
        "S2-NBR": get_s2_index_legend(
            "NBR", "Burn Ratio", -0.2, 0.5, labels=["Burned", "Regrow", "Healthy"]
        ),
        # S2 Composites
        "S2-TCI": tci_s2,
        "S2-TCI-AIS": get_ais_legend(tci_s2, "S2 TCI"),
        "S2-NIRFC": get_s2_composite_legend("NIRFC", "NIR (Veg)", "Red", "Green"),
        "S2-AP": get_s2_composite_legend(
            "AP",
            "SWIR-2",
            "SWIR-1",
            "NIR",
            extra_info="Pierces smoke/haze. Cyan/Blue=Vegetation (high NIR), Yellow/Green=Bare soil/Urban/Scarring (high SWIR), Red=Fire/Hot (very high SWIR2), Dark=Water.",
        ),
        "S2-AP-GF": get_s2_composite_legend(
            "AP-GF",
            "SWIR-2 (sharpened)",
            "SWIR-1 (sharpened)",
            "NIR",
            extra_info="Guided-filter sharpened 10m SWIR (synthetic pan B02/B03/B04/B08). Cyan/Blue=Vegetation, Yellow/Green=Bare/Urban, Red=Fire, Dark=Water. Pierces smoke/haze.",
        ),
        # S3 Fire Detection
        "S3-BT": get_s3_bt_legend(),
        "S3-FIRE": get_s3_fire_legend(),
        "S1-DELTA": get_s1_delta_legend(),
        "S2-CAMO": get_s2_composite_legend(
            "CAMO",
            "NDVI",
            "NDRE",
            "TCI-Green",
            extra_info="Yellow=Natural Veg. Magenta/Cyan=Possible Synthetic/Broken Cover.",
        ),
    }
    with open(os.path.join(output_dir, "legends.json"), "w") as f:
        json.dump(legends, f, separators=(",", ":"))
    print(f"Legends saved to {output_dir}/legends.json")


if __name__ == "__main__":
    save_all_legends(c.DIRS["S1S2_LEGENDS"])
