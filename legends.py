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
            <span>-15dB</span>
            <span>-7dB</span>
            <span>0dB</span>
        </div>
        <div style="margin-top: 8px; color: #aaa; font-size: 10px;">Base: Sentinel-2 TCI (Natural)</div>
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
            Yellow = Machinery in Veg (High Detection)
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
            <span>{c.S1_dB_MIN}dB</span>
            <span>{c.S1_dB_MAX}dB</span>
        </div>
    </div>
    """

def get_s2_index_legend(name, unit, vmin, vmax, colormap="RdYlGn"):
    """Returns HTML for standard S2 index products."""
    gradient = "linear-gradient(to right, #a50026, #d73027, #f46d43, #fdae61, #fee08b, #ffffbf, #d9ef8b, #a6d96a, #66bd63, #1a9850, #006837)"
    if colormap == "grayscale":
        gradient = "linear-gradient(to right, #000, #fff)"
    if colormap == "urban":
        gradient = "linear-gradient(to right, #28283c, #505050, #ffff00, #ff0000)"
    
    return f"""
    <div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;">
        <div style="font-weight: bold; margin-bottom: 5px; color: #ffeb3b;">S2 {name} ({unit})</div>
        <div style="height: 12px; width: 200px; background: {gradient}; border: 1px solid #444;"></div>
        <div style="display: flex; justify-content: space-between; width: 200px; margin-top: 2px;">
            <span>{vmin}</span>
            <span>{vmax}</span>
        </div>
    </div>
    """

def get_s2_composite_legend(name, r_desc, g_desc, b_desc):
    """Returns HTML for S2 multi-band composites (NIRFC, AP, CAMO)."""
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
    </div>
    """

def save_all_legends(output_dir):
    """Saves all legends as a JSON dictionary for frontend consumption."""
    os.makedirs(output_dir, exist_ok=True)
    legends = {
        # Fusion
        "RADAR-BURN": get_radar_burn_legend(),
        "LIFE-MACHINE": get_life_machine_legend(),
        # S1
        "S1-VH": get_standard_sar_legend("VH"),
        "S1-VV": get_standard_sar_legend("VV"),
        # S2 Indices
        "S2-NDVI": get_s2_index_legend("NDVI", "Veg Index", -0.1, 0.9),
        "S2-NDRE": get_s2_index_legend("NDRE", "Red-Edge", -0.1, 0.5),
        "S2-NDBI": get_s2_index_legend("NDBI", "Building Index", -0.6, 0.1, "urban"),
        "S2-NBR": get_s2_index_legend("NBR", "Burn Ratio", -0.2, 0.5),
        # S2 Composites
        "S2-TCI": """<div class="legend-box" style="padding: 10px; background: rgba(0,0,0,0.8); color: white; border-radius: 5px; font-family: monospace; font-size: 12px;"><div style="font-weight: bold; color: #ffeb3b;">S2 TCI (Natural Color)</div></div>""",
        "S2-NIRFC": get_s2_composite_legend("NIRFC", "NIR (Veg)", "Red", "Green"),
        "S2-AP": get_s2_composite_legend("AP", "SWIR-2", "SWIR-1", "NIR"),
        "S2-CAMO": get_s2_composite_legend("CAMO", "NDVI", "NDRE", "TCI-Green")
    }
    with open(os.path.join(output_dir, "legends.json"), "w") as f:
        json.dump(legends, f, indent=2)
    print(f"Legends saved to {output_dir}/legends.json")

if __name__ == "__main__":
    save_all_legends(os.path.join(c.DIRS['OUT'], "scripts"))
