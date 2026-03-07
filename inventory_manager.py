#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# inventory_manager.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Global inventory manager for compiling visual product metadata.
"""

import json
import os
from typing import Any, Dict, List

import constants as c
import functions as func


def rebuild_inventory() -> None:
    """
    Scans output/visual for all .json sidecar files and compiles a central inventory.json.
    This file acts as the primary data source for the frontend layer picker.
    """
    func.perf_logger.start_step("Rebuilding Global Inventory")

    visual_root: str = os.path.join(c.BASE_DIR, "output/visual")
    inventory: Dict[str, List[Dict[str, Any]]] = {"layers": []}

    # Walk through all visual subdirectories
    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".json") and file != "inventory.json":
                json_path: str = os.path.join(root, file)
                try:
                    with open(json_path, "r", encoding="utf-8") as f:
                        meta: Dict[str, Any] = json.load(f)
                        # Add relative path for frontend consumption
                        # We want the path relative to the 'output' directory
                        rel_path: str = os.path.relpath(
                            json_path.replace(".json", ".tif"), c.DIRS["OUT"]
                        )
                        meta["path"] = rel_path
                        inventory["layers"].append(meta)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print(f"Warning: Could not index {file}: {e}", flush=True)

    # Sort layers by acquisition time (Newest first)
    inventory["layers"].sort(key=lambda x: str(x.get("acquisition_time", "")), reverse=True)

    inventory_path: str = os.path.join(c.DIRS["OUT"], "visual/inventory.json")
    with open(inventory_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2)

    print(f"Global inventory updated: {len(inventory['layers'])} layers indexed.", flush=True)
    func.perf_logger.end_step()


if __name__ == "__main__":
    rebuild_inventory()
