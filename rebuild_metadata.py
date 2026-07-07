#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# rebuild_metadata.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Maintenance utility to regenerate all .json sidecar files for existing visual TIFFs.
Preserves existing sidecar fields that cannot be derived from TIFF tags alone.
"""

import json
import os
import metadata_engine
import inventory_manager
import constants as c

# Fields that should be preserved from existing sidecar when TIFF tags are missing
PRESERVE_FIELDS = {"satellite", "cloud_cover", "relative_orbit", "orbit_direction"}


def rebuild_all():
    print("--- Starting Bulk Metadata Regeneration ---", flush=True)
    visual_root = os.path.join(c.DIRS["OUT"], "visual")
    count = 0

    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".tif"):
                tif_path = os.path.join(root, file)
                json_path = tif_path.replace(".tif", ".json")

                old_meta = {}
                if os.path.exists(json_path):
                    try:
                        with open(json_path, "r", encoding="utf-8") as f:
                            old_meta = json.load(f)
                    except Exception:
                        pass

                try:
                    metadata_engine.generate_sidecar(tif_path)

                    if old_meta and os.path.exists(json_path):
                        with open(json_path, "r", encoding="utf-8") as f:
                            new_meta = json.load(f)
                        changed = False
                        for field in PRESERVE_FIELDS:
                            if field in old_meta and field not in new_meta:
                                new_meta[field] = old_meta[field]
                                changed = True
                        if changed:
                            with open(json_path, "w", encoding="utf-8") as f:
                                json.dump(new_meta, f, separators=(",", ":"))
                    count += 1
                except Exception as e:
                    print(
                        f"ERROR regenerating sidecar for {file}: {e}",
                        flush=True,
                    )

    print(f"\nSuccessfully regenerated {count} sidecar files.", flush=True)

    print("\nRebuilding global inventory...", flush=True)
    inventory_manager.rebuild_inventory()

    print("--- Regeneration Complete ---", flush=True)


if __name__ == "__main__":
    rebuild_all()
