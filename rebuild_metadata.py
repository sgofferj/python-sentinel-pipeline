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
Now delegates all identification and generation logic to metadata_engine.py.
"""

import os
import metadata_engine
import inventory_manager
import constants as c


def rebuild_all():
    print("--- Starting Bulk Metadata Regeneration ---", flush=True)
    visual_root = os.path.join(c.DIRS["OUT"], "visual")
    count = 0

    # Walk through all visual subdirectories
    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".tif"):
                tif_path = os.path.join(root, file)

                try:
                    # Engine now handles identification, legend mapping, 
                    # resolution and timestamp extraction automatically.
                    metadata_engine.generate_sidecar(tif_path)
                    count += 1
                except Exception as e:  # pylint: disable=broad-exception-caught
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
