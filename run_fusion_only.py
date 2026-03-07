#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# run_fusion_only.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Maintenance script to trigger the fusion stage independently.
"""

from correlate import run_correlation
import functions as func
import inventory_manager

if __name__ == "__main__":
    # Initialize logger for this manual step
    func.perf_logger.start_run()
    print("Running Fusion Step only...", flush=True)
    run_correlation()
    inventory_manager.rebuild_inventory()
    func.perf_logger.stop_run()
