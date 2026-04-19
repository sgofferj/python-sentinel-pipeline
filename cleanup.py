#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cleanup.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Cleanup utility for the Sentinel pipeline.
Removes products older than a specified number of days based on acquisition time.
Cleans up visual/analytic outputs, sidecars, source .SAFE directories, and logs.
"""

import argparse
import json
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import constants as c
import inventory_manager


def parse_args() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(description="Clean up old Sentinel products.")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Age in days of products to keep (default: 30)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Actually perform the deletion (default is dry-run)",
    )
    return parser.parse_args()


def get_acquisition_time(json_path: str) -> Optional[datetime]:
    """Extracts acquisition time from a sidecar JSON file."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            atime_str = data.get("acquisition_time")
            if atime_str and atime_str != "Unknown":
                # Handle formats like 2026-04-07T05:06:44Z
                return datetime.fromisoformat(atime_str.replace("Z", "+00:00"))
    except (json.JSONDecodeError, IOError, ValueError):
        pass
    return None


def find_outdated_products(days: int) -> List[Dict[str, Any]]:
    """Scans visual outputs to find products older than 'days'."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    outdated = []

    visual_root = os.path.join(c.DIRS["OUT"], "visual")

    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".json") and file != "inventory.json":
                json_path = os.path.join(root, file)
                acq_time = get_acquisition_time(json_path)

                if acq_time and acq_time < cutoff_date:
                    # Found an outdated product
                    base_name = file.replace(".json", "")
                    outdated.append(
                        {
                            "base_name": base_name,
                            "json_path": json_path,
                            "acq_time": acq_time,
                        }
                    )

    return outdated


def remove_product_files(dir_path: str, base_name: str, dry_run: bool = True) -> int:
    """Removes all files in a directory that start with base_name."""
    removed = 0
    if not os.path.exists(dir_path):
        return 0

    for filename in os.listdir(dir_path):
        if filename.startswith(base_name):
            file_to_remove = os.path.join(dir_path, filename)
            if dry_run:
                print(f"[DRY-RUN] Would remove output file: {file_to_remove}", flush=True)
                removed += 1
            else:
                try:
                    if os.path.isfile(file_to_remove):
                        os.remove(file_to_remove)
                        removed += 1
                except OSError as e:
                    print(f"Error removing {file_to_remove}: {e}", flush=True)
    return removed


def cleanup_outputs(products: List[Dict[str, Any]], dry_run: bool = True) -> None:
    """Removes all output files (visual/analytic/sidecars) for outdated products."""
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(
        f"{action} {len(products)} outdated products from output directories...",
        flush=True,
    )

    removed_count = 0
    # Collect all directories that are subdirs of OUT
    out_dirs = [dp for dp in c.DIRS.values() if dp.startswith(c.DIRS["OUT"])]

    for prod in products:
        base_name = prod["base_name"]
        for dir_path in out_dirs:
            removed_count += remove_product_files(dir_path, base_name, dry_run)

    count_label = "Would remove" if dry_run else "Removed"
    print(f"{count_label} {removed_count} output files.", flush=True)


def cleanup_source_data(products: List[Dict[str, Any]], dry_run: bool = True) -> None:
    """Removes source .SAFE directories from DIRS['DL']."""
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(f"{action} source .SAFE directories...", flush=True)

    safe_dirs = [d for d in os.listdir(c.DIRS["DL"]) if d.endswith(".SAFE")]
    removed_safes = 0

    for prod in products:
        base_name = prod["base_name"]

        # S1 Logic
        s1_match = re.search(r"S1_(\d{8}T\d{6})_(\d{8}T\d{6})", base_name)
        if s1_match:
            start_t, end_t = s1_match.groups()
            for safe in safe_dirs:
                if f"_{start_t}_" in safe and f"_{end_t}_" in safe:
                    safe_path = os.path.join(c.DIRS["DL"], safe)
                    if os.path.exists(safe_path):
                        if dry_run:
                            print(f"[DRY-RUN] Would remove source S1: {safe}", flush=True)
                        else:
                            print(f"Removing source S1 product: {safe}", flush=True)
                            shutil.rmtree(safe_path)
                        removed_safes += 1

        # S2 Logic
        s2_match = re.search(r"(\d{8}T\d{6})Z", base_name)
        if s2_match:
            time_str = s2_match.group(1)
            for safe in safe_dirs:
                if f"_{time_str}_" in safe:
                    safe_path = os.path.join(c.DIRS["DL"], safe)
                    if os.path.exists(safe_path):
                        if dry_run:
                            print(f"[DRY-RUN] Would remove source S2: {safe}", flush=True)
                        else:
                            print(f"Removing source S2 product: {safe}", flush=True)
                            shutil.rmtree(safe_path)
                        removed_safes += 1

    count_label = "Would remove" if dry_run else "Removed"
    print(f"{count_label} {removed_safes} source .SAFE directories.", flush=True)


def should_keep_entry(title: str, products: List[Dict[str, Any]]) -> bool:
    """Checks if a log entry should be kept based on outdated products."""
    for prod in products:
        base_name = prod["base_name"]

        # S1 title matches via timestamps
        s1_match = re.search(r"S1_(\d{8}T\d{6})_(\d{8}T\d{6})", base_name)
        if s1_match:
            start_t, end_t = s1_match.groups()
            if f"_{start_t}_" in title and f"_{end_t}_" in title:
                return False

        # S2 title matches via timestamp
        s2_match = re.search(r"(\d{8}T\d{6})Z", base_name)
        if s2_match:
            time_str = s2_match.group(1)
            if f"_{time_str}_" in title:
                return False
    return True


def cleanup_logs(products: List[Dict[str, Any]], dry_run: bool = True) -> None:
    """Removes entries for cleaned products from s1_last.json and s2_last.json."""
    action = "Dry-run: Checking" if dry_run else "Updating"
    print(f"{action} search logs...", flush=True)

    for sat in ["s1", "s2"]:
        log_path = os.path.join(c.DIRS["DL"], f"{sat}_last.json")
        if not os.path.exists(log_path):
            continue

        try:
            with open(log_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        original_files = log_data.get("files", [])
        new_files = [
            e for e in original_files 
            if should_keep_entry(e.get("properties", {}).get("title", ""), products)
        ]

        if len(new_files) < len(original_files):
            diff = len(original_files) - len(new_files)
            if dry_run:
                print(f"[DRY-RUN] Would remove {diff} entries from {sat}_last.json", flush=True)
            else:
                log_data["files"] = new_files
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(log_data, f, indent=4)
                print(f"Updated {sat}_last.json: removed {diff} entries.", flush=True)


def run_cleanup(days: int = 30, dry_run: bool = True) -> None:
    """External entry point for cleanup function."""
    mode = "DRY-RUN" if dry_run else "LIVE (FORCE)"
    print(
        f"--- Starting cleanup ({mode}) for products older than {days} days ---",
        flush=True,
    )

    outdated_products_list = find_outdated_products(days)

    if not outdated_products_list:
        print("No outdated products found.", flush=True)
    else:
        cleanup_outputs(outdated_products_list, dry_run)
        cleanup_source_data(outdated_products_list, dry_run)
        cleanup_logs(outdated_products_list, dry_run)

        if not dry_run:
            print("\nRebuilding inventory...", flush=True)
            inventory_manager.rebuild_inventory()
        else:
            print("\n[DRY-RUN] Skipping inventory rebuild.", flush=True)

    print(f"--- Cleanup ({mode}) complete ---", flush=True)


def main() -> None:
    """Main entry point for cleanup script."""
    args = parse_args()
    run_cleanup(args.days, not args.force)


if __name__ == "__main__":
    main()
