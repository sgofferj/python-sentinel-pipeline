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
from typing import Any, Dict, List, Optional, Tuple

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
    parser.add_argument(
        "--prune-outside-search",
        action="store_true",
        help="Remove products outside current search boxes (S1_BOX/S2_BOX/S3_BOX in .env)",
    )
    return parser.parse_args()


def get_acquisition_time(json_path: str) -> Optional[datetime]:
    """Extracts acquisition time from a sidecar JSON file."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            atime_str = data.get("acquisition_time")
            if atime_str and atime_str != "Unknown":
                return datetime.fromisoformat(atime_str.replace("Z", "+00:00"))
    except (json.JSONDecodeError, IOError, ValueError):
        pass
    return None


def parse_acquisition_time_from_filename(filename: str) -> Optional[datetime]:
    """Parses acquisition time from analytic filename."""
    s2_match = re.search(r"S2._.*_(\d{8}T\d{6})", filename)
    if s2_match:
        time_str = s2_match.group(1)
        try:
            dt = datetime.strptime(time_str, "%Y%m%dT%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    s1_match = re.search(r"S1[AB]_.*_(\d{8}T\d{6})_", filename)
    if s1_match:
        time_str = s1_match.group(1)
        try:
            dt = datetime.strptime(time_str, "%Y%m%dT%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    s3_match = re.search(r"S3[AB]_.*_(\d{8}T\d{6})_", filename)
    if s3_match:
        time_str = s3_match.group(1)
        try:
            dt = datetime.strptime(time_str, "%Y%m%dT%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    return None


def find_outdated_products(
    days: int, prefix: Optional[str] = None, scan_dir: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Scans visual outputs to find products older than 'days'.

    Args:
        days: Age in days of products to keep.
        prefix: Optional satellite prefix filter (e.g. "S1", "S2", "S3").
                Only products whose base name starts with this prefix are returned.
        scan_dir: Optional subdirectory under visual/ to restrict scanning to
                  (e.g. "fused" for fusion products).
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    outdated: List[Dict[str, Any]] = []

    visual_root = os.path.join(c.DIRS["OUT"], "visual")
    if scan_dir:
        visual_root = os.path.join(visual_root, scan_dir)

    if not os.path.exists(visual_root):
        return outdated

    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".json") and file != "inventory.json":
                json_path = os.path.join(root, file)
                acq_time = get_acquisition_time(json_path)

                if acq_time and acq_time < cutoff_date:
                    base_name = file.replace(".json", "")
                    if prefix and not base_name.startswith(prefix):
                        continue
                    outdated.append(
                        {
                            "base_name": base_name,
                            "json_path": json_path,
                            "acq_time": acq_time,
                        }
                    )

    return outdated


def load_search_boxes() -> List[Tuple[float, float, float, float]]:
    """Parses S1_BOX, S2_BOX, S3_BOX from env into (west, south, east, north) tuples.

    Environment variables are already expanded by load_dotenv() via constants.py.
    Multi-box values use semicolon separators (e.g. "BOX1;BOX2").
    """
    boxes: List[Tuple[float, float, float, float]] = []
    for key in ("S1_BOX", "S2_BOX", "S3_BOX"):
        raw = os.getenv(key)
        if not raw:
            continue
        for part in raw.split(";"):
            part = part.strip()
            if not part:
                continue
            try:
                coords = [float(x) for x in part.split(",")]
                if len(coords) == 4:
                    boxes.append(tuple(coords))  # type: ignore[arg-type]
                else:
                    print(
                        f"  WARNING: Skipping malformed box '{part}' in {key} "
                        f"(expected 4 coordinates, got {len(coords)}).",
                        flush=True,
                    )
            except (ValueError, TypeError) as exc:
                print(
                    f"  WARNING: Skipping unparseable box '{part}' in {key}: {exc}.",
                    flush=True,
                )
    return boxes


def find_products_outside_search(
    search_boxes: List[Tuple[float, float, float, float]],
) -> List[Dict[str, Any]]:
    """Returns products whose sidecar bounds don't intersect any search box.

    Reads the 'bounds' field from each sidecar JSON (Leaflet latLngBounds format:
    [[south, west], [north, east]]) and checks against the search-area boxes.
    Products with no sidecar, no bounds, or unparseable bounds are kept
    (conservative — don't delete uncertain items).
    """
    if not search_boxes:
        print("  No search boxes defined. Skipping outside-search prune.", flush=True)
        return []

    visual_root = os.path.join(c.DIRS["OUT"], "visual")
    if not os.path.exists(visual_root):
        return []

    outdated: List[Dict[str, Any]] = []
    for root, _, files in os.walk(visual_root):
        for file in files:
            if not file.endswith(".json") or file == "inventory.json":
                continue
            json_path = os.path.join(root, file)
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            bounds = meta.get("bounds")
            if not bounds or not isinstance(bounds, list) or len(bounds) != 2:
                continue

            # Leaflet bounds: [[south, west], [north, east]]
            try:
                west = float(bounds[0][1])
                south = float(bounds[0][0])
                east = float(bounds[1][1])
                north = float(bounds[1][0])
            except (TypeError, IndexError, ValueError):
                continue

            # Check intersection with any search box
            intersects = False
            for sw, ss, se, sn in search_boxes:
                if west < se and east > sw and south < sn and north > ss:
                    intersects = True
                    break

            if not intersects:
                base_name = file.replace(".json", "")
                outdated.append(
                    {
                        "base_name": base_name,
                        "json_path": json_path,
                    }
                )

    return outdated


_S2_TILE_RE = re.compile(r"_T(\d{2}[A-Z]{3})_")


def find_s2_excess_versions(max_versions: int) -> List[Dict[str, Any]]:
    """Scans S2 visual products and returns those exceeding max_versions per tile.

    Groups S2 products by their UTM grid tile (e.g. T35VPK) and keeps only
    the N most recent ones. The rest are marked outdated regardless of age.
    """
    products_by_tile: Dict[str, List[Dict[str, Any]]] = {}
    visual_root = os.path.join(c.DIRS["OUT"], "visual")

    if not os.path.exists(visual_root):
        return []

    for root, _, files in os.walk(visual_root):
        for file in files:
            if file.endswith(".json") and file != "inventory.json":
                json_path = os.path.join(root, file)
                base_name = file.replace(".json", "")

                if not base_name.startswith("S2"):
                    continue

                match = _S2_TILE_RE.search(base_name)
                if not match:
                    continue
                tile = match.group(1)

                acq_time = get_acquisition_time(json_path)
                if acq_time is None:
                    continue

                products_by_tile.setdefault(tile, []).append(
                    {
                        "base_name": base_name,
                        "json_path": json_path,
                        "acq_time": acq_time,
                    }
                )

    outdated: List[Dict[str, Any]] = []
    for tile, products in products_by_tile.items():
        products.sort(key=lambda p: p["acq_time"], reverse=True)
        n_total = len(products)
        if n_total > max_versions:
            excess = products[max_versions:]
            print(
                f"  Tile {tile}: {n_total} products, "
                f"keeping {max_versions} newest, removing {len(excess)}.",
                flush=True,
            )
            outdated.extend(excess)
        else:
            print(
                f"  Tile {tile}: {n_total} products (\u2264{max_versions}), keeping all.",
                flush=True,
            )

    return outdated


ANALYTIC_HOURS_CUTOFF = 36


def find_outdated_analytic_files(
    hours: int = ANALYTIC_HOURS_CUTOFF,
) -> List[Dict[str, Any]]:
    """Scans analytic outputs to find files older than 'hours' from acquisition time."""
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    outdated: List[Dict[str, Any]] = []

    analytic_root = os.path.join(c.DIRS["OUT"], "analytic")

    if not os.path.exists(analytic_root):
        return outdated

    for root, _, files in os.walk(analytic_root):
        # Skip Delta and S1 VV/VH analytic - needed for Delta (14d retention) handled by cleanup_delta_outputs / S1 retention
        if "delta" in root.lower() or "s1/vv" in root.lower() or "s1\\vv" in root.lower() or "s1/vh" in root.lower():
            continue
        for file in files:
            file_path = os.path.join(root, file)

            acq_time = parse_acquisition_time_from_filename(file)
            if acq_time is None:
                try:
                    file_mtime = datetime.fromtimestamp(
                        os.path.getmtime(file_path), tz=timezone.utc
                    )
                    acq_time = file_mtime
                except OSError:
                    continue

            if acq_time < cutoff_time:
                outdated.append(
                    {
                        "file_path": file_path,
                        "file_name": file,
                        "acq_time": acq_time,
                    }
                )

    return outdated


def cleanup_analytic_outputs(
    products: List[Dict[str, Any]], dry_run: bool = True
) -> None:
    """Removes outdated analytic files."""
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(
        f"{action} analytic outputs older than {ANALYTIC_HOURS_CUTOFF} hours...",
        flush=True,
    )

    removed_count = 0

    for prod in products:
        file_path = prod["file_path"]
        if dry_run:
            print(f"[DRY-RUN] Would remove analytic file: {file_path}", flush=True)
            removed_count += 1
        else:
            try:
                os.remove(file_path)
                removed_count += 1
            except OSError as e:
                print(f"Error removing {file_path}: {e}", flush=True)

    count_label = "Would remove" if dry_run else "Removed"
    print(f"{count_label} {removed_count} analytic files.", flush=True)


def remove_product_files(dir_path: str, base_name: str, dry_run: bool = True) -> int:
    """Removes all files in a directory that start with base_name."""
    removed = 0
    if not os.path.exists(dir_path):
        return 0

    for filename in os.listdir(dir_path):
        if filename.startswith(base_name):
            file_to_remove = os.path.join(dir_path, filename)
            if dry_run:
                print(
                    f"[DRY-RUN] Would remove output file: {file_to_remove}", flush=True
                )
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
    """
    Removes all output files (visual only - analytic handled separately)
    for outdated products.
    """
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(
        f"{action} {len(products)} outdated products from visual output directories...",
        flush=True,
    )

    removed_count = 0
    visual_dirs = [
        dp
        for dp in c.DIRS.values()
        if dp.startswith(os.path.join(c.DIRS["OUT"], "visual"))
        and dp not in (c.DIRS["VIS_ROI"], c.DIRS["VIS_S1_DELTA"])
    ]

    for prod in products:
        base_name = prod["base_name"]
        for dir_path in visual_dirs:
            removed_count += remove_product_files(dir_path, base_name, dry_run)

    count_label = "Would remove" if dry_run else "Removed"
    print(f"{count_label} {removed_count} visual output files.", flush=True)


def cleanup_source_data(products: List[Dict[str, Any]], dry_run: bool = True) -> None:
    """Removes source .SAFE and .SEN3 directories from DIRS['DL']."""
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(f"{action} source directories...", flush=True)

    dl = c.DIRS["DL"]
    safe_dirs = [
        d for d in os.listdir(dl) if d.endswith(".SAFE") or d.endswith(".SEN3")
    ]
    removed_safes = 0

    for prod in products:
        base_name = prod["base_name"]

        s1_match = re.search(r"S1[AB]_.*_(\d{8}T\d{6})_(\d{8}T\d{6})", base_name)
        s3_match = re.search(r"S3[AB]_.*_(\d{8}T\d{6})_", base_name) or re.search(
            r"S3-(\d{8}T\d{6})Z-", base_name
        )
        s2_match = re.search(r"(\d{8}T\d{6})", base_name)

        # S1 Logic
        if s1_match:
            start_t, end_t = s1_match.groups()
            for safe in safe_dirs:
                if f"_{start_t}_" in safe and f"_{end_t}_" in safe:
                    safe_path = os.path.join(c.DIRS["DL"], safe)
                    if os.path.exists(safe_path):
                        if dry_run:
                            print(
                                f"[DRY-RUN] Would remove source S1: {safe}",
                                flush=True,
                            )
                        else:
                            print(f"Removing source S1 product: {safe}", flush=True)
                            shutil.rmtree(safe_path)
                        removed_safes += 1
                    break

        # S3 Logic (must precede S2 catch-all)
        elif s3_match:
            time_str = s3_match.group(1)
            for safe in safe_dirs:
                if f"_{time_str}_" in safe and safe.endswith(".SEN3"):
                    safe_path = os.path.join(c.DIRS["DL"], safe)
                    if os.path.exists(safe_path):
                        if dry_run:
                            print(
                                f"[DRY-RUN] Would remove source S3: {safe}",
                                flush=True,
                            )
                        else:
                            print(f"Removing source S3 product: {safe}", flush=True)
                            shutil.rmtree(safe_path)
                        removed_safes += 1
                    break

        # S2 Logic (catch-all for non-S1/S3 products)
        elif s2_match:
            time_str = s2_match.group(1)
            for safe in safe_dirs:
                if f"_{time_str}_" in safe:
                    safe_path = os.path.join(c.DIRS["DL"], safe)
                    if os.path.exists(safe_path):
                        if dry_run:
                            print(
                                f"[DRY-RUN] Would remove source S2: {safe}",
                                flush=True,
                            )
                        else:
                            print(f"Removing source S2 product: {safe}", flush=True)
                            shutil.rmtree(safe_path)
                        removed_safes += 1
                    break

    count_label = "Would remove" if dry_run else "Removed"
    print(
        f"{count_label} {removed_safes} source (.SAFE/.SEN3) directories.", flush=True
    )


def cleanup_all_source_data(dry_run: bool = True) -> None:
    """Removes ALL source .SAFE and .SEN3 directories from DIRS['DL'].

    Unlike cleanup_source_data(), this does not filter by a product list —
    it removes every source directory it finds. Used by CLEANUP_RAW to
    ensure no stale source directories linger from failed runs.
    """
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(f"{action} ALL source directories...", flush=True)

    dl = c.DIRS["DL"]
    safe_dirs = [
        d for d in os.listdir(dl) if d.endswith(".SAFE") or d.endswith(".SEN3")
    ]
    removed_safes = 0

    for safe in safe_dirs:
        safe_path = os.path.join(dl, safe)
        if dry_run:
            print(f"[DRY-RUN] Would remove source: {safe}", flush=True)
            removed_safes += 1
        else:
            try:
                print(f"Removing source: {safe}", flush=True)
                shutil.rmtree(safe_path)
                removed_safes += 1
            except OSError as e:
                print(f"Error removing {safe}: {e}", flush=True)

    count_label = "Would remove" if dry_run else "Removed"
    print(
        f"{count_label} {removed_safes} source (.SAFE/.SEN3) directories.",
        flush=True,
    )


def should_keep_entry(title: str, products: List[Dict[str, Any]]) -> bool:
    """Checks if a log entry should be kept based on outdated products."""
    for prod in products:
        base_name = prod["base_name"]

        s1_match = re.search(r"S1[AB]_.*_(\d{8}T\d{6})_(\d{8}T\d{6})", base_name)
        s3_match = re.search(r"S3[AB]_.*_(\d{8}T\d{6})_", base_name) or re.search(
            r"S3-(\d{8}T\d{6})Z-", base_name
        )
        s2_match = re.search(r"(\d{8}T\d{6})", base_name)

        # S1 title matches via timestamps
        if s1_match:
            start_t, end_t = s1_match.groups()
            if f"_{start_t}_" in title and f"_{end_t}_" in title:
                return False

        # S3 title matches via timestamp (before S2 catch-all)
        elif s3_match:
            time_str = s3_match.group(1)
            if f"_{time_str}_" in title:
                return False

        # S2 title matches via timestamp (catch-all)
        elif s2_match:
            time_str = s2_match.group(1)
            if f"_{time_str}_" in title:
                return False

    return True


def cleanup_logs(products: List[Dict[str, Any]], dry_run: bool = True) -> None:
    """Removes entries for cleaned products from s1_last.json and s2_last.json."""
    action = "Dry-run: Checking" if dry_run else "Updating"
    print(f"{action} search logs...", flush=True)

    for sat in ["s1", "s2", "s3"]:
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
            e
            for e in original_files
            if should_keep_entry(e.get("properties", {}).get("title", ""), products)
        ]

        if len(new_files) < len(original_files):
            diff = len(original_files) - len(new_files)
            if dry_run:
                print(
                    f"[DRY-RUN] Would remove {diff} entries from {sat}_last.json",
                    flush=True,
                )
            else:
                log_data["files"] = new_files
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(log_data, f, indent=4)
                print(f"Updated {sat}_last.json: removed {diff} entries.", flush=True)


def cleanup_roi_outputs(days: int, dry_run: bool = True) -> None:
    """Removes outdated ROI crops and social media images."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(
        f"{action} ROI outputs older than {days} days...",
        flush=True,
    )

    roi_root = c.DIRS["VIS_ROI"]
    if not os.path.exists(roi_root):
        return

    removed_count = 0
    for filename in os.listdir(roi_root):
        # Format: ROI_Name_Product_YYYY-MM-DDTHHMMSSZ.tif
        # Or: ROI_Name_Product_YYYY-MM-DDTHHMMSSZ_social.jpg
        match = re.search(r"_(\d{4}-\d{2}-\d{2}T\d{6})Z", filename)
        if not match:
            continue

        time_str = match.group(1)
        try:
            acq_time = datetime.strptime(time_str, "%Y-%m-%dT%H%M%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue

        if acq_time < cutoff_date:
            file_path = os.path.join(roi_root, filename)
            if dry_run:
                print(f"[DRY-RUN] Would remove ROI file: {file_path}", flush=True)
                removed_count += 1
            else:
                try:
                    os.remove(file_path)
                    removed_count += 1
                except OSError as e:
                    print(f"Error removing {file_path}: {e}", flush=True)

    count_label = "Would remove" if dry_run else "Removed"
    print(f"{count_label} {removed_count} ROI files.", flush=True)


def cleanup_delta_outputs(days: int, dry_run: bool = True) -> None:
    """Removes outdated S1 Delta visual + analytic products (same retention as S1)."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(f"{action} S1 Delta outputs older than {days} days...", flush=True)
    for root_key in ("VIS_S1_DELTA",):
        root = c.DIRS.get(root_key, "")
        if not root or not os.path.exists(root):
            continue
        removed = 0
        for filename in os.listdir(root):
            match = re.search(r"_(\d{4}-\d{2}-\d{2}T\d{6})Z", filename)
            if not match:
                continue
            time_str = match.group(1)
            try:
                acq_time = datetime.strptime(time_str, "%Y-%m-%dT%H%M%S").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if acq_time < cutoff_date:
                file_path = os.path.join(root, filename)
                if dry_run:
                    print(f"[DRY-RUN] Would remove Delta file: {file_path}", flush=True)
                    removed += 1
                else:
                    try:
                        os.remove(file_path)
                        removed += 1
                    except OSError as e:
                        print(f"Error removing {file_path}: {e}", flush=True)
        count_label = "Would remove" if dry_run else "Removed"
        print(f"{count_label} {removed} {root_key} files.", flush=True)


def cleanup_s2_filtered_redundant(dry_run: bool = True) -> None:
    """
    Removes unfiltered S2 visual products when a filtered counterpart
    exists for the same scene (same tile + timestamp).

    Pairs: TCI <-> TCI-GF, NIRFC <-> NIRFC-GF, AP <-> AP-GF
    Same scene = filename prefix before product suffix, e.g.
    T35VLF-20260821T095041Z-TCI.tif vs T35VLF-20260821T095041Z-TCI-GF.tif
    -> scene = T35VLF-20260821T095041Z

    Only affects visual/s2/* (not ROI, not Delta, not analytic).
    When GF exists it is prioritised (higher detail), so the unfiltered
    is redundant. This matches the per-ROI AIS GF priority.
    """
    pairs = [
        ("TCI", "TCI-GF", "VIS_S2_TCI", "VIS_S2_TCI_GF"),
        ("NIRFC", "NIRFC-GF", "VIS_S2_NIRFC", "VIS_S2_NIRFC_GF"),
        ("AP", "AP-GF", "VIS_S2_AP", "VIS_S2_AP_GF"),
    ]
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(f"{action} S2 unfiltered redundant (filtered exists)...", flush=True)
    total_removed = 0
    for base_prod, gf_prod, base_key, gf_key in pairs:
        base_dir = c.DIRS.get(base_key, "")
        gf_dir = c.DIRS.get(gf_key, "")
        if not base_dir or not gf_dir:
            continue
        if not os.path.exists(base_dir) or not os.path.exists(gf_dir):
            continue
        # Collect scenes that have a filtered product
        gf_scenes: set[str] = set()
        for fname in os.listdir(gf_dir):
            if not fname.endswith(".tif"):
                continue
            suffix = f"-{gf_prod}.tif"
            if fname.endswith(suffix):
                scene = fname[: -len(suffix)]
                gf_scenes.add(scene)
        if not gf_scenes:
            continue
        removed = 0
        # Check base products against filtered set
        for fname in list(os.listdir(base_dir)):
            if fname.endswith(".tif") and fname.endswith(f"-{base_prod}.tif"):
                scene = fname[: -len(f"-{base_prod}.tif")]
                if scene in gf_scenes:
                    base_tif = os.path.join(base_dir, fname)
                    base_json = base_tif.replace(".tif", ".json")
                    if dry_run:
                        print(
                            f"  [DRY-RUN] Would remove redundant {base_prod} (filtered {gf_prod} exists): {base_tif}",
                            flush=True,
                        )
                        if os.path.exists(base_json):
                            print(f"  [DRY-RUN] Would remove {base_json}", flush=True)
                        removed += 1
                    else:
                        try:
                            os.remove(base_tif)
                            removed += 1
                            print(
                                f"  Removed redundant {base_prod}: {base_tif} (filtered {gf_prod} exists for {scene})",
                                flush=True,
                            )
                        except OSError as e:
                            print(f"  Error removing {base_tif}: {e}", flush=True)
                        try:
                            if os.path.exists(base_json):
                                os.remove(base_json)
                                print(f"  Removed sidecar {base_json}", flush=True)
                        except OSError as e:
                            print(f"  Error removing {base_json}: {e}", flush=True)
                        # Remove any extra files for same scene+product (e.g. .tif.ovr, .tif.aux.xml)
                        for extra in list(os.listdir(base_dir)):
                            if extra.startswith(f"{scene}-{base_prod}.") and extra not in (
                                fname,
                                fname.replace(".tif", ".json"),
                            ):
                                extra_path = os.path.join(base_dir, extra)
                                try:
                                    os.remove(extra_path)
                                    print(f"  Removed extra {extra_path}", flush=True)
                                except OSError:
                                    pass
            elif fname.endswith(".json") and fname.endswith(f"-{base_prod}.json"):
                # Orphan sidecar without tif (already counted via tif, but handle orphan)
                scene = fname[: -len(f"-{base_prod}.json")]
                if scene in gf_scenes:
                    json_path = os.path.join(base_dir, fname)
                    tif_path = json_path.replace(".json", ".tif")
                    if not os.path.exists(tif_path):
                        if dry_run:
                            print(
                                f"  [DRY-RUN] Would remove orphan {base_json}",
                                flush=True,
                            )
                            removed += 1
                        else:
                            try:
                                os.remove(json_path)
                                removed += 1
                                print(f"  Removed orphan {json_path}", flush=True)
                            except OSError:
                                pass
        if removed:
            print(
                f"  {base_prod}: {removed} redundant file(s) {'would be ' if dry_run else ''}removed (filtered {gf_prod} exists).",
                flush=True,
            )
            total_removed += removed
    if total_removed == 0:
        print("  No S2 redundant unfiltered products found.", flush=True)
    else:
        label = "Would remove" if dry_run else "Removed"
        print(f"  {label} total {total_removed} S2 redundant unfiltered files.", flush=True)


STALE_TMP_PATTERNS: List[str] = [
    "vv_raw.tif",
    "vh_raw.tif",
    "vv.tif",
    "vh.tif",
    "s2_10m.tif",
    "s2_20m.tif",
    "s3_bt.tif",
]


def glob_remove(pattern: str, root: str, dry_run: bool = True) -> int:
    """Recursively remove files matching a filename pattern under root."""
    removed = 0
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(pattern) or fn == pattern:
                path = os.path.join(dirpath, fn)
                if dry_run:
                    print(f"  [DRY-RUN] Would remove {path}", flush=True)
                else:
                    try:
                        os.remove(path)
                        removed += 1
                    except OSError as e:
                        print(f"  Error removing {path}: {e}", flush=True)
    if not dry_run:
        return removed
    return 0


def cleanup_temp_files(dry_run: bool = True) -> None:
    """
    Removes known temporary/intermediate files left by interrupted pipeline runs.

    Safe to call at any time — only deletes well-known temp patterns.
    """
    action = "Dry-run: Checking" if dry_run else "Cleaning up"
    print(f"\n--- {action} stale temporary files ---", flush=True)
    total = 0

    # 1. Known /tmp/ intermediates by exact name
    tmp_dir = "/tmp"
    for name in STALE_TMP_PATTERNS:
        path = os.path.join(tmp_dir, name)
        if os.path.exists(path):
            if dry_run:
                print(f"  [DRY-RUN] Would remove {path}", flush=True)
            else:
                try:
                    os.remove(path)
                    total += 1
                except OSError as e:
                    print(f"  Error removing {path}: {e}", flush=True)

    # 2. *.grid.tif GPU warp grids (anywhere under /tmp or output)
    for root in [tmp_dir, c.DIRS["OUT"]]:
        total += glob_remove(".grid.tif", root, dry_run)

    # 3. COG finalizer orphans: *.tmp.tif, *.tmp.json, *.ovr.tmp, *.tif.tmp, etc.
    #    These are left when gdal_translate / gdaladdo is killed mid-run.
    #    They appear as S1_...tif.tmp.tif, S1_...tif.tmp.tif.ovr.tmp, etc.
    #    Also handle generic ".tmp" in filename to catch S1_...tif.tmp and variants.
    for root in [c.DIRS["OUT"]]:
        total += glob_remove(".tmp.tif", root, dry_run)
        total += glob_remove(".tmp.json", root, dry_run)
        total += glob_remove(".ovr.tmp", root, dry_run)
        total += glob_remove(".tif.tmp", root, dry_run)
        # Generic: any file with ".tmp" in name (e.g., S1_...tif.tmp.tif, S1_...json.tmp.json)
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if ".tmp" in fn:
                    # Already handled exact suffixes above, but catch e.g., .tmp.tif.json
                    if fn.endswith(".tmp.tif") or fn.endswith(".tmp.json") or fn.endswith(".ovr.tmp") or fn.endswith(".tif.tmp"):
                        continue
                    path = os.path.join(dirpath, fn)
                    if dry_run:
                        print(f"  [DRY-RUN] Would remove {path}", flush=True)
                    else:
                        try:
                            os.remove(path)
                            total += 1
                        except OSError as e:
                            print(f"  Error removing {path}: {e}", flush=True)


def run_cleanup(
    days: int = 30,
    dry_run: bool = True,
    s1_days: Optional[int] = None,
    s2_days: Optional[int] = None,
    s3_days: Optional[int] = None,
    fusion_days: Optional[int] = None,
    s2_versions: Optional[int] = None,
    prune_outside_search: bool = False,
) -> None:
    """External entry point for cleanup function.

    Args:
        days: Default age in days (fallback when no per-satellite override).
        dry_run: If True, only list files without deleting.
        s1_days: Override days for Sentinel-1 products (None = use `days`).
        s2_days: Override days for Sentinel-2 products (None = use `days`).
        s3_days: Override days for Sentinel-3 products (None = use `days`).
        fusion_days: Override days for fusion products (None = use `days`).
        s2_versions: If set, keep at most this many S2 products per grid tile,
                     discarding older ones regardless of age. When combined with
                     day-based cleanup (CLEANUP_S2_DAYS or fallback CLEANUP_DAYS),
                     products are removed if they exceed EITHER constraint.
        prune_outside_search: If True, also remove products whose footprint bounds
                              fall outside all search boxes defined in .env
                              (S1_BOX, S2_BOX, S3_BOX).
    """
    mode = "DRY-RUN" if dry_run else "LIVE (FORCE)"

    # Allow overriding ROI cleanup days via environment variable
    roi_days = int(os.getenv("CLEANUP_ROI_DAYS", str(days)))

    has_overrides = any(
        x is not None for x in [s1_days, s2_days, s3_days, fusion_days, s2_versions]
    )

    if has_overrides:
        print(
            f"--- Starting cleanup ({mode}) ---",
            flush=True,
        )
        s2_limit = (
            f"{s2_versions}v+{s2_days if s2_days is not None else days}d"
            if s2_versions is not None
            else f"{s2_days if s2_days is not None else days}d"
        )
        print(
            f"--- Cleanup limits: S1={s1_days if s1_days is not None else days}d, "
            f"S2={s2_limit}, "
            f"S3={s3_days if s3_days is not None else days}d, "
            f"FUSION={fusion_days if fusion_days is not None else days}d, "
            f"ROI={roi_days}d ---",
            flush=True,
        )
        outdated_products_list = []
        # S1 and S3: day-based
        for prefix, sat_days in [
            ("S1", s1_days if s1_days is not None else days),
            ("S3", s3_days if s3_days is not None else days),
        ]:
            sat_products = find_outdated_products(sat_days, prefix=prefix)
            if sat_products:
                print(
                    f"  Found {len(sat_products)} outdated {prefix} products "
                    f"(>{sat_days} days).",
                    flush=True,
                )
            outdated_products_list.extend(sat_products)

        # S2: combined version-based AND day-based (OR logic)
        s2_products: List[Dict[str, Any]] = []
        if s2_versions is not None:
            version_excess = find_s2_excess_versions(s2_versions)
            s2_products.extend(version_excess)
            if version_excess:
                print(
                    f"  Found {len(version_excess)} S2 products exceeding "
                    f"tile version limit ({s2_versions}).",
                    flush=True,
                )
        s2_val = s2_days if s2_days is not None else days
        age_outdated = find_outdated_products(s2_val, prefix="S2")
        if age_outdated:
            print(
                f"  Found {len(age_outdated)} S2 products older than {s2_val} days.",
                flush=True,
            )
        # Union: deduplicate by json_path (a product may be both too old and excess)
        seen: set[str] = set()
        for prod in s2_products + age_outdated:
            if prod["json_path"] not in seen:
                seen.add(prod["json_path"])
                outdated_products_list.append(prod)

    else:
        print(
            f"--- Starting cleanup ({mode}) for products older than {days} days ---",
            flush=True,
        )
        if roi_days != days:
            print(f"--- ROI cleanup limit set to {roi_days} days ---", flush=True)
        outdated_products_list = find_outdated_products(days)

    # Outside-search prune: union with existing outdated list
    if prune_outside_search:
        search_boxes = load_search_boxes()
        outside = find_products_outside_search(search_boxes)
        if outside:
            n_before = len(outdated_products_list)
            existing_paths = {p["json_path"] for p in outdated_products_list}
            new_outside = [p for p in outside if p["json_path"] not in existing_paths]
            outdated_products_list.extend(new_outside)
            print(
                f"  Found {len(new_outside)} products outside current search areas "
                f"({len(outside)} total, {n_before} already marked).",
                flush=True,
            )

    # Delta uses S1 retention
    delta_days = s1_days if s1_days is not None else days
    if not outdated_products_list:
        print("No outdated products found.", flush=True)
    else:
        cleanup_outputs(outdated_products_list, dry_run)
        cleanup_roi_outputs(roi_days, dry_run)
        cleanup_delta_outputs(delta_days, dry_run)
        cleanup_source_data(outdated_products_list, dry_run)
        cleanup_logs(outdated_products_list, dry_run)
    # Even when no outdated visual, delta may have orphaned files
    if not outdated_products_list:
        cleanup_delta_outputs(delta_days, dry_run)

        if not dry_run:
            print("\nRebuilding inventory...", flush=True)
            inventory_manager.rebuild_inventory()
        else:
            print("\n[DRY-RUN] Skipping inventory rebuild.", flush=True)

    # S2 filtered priority: remove unfiltered when filtered exists for same scene (post-pipeline)
    # This is independent of age - if TCI-GF exists, TCI is redundant (same for NIRFC, AP).
    # Matches per-ROI AIS GF priority (roi_manager.py AIS_BASE_MAP).
    cleanup_s2_filtered_redundant(dry_run=dry_run)
    if not dry_run:
        # Inventory may have changed due to filtered cleanup; rebuild again (idempotent)
        # Only rebuild if we are not already rebuilding above - but safe to rebuild unconditionally
        # Check if any redundant files were actually removed by scanning again? Just rebuild.
        print("\nRebuilding inventory after S2 filtered cleanup...", flush=True)
        inventory_manager.rebuild_inventory()
    else:
        print("\n[DRY-RUN] Skipping inventory rebuild after S2 filtered cleanup.", flush=True)

    print(
        f"\n--- Cleaning up analytic outputs older than {ANALYTIC_HOURS_CUTOFF} hours ---",
        flush=True,
    )
    outdated_analytic_list = find_outdated_analytic_files(ANALYTIC_HOURS_CUTOFF)
    if outdated_analytic_list:
        cleanup_analytic_outputs(outdated_analytic_list, dry_run)
    else:
        print("No outdated analytic files found.", flush=True)

    print(f"--- Cleanup ({mode}) complete ---", flush=True)


def main() -> None:
    """Main entry point for cleanup script."""
    args = parse_args()
    # Load per-satellite overrides from .env (already loaded via constants.py)
    s1_days: Optional[int] = int(v) if (v := os.getenv("CLEANUP_S1_DAYS")) else None
    s2_days: Optional[int] = int(v) if (v := os.getenv("CLEANUP_S2_DAYS")) else None
    s3_days: Optional[int] = int(v) if (v := os.getenv("CLEANUP_S3_DAYS")) else None
    fusion_days: Optional[int] = (
        int(v) if (v := os.getenv("CLEANUP_FUSION_DAYS")) else None
    )
    s2_versions: Optional[int] = (
        int(v) if (v := os.getenv("CLEANUP_S2_VERSIONS")) else None
    )

    run_cleanup(
        days=args.days,
        dry_run=not args.force,
        s1_days=s1_days,
        s2_days=s2_days,
        s3_days=s3_days,
        fusion_days=fusion_days,
        s2_versions=s2_versions,
        prune_outside_search=args.prune_outside_search,
    )


if __name__ == "__main__":
    main()
