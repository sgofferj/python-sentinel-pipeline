#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# notify_roi.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Resend the latest Apprise notification for a given ROI.

Scans the ROI crop directory for the most recent product for each product type
configured for the ROI, rebuilds the notification message from the sidecar
metadata, and re-sends it via Apprise.

Usage:
    python notify_roi.py My_ROI              # resend for a single ROI
    python notify_roi.py --list              # list ROIs with recent crops
    python notify_roi.py --dry-run My_ROI    # validate without sending
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image

import functions as func
import notifications as notify

# Allow large TIFFs (ROI crops can exceed PIL's default 89Mpx limit)
from PIL import ImageFile
ImageFile.MAX_IMAGE_PIXELS = None

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ROI_CONFIG_PATH = "roi_config.json"
VIS_ROI_DIR = func.c.DIRS["VIS_ROI"]


def load_roi_config() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Load ROI definitions from roi_config.json."""
    if not os.path.exists(ROI_CONFIG_PATH):
        print(f"ERROR: ROI config not found: {ROI_CONFIG_PATH}", flush=True)
        sys.exit(1)

    with open(ROI_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    config = cfg.get("config", {})
    rois = cfg.get("rois", [])
    return config, rois


def find_roi(
    rois: List[Dict[str, Any]], name: str
) -> Optional[Dict[str, Any]]:
    """Find an ROI definition by name (case-insensitive)."""
    name_lower = name.lower().replace("_", " ")
    for roi in rois:
        roi_name = roi.get("name", "").lower().replace("_", " ")
        if roi_name == name_lower:
            return roi
    return None


def list_recent_rois(rois: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return ROI definitions that have at least one crop in the output dir."""
    result = []
    for roi in rois:
        roi_name = func.resolve_env_variable(roi.get("name", ""))
        pattern = re.escape(roi_name) + r"_(.+)\.tif$"
        for fname in os.listdir(VIS_ROI_DIR):
            if re.match(pattern, fname):
                result.append(roi)
                break
    return result


# ---------------------------------------------------------------------------
# ROI crop scanning
# ---------------------------------------------------------------------------
ROI_FILE_RE = re.compile(
    r"^(?P<roi>.+?)_(?P<product>[A-Z][A-Z0-9_-]+)_"
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{6}Z)\.tif$"
)


def scan_roi_crops(roi_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scan the ROI output dir for crops belonging to *roi_name*.

    Returns {product_type: [{path, sidecar, acq_time, render_time}, ...]}
    sorted by acquisition time (newest first per product).
    """
    crops: Dict[str, List[Dict[str, Any]]] = {}

    if not os.path.isdir(VIS_ROI_DIR):
        print(f"ERROR: ROI directory not found: {VIS_ROI_DIR}", flush=True)
        sys.exit(1)

    for fname in os.listdir(VIS_ROI_DIR):
        m = ROI_FILE_RE.match(fname)
        if not m:
            continue
        if m.group("roi") != roi_name:
            continue

        product = m.group("product")
        base = fname[: -len(".tif")]

        tif_path = os.path.join(VIS_ROI_DIR, fname)
        json_path = os.path.join(VIS_ROI_DIR, base + ".json")

        # Load sidecar
        sidecar = {}
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    sidecar = json.load(f)
            except Exception as e:
                print(f"  Warning: failed to read sidecar {json_path}: {e}", flush=True)

        acq_time = sidecar.get("acquisition_time", "")
        render_time = sidecar.get("render_time", "")

        def _parse_ts(ts: str) -> datetime:
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        crops.setdefault(product, []).append(
            {
                "path": tif_path,
                "json_path": json_path,
                "sidecar": sidecar,
                "acquisition_time": acq_time,
                "render_time": render_time,
                "_acq_dt": _parse_ts(acq_time),
                "_render_dt": _parse_ts(render_time),
            }
        )

    # Sort each product group by acquisition time descending
    for product in crops:
        crops[product].sort(key=lambda c: c["_acq_dt"], reverse=True)

    return crops


# ---------------------------------------------------------------------------
# Notification helpers
# ---------------------------------------------------------------------------
PRODUCT_NAMES: Dict[str, str] = {
    "TCI": "True Color",
    "TCI-GF": "True Color (Guided Filter)",
    "RATIO": "Radar Ratio (VV/VH)",
    "RATIOVVVH": "Radar Ratio (VV/VH)",
    "VV": "Radar VV",
    "VH": "Radar VH",
    "NIRFC": "False Color (NIR)",
    "NIRFC-GF": "False Color (Guided Filter)",
    "NDVI": "NDVI (Vegetation Index)",
    "NDRE": "NDRE (Plant Stress)",
    "NDBI": "NDBI (Urban/Built-up)",
    "NBR": "NBR (Burn Index)",
    "CAMO": "Camo Detection",
    "AP": "Atmospheric Penetration (SWIR)",
    "LIFE-MACHINE": "Life/Machine Separation",
    "RADAR-BURN": "Radar Burn Detection",
    "BT": "Brightness Temperature",
    "FIRE": "Thermal Anomaly",
}


def get_human_name(product_type: str) -> str:
    """Convert a product type string to a human-readable name."""
    # Already stripped? try whole string first
    pt = product_type.upper()
    if pt in PRODUCT_NAMES:
        return PRODUCT_NAMES[pt]
    # Strip sensor prefix (S2-TCI-GF → TCI-GF)
    rest = product_type.split("-", 1)[1] if "-" in product_type else product_type
    parts = rest.split("-")
    for i in range(len(parts)):
        candidate = "-".join(parts[i:]).upper()
        if candidate in PRODUCT_NAMES:
            return PRODUCT_NAMES[candidate]
    return rest


def constellation_from_satellite(sat: str) -> str:
    """Derive constellation label from satellite ID (S1A → Sentinel-1, etc.)."""
    s = sat.upper().strip()
    if s.startswith("S1"):
        return "Sentinel-1"
    if s.startswith("S2"):
        return "Sentinel-2"
    if s.startswith("S3"):
        return "Sentinel-3"
    return sat


def ensure_full_jpeg(
    tif_path: str, social_base: str
) -> Optional[str]:
    """
    Ensure a full-size JPEG exists for *tif_path*.
    Returns the JPEG path, or None on failure.
    """
    jpg_path = social_base + "_full.jpg"
    if os.path.exists(jpg_path):
        # Already exists — verify it's newer than the TIFF
        tif_mtime = os.path.getmtime(tif_path)
        jpg_mtime = os.path.getmtime(jpg_path)
        if jpg_mtime >= tif_mtime:
            return jpg_path
        print(f"  JPEG stale (TIFF is newer), regenerating…", flush=True)

    # (Re-)generate from TIFF
    try:
        with Image.open(tif_path) as img:
            rgb = img if img.mode == "RGB" else img.convert("RGB")
            rgb.save(jpg_path, "JPEG", quality=90, optimize=True)
        print(f"  Created JPEG: {jpg_path}", flush=True)
        return jpg_path
    except Exception as e:
        print(f"  ERROR creating JPEG from {tif_path}: {e}", flush=True)
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resend Apprise notification for an ROI from the latest crop."
    )
    parser.add_argument(
        "roi_name",
        nargs="?",
        help="ROI name (case-insensitive, e.g. My_ROI)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate everything but do NOT send the notification",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List ROIs that have crops in the output directory",
    )
    args = parser.parse_args()

    # Initialise performance logger so log_info() writes to today's log
    func.perf_logger.start_run()

    config, rois = load_roi_config()

    # --- --list mode -------------------------------------------------------
    if args.list:
        recent = list_recent_rois(rois)
        if not recent:
            print("No ROIs with recent crops found.", flush=True)
        else:
            print(f"ROIs with crops in {VIS_ROI_DIR}:", flush=True)
            for roi in recent:
                name = func.resolve_env_variable(roi.get("name", ""))
                products = ", ".join(roi.get("products", []))
                url = "configured" if roi.get("apprise_url") else "(none)"
                print(f"  {name}  [{products}]  apprise: {url}", flush=True)
        func.perf_logger.stop_run()
        return

    # --- ROI name required ------------------------------------------------
    if not args.roi_name:
        parser.print_help()
        sys.exit(1)

    roi_name = args.roi_name

    # Resolve ROI config
    roi_def = find_roi(rois, roi_name)
    if roi_def is None:
        print(
            f"ERROR: ROI '{roi_name}' not found in {ROI_CONFIG_PATH}.",
            flush=True,
        )
        print("Use --list to see available ROIs.", flush=True)
        sys.exit(1)

    # Resolve env vars in the ROI name (e.g. ${VAR})
    resolved_name = func.resolve_env_variable(roi_def.get("name", ""))
    apprise_url = roi_def.get("apprise_url", "")
    desired_products = roi_def.get("products", [])

    print(f"ROI:        {resolved_name}", flush=True)
    print(f"Products:   {', '.join(desired_products)}", flush=True)
    print(f"Apprise:    {'configured' if apprise_url else 'NONE — notification will be skipped'}", flush=True)
    print(f"Dry-run:    {'yes' if args.dry_run else 'no'}", flush=True)
    print(flush=True)

    # Scan crops
    crops_by_product = scan_roi_crops(resolved_name)

    if not crops_by_product:
        print(f"No crops found for '{resolved_name}' in {VIS_ROI_DIR}.", flush=True)
        sys.exit(1)

    # For each configured product, pick the latest crop and notify
    notified_count = 0
    skipped_count = 0
    error_count = 0

    for product in desired_products:
        # Normalise for matching (TCI-GF vs S2-TCI-GF vs TCI_GF etc.)
        product_up = product.upper().replace("-", "_").replace("RATIOVVVH", "RATIO")

        # Find the matching crop group
        matched_crops = None
        for crop_product, crop_list in crops_by_product.items():
            cp_up = crop_product.upper().replace("-", "_").replace("RATIOVVVH", "RATIO")
            if product_up in cp_up or cp_up in product_up:
                matched_crops = crop_list
                break

        if not matched_crops:
            print(
                f"  [{product}] No matching crop found — skipping.",
                flush=True,
            )
            func.perf_logger.log_info(
                f"notify_roi: {resolved_name} {product}: no crop found"
            )
            skipped_count += 1
            continue

        latest = matched_crops[0]  # newest by acquisition_time
        tif_path = latest["path"]
        sidecar = latest["sidecar"]
        acq_time = sidecar.get("acquisition_time", "unknown")
        sat = sidecar.get("satellite", "?")
        constellation = constellation_from_satellite(sat)

        human_prod = get_human_name(product)

        print(
            f"  [{product}] Latest: {os.path.basename(tif_path)}",
            flush=True,
        )
        print(f"             Acquired: {acq_time}  Satellite: {sat}", flush=True)

        # Build social_base path for JPEG
        iso_clean = acq_time.replace(":", "") if acq_time else "unknown"
        social_base = os.path.join(
            VIS_ROI_DIR,
            f"{resolved_name}_{product}_{iso_clean}_social",
        )

        # Ensure the full-size JPEG exists
        full_jpg = ensure_full_jpeg(tif_path, social_base)
        if not full_jpg:
            print(f"  [{product}] ERROR: cannot create JPEG — skipping notification.", flush=True)
            func.perf_logger.log_info(
                f"notify_roi: {resolved_name} {product}: JPEG creation failed, skipped"
            )
            error_count += 1
            continue

        # Build message (same format as roi_manager.py)
        msg = (
            f"New {human_prod} image for ROI {resolved_name}\n"
            f"Acquired: {acq_time}\n"
            f"Satellite: {constellation}"
        )
        title = f"ROI Update: {resolved_name}"

        if args.dry_run:
            print(f"  [{product}] DRY-RUN — would send:", flush=True)
            print(f"           Title: {title}", flush=True)
            print(f"           Apprise: {'configured' if apprise_url else '(none)'}", flush=True)
            print(f"           File:  {full_jpg}", flush=True)
            print(f"           Body:  {msg.replace(chr(10), ' | ')}", flush=True)
            notified_count += 1
            continue

        if not apprise_url:
            print(
                f"  [{product}] No apprise_url configured — skipping notification.",
                flush=True,
            )
            func.perf_logger.log_info(
                f"notify_roi: {resolved_name} {product}: no apprise_url"
            )
            skipped_count += 1
            continue

        # Send notification
        print(f"  [{product}] Sending notification…", flush=True)
        func.perf_logger.log_info(
            f"notify_roi: sending {resolved_name} {product} "
            f"(acq={acq_time}, sat={sat})"
        )

        notify.send_notification(
            message=msg,
            title=title,
            urls=apprise_url,
            attachment=full_jpg,
        )

        print(f"  [{product}] Done.", flush=True)
        notified_count += 1

    # Summary
    print(flush=True)
    print(
        f"Summary: {notified_count} notified, {skipped_count} skipped, "
        f"{error_count} errors.",
        flush=True,
    )
    func.perf_logger.log_info(
        f"notify_roi: {resolved_name} — {notified_count} sent, "
        f"{skipped_count} skipped, {error_count} errors"
    )

    func.perf_logger.stop_run()


if __name__ == "__main__":
    main()
