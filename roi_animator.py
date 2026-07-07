#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# roi_animator.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
ROI Animator: Generates ordered PNG sequences from ROI crops for animation/timelapses.
"""

import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from PIL import Image
import constants as c


def parse_args() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate PNG sequences from ROI crops for animations."
    )
    parser.add_argument(
        "--roi",
        "-r",
        required=True,
        help="Name of the ROI (as defined in roi_config.json)",
    )
    parser.add_argument(
        "--product", "-p", required=True, help="Product type (e.g., TCI, RATIO, NDVI)"
    )
    parser.add_argument(
        "--start",
        "-s",
        help="Start date/time (YYYY-MM-DD or ISO timestamp). Defaults to beginning of time.",
    )
    parser.add_argument(
        "--end",
        "-e",
        help="End date/time (YYYY-MM-DD or ISO timestamp). Defaults to now.",
    )
    parser.add_argument(
        "--orbit-direction",
        "-od",
        choices=["ASCENDING", "DESCENDING"],
        help="Optional: Filter S1 products by orbit direction",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        help="Optional: Override base output directory for animations",
    )
    return parser.parse_args()


def parse_time(time_str: Optional[str], default: datetime) -> datetime:
    """Parses various time formats into a UTC datetime object."""
    if not time_str:
        return default

    # Try ISO format
    try:
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass

    # Try YYYY-MM-DD
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    print(f"Warning: Could not parse time '{time_str}', using default.")
    return default


def find_matching_crops(
    roi_name: str,
    product_type: str,
    start_dt: datetime,
    end_dt: datetime,
    orbit_dir: Optional[str],
) -> List[Dict[str, Any]]:
    """Scans the ROI directory for matching crops and returns a sorted list."""
    roi_dir = c.DIRS["VIS_ROI"]
    if not os.path.exists(roi_dir):
        print(f"Error: ROI directory {roi_dir} not found.")
        return []

    matches = []
    # Normalize product type for comparison (e.g. RATIOVVVH -> RATIO)
    p_norm = product_type.upper().replace("RATIOVVVH", "RATIO")

    for filename in os.listdir(roi_dir):
        if not filename.endswith(".json"):
            continue

        json_path = os.path.join(roi_dir, filename)
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                meta = json.load(f)

                prod_id = meta.get("product", "")
                # Format: ROI-Name-Suffix
                if not prod_id.startswith("ROI-"):
                    continue

                if roi_name not in prod_id:
                    continue

                # Check Product Suffix
                legend_id = meta.get("legend_id", "").upper()
                if p_norm not in legend_id:
                    continue

                # Check Time Range
                acq_time_str = meta.get("acquisition_time")
                if not acq_time_str:
                    continue
                acq_dt = datetime.fromisoformat(acq_time_str.replace("Z", "+00:00"))

                if not start_dt <= acq_dt <= end_dt:
                    continue

                # Check Orbit Direction (if applicable)
                if orbit_dir:
                    meta_od = meta.get("orbit_direction")
                    if meta_od and meta_od.upper() != orbit_dir.upper():
                        continue

                tif_path = json_path.replace(".json", ".tif")
                if os.path.exists(tif_path):
                    matches.append({"tif": tif_path, "acq_time": acq_dt, "meta": meta})
        except (json.JSONDecodeError, IOError, ValueError) as e:
            print(f"Warning: Error reading {filename}: {e}")

    # Sort by acquisition time
    matches.sort(key=lambda x: x["acq_time"])
    return matches


def main() -> None:
    """Main execution function for generating the PNG sequence."""
    args = parse_args()

    start_dt = parse_time(args.start, datetime(2000, 1, 1, tzinfo=timezone.utc))
    end_dt = parse_time(args.end, datetime.now(timezone.utc))

    print(f"Searching for {args.roi} {args.product} crops...")
    if args.orbit_direction:
        print(f"Filter: Orbit Direction = {args.orbit_direction}")

    matches = find_matching_crops(
        args.roi, args.product, start_dt, end_dt, args.orbit_direction
    )

    if not matches:
        print("No matching crops found.")
        return

    print(f"Found {len(matches)} matching images.")

    # Create animation directory name
    s_str = start_dt.strftime("%Y%m%d")
    e_str = end_dt.strftime("%Y%m%d")
    anim_name = f"{args.roi}_{args.product}_{s_str}_{e_str}"
    if args.orbit_direction:
        anim_name += f"_{args.orbit_direction}"

    # Sanitize for directory name
    anim_name = re.sub(r"[^a-zA-Z0-9_\-]", "_", anim_name)

    base_out = args.output_dir or os.path.join(c.DIRS["OUT"], "animations")
    target_dir = os.path.join(base_out, anim_name)

    if not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)
        print(f"Created directory: {target_dir}")
    else:
        print(f"Output directory already exists: {target_dir}")

    print("Converting to PNG series...")
    for i, match in enumerate(matches, 1):
        tif_path = match["tif"]
        png_name = f"{i:05d}.png"
        png_path = os.path.join(target_dir, png_name)

        try:
            with Image.open(tif_path) as img:
                rgb_img = img.convert("RGB")
                rgb_img.save(png_path, "PNG")
                print(
                    f" [{i}/{len(matches)}] Saved {png_name} (from {os.path.basename(tif_path)})"
                )
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error converting {tif_path}: {e}")

    print(f"\nSuccess! PNG sequence generated in: {target_dir}")
    print("Example ffmpeg command:")
    print(
        f"ffmpeg -framerate 5 -i {target_dir}/%05d.png -c:v libx264 -pix_fmt yuv420p animation.mp4"
    )


if __name__ == "__main__":
    main()
