#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# functions.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
General utility functions and Performance Logging for the Sentinel pipeline.
"""

import json
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import numpy as np
import psutil
import rasterio as rio
from osgeo import gdal
from rasterio.warp import transform_bounds
from rasterio.windows import Window, from_bounds

import constants as c

gdal.UseExceptions()

# --- CUDA Acceleration ---
try:
    import cupy as cp

    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
    if HAS_CUDA:
        # Check if we can actually touch the device
        cp.cuda.Device(0).use()
        print(f"CUDA Accelerated: {cp.cuda.Device(0).compute_capability}", flush=True)
except ImportError:
    HAS_CUDA = False


def gpu_calc_idx(ba: np.ndarray, bb: np.ndarray, alpha_mask: np.ndarray) -> np.ndarray:
    """
    Calculates a normalized difference index using CUDA if available.
    (ba - bb) / (ba + bb)
    """
    if not HAS_CUDA:
        denom = ba.astype(float) + bb.astype(float)
        idx = np.full_like(ba, -1.0, dtype=np.float32)
        m = (denom != 0) & (alpha_mask > 0)
        idx[m] = (ba[m].astype(float) - bb[m].astype(float)) / denom[m]
        return idx

    # GPU Path
    m_pool = cp.get_default_memory_pool()
    ba_g = cp.array(ba, dtype=cp.float32)
    bb_g = cp.array(bb, dtype=cp.float32)
    mask_g = cp.array(alpha_mask, dtype=cp.uint8)

    denom_g = ba_g + bb_g
    idx_g = cp.full_like(ba_g, -1.0, dtype=cp.float32)

    valid = (denom_g != 0) & (mask_g > 0)
    idx_g[valid] = (ba_g[valid] - bb_g[valid]) / denom_g[valid]

    res: np.ndarray = cp.asnumpy(idx_g)

    del ba_g, bb_g, mask_g, denom_g, idx_g
    m_pool.free_all_blocks()
    return res


# ----- Performance Logging -----------------------------------------


class PerformanceLogger:
    """
    Tracks and logs system performance metrics per pipeline step.
    Supports recursive child process tracking and optional GPU monitoring.
    """

    def __init__(self) -> None:
        self.logfile: Optional[str] = None
        self.start_time: float = 0.0
        self.step_start: float = 0.0
        self.step_name: str = ""
        self.process = psutil.Process(os.getpid())
        self.cpu_samples: List[float] = []
        self.mem_samples: List[float] = []
        self.gpu_mem_samples: List[float] = []
        self.gpu_util_samples: List[float] = []
        self.monitor_active: bool = False
        self.use_gpu_step: bool = False
        self._monitor_thread: Optional[threading.Thread] = None

    def start_run(self) -> None:
        """Initializes a new pipeline run log."""
        timestamp: str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        os.makedirs(c.DIRS["S1S2_LOGS"], exist_ok=True)
        self.logfile = os.path.join(c.DIRS["S1S2_LOGS"], f"pipeline_{timestamp}.log")
        self.start_time = time.time()
        with open(self.logfile, "w", encoding="utf-8") as f:
            f.write(f"--- Pipeline Run Started at {datetime.now().isoformat()} ---\n")
            if HAS_CUDA:
                f.write(
                    f"Hardware: NVIDIA GPU (CUDA {cp.cuda.Device(0).compute_capability}) ACTIVE\n"
                )
            else:
                f.write("Hardware: CPU Only\n")
            header = (
                "Step | Duration(s) | Peak Mem(MB) | Avg CPU% | Peak CPU% "
                "| Peak GPU Mem | Peak GPU Util\n"
            )
            f.write(header)
            f.write("-" * 100 + "\n")
            f.flush()
        self._start_monitoring()

    def _start_monitoring(self) -> None:
        self.monitor_active = True

        def monitor() -> None:
            while self.monitor_active:
                try:
                    # RECURSIVE TRACKING
                    procs = [self.process] + self.process.children(recursive=True)
                    total_cpu = 0.0
                    total_rss = 0.0
                    for p in procs:
                        try:
                            total_cpu += p.cpu_percent()
                            total_rss += p.memory_info().rss
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue
                    self.cpu_samples.append(total_cpu)
                    self.mem_samples.append(total_rss / (1024 * 1024))

                    if HAS_CUDA and self.use_gpu_step:
                        try:
                            res = subprocess.check_output(
                                [
                                    "nvidia-smi",
                                    "--query-gpu=memory.used,utilization.gpu",
                                    "--format=csv,noheader,nounits",
                                ],
                                encoding="utf-8",
                            )
                            mem, util = res.strip().split(",")
                            self.gpu_mem_samples.append(float(mem))
                            self.gpu_util_samples.append(float(util))
                        except Exception:  # pylint: disable=broad-exception-caught
                            pass
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
                time.sleep(0.5)

        self._monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._monitor_thread.start()

    def start_step(self, name: str, use_gpu: bool = False) -> None:
        """Starts tracking a new pipeline step."""
        self.step_name = name
        self.step_start = time.time()
        self.use_gpu_step = use_gpu
        self.cpu_samples = []
        self.mem_samples = []
        self.gpu_mem_samples = []
        self.gpu_util_samples = []
        print(f"\n>>> Starting Step: {name}", flush=True)

    def end_step(self) -> None:
        """Finalizes tracking for the current step and writes to log."""
        if not self.logfile:
            return
        duration: float = time.time() - self.step_start
        peak_mem: float = max(self.mem_samples) if self.mem_samples else 0
        avg_cpu: float = (
            sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        )
        peak_cpu: float = max(self.cpu_samples) if self.cpu_samples else 0

        gpu_part: str = ""
        if self.use_gpu_step and HAS_CUDA:
            peak_gpu_mem: float = (
                max(self.gpu_mem_samples) if self.gpu_mem_samples else 0
            )
            peak_gpu_util: float = (
                max(self.gpu_util_samples) if self.gpu_util_samples else 0
            )
            gpu_part = f" | {peak_gpu_mem:.0f}MiB | {peak_gpu_util:.0f}%"
        elif HAS_CUDA:
            gpu_part = " | N/A | N/A"

        log_line: str = (
            f"{self.step_name} | {duration:.2f}s | {peak_mem:.2f}MB | "
            f"{avg_cpu:.1f}% | {peak_cpu:.1f}%{gpu_part}\n"
        )
        with open(self.logfile, "a", encoding="utf-8") as f:
            f.write(log_line)
            f.flush()
        print(
            f"<<< Step {self.step_name} finished in {duration:.2f}s (Peak Mem: {peak_mem:.2f}MB)",
            flush=True,
        )

    def stop_run(self) -> None:
        """Finalizes the entire pipeline run."""
        if not self.logfile:
            return
        self.monitor_active = False
        total_duration: float = time.time() - self.start_time
        with open(self.logfile, "a", encoding="utf-8") as f:
            f.write("-" * 100 + "\n")
            f.write(f"--- Pipeline Run Finished at {datetime.now().isoformat()} ---\n")
            f.write(f"Total Duration: {total_duration:.2f}s\n")
            f.flush()
        print(
            f"\n*** Pipeline Run Complete. Total time: {total_duration:.2f}s",
            flush=True,
        )

    def log_info(self, msg: str) -> None:
        """Writes an informational message to the log."""
        if not self.logfile:
            return
        with open(self.logfile, "a", encoding="utf-8") as f:
            f.write(f"INFO: {msg}\n")
            f.flush()


# Global singleton
perf_logger = PerformanceLogger()

# ----- General helper functions ------------------------------------


def strtobool(val: str) -> int:
    """Convert a string representation of truth to true (1) or false (0)."""
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    if val in ("n", "no", "f", "false", "off", "0"):
        return 0
    raise ValueError(f"invalid truth value {val!r}")


def get_boxes(boxes: Optional[str]) -> List[str]:
    """Parses a box string, a semicolon-separated list of boxes, or a JSON list into a Python list."""
    if not boxes:
        return []
    
    # Try JSON parsing first
    try:
        result = json.loads(boxes)
        if isinstance(result, list):
            return [str(x).strip() for x in result]
        return [str(result).strip()]
    except (json.JSONDecodeError, TypeError):
        # If not JSON, try splitting by semicolon
        if ";" in boxes:
            return [x.strip() for x in boxes.split(";")]
        # Otherwise treat as a single box
        return [boxes.strip()]


def this_moment() -> str:
    """Returns current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def yesterday(frmt: str = "%Y-%m-%d", is_string: bool = True) -> Union[str, datetime]:
    """Returns yesterday's date."""
    yest = datetime.now() - timedelta(1)
    return yest.strftime(frmt) if is_string else yest


def normalize(ds_arr: np.ndarray) -> np.ndarray:
    """Normalize an array using 2nd and 98th percentiles."""
    dmin, dmax = np.percentile(ds_arr, (c.S2_PCT_MIN, c.S2_PCT_MAX))
    dsn = (ds_arr.astype(float) - dmin) / (dmax - dmin)
    return np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)


def normalize_min_max(ds_arr: np.ndarray) -> np.ndarray:
    """Normalize an array using absolute min and max."""
    dmin, dmax = np.min(ds_arr), np.max(ds_arr)
    dsn = (ds_arr.astype(float) - dmin) / (dmax - dmin)
    return np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)


def scale_ones(ds_arr: np.ndarray) -> np.ndarray:
    """Scale an array from [-1, 1] to [0, 255]."""
    dmin, dmax = -1, 1
    dsn = (ds_arr.astype(float) - dmin) / (dmax - dmin)
    return np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)


def get_window(
    dst_crs: rio.crs.CRS,
    dst_transform: rio.transform.Affine,
    width: int,
    height: float,
    box: str,
) -> Window:
    """Calculates a pixel window from a bounding box string."""
    west, south, east, north = map(float, box.split(","))
    left, bottom, right, top = transform_bounds(
        rio.CRS.from_epsg(4326), dst_crs, west, south, east, north, densify_pts=21
    )
    win = (
        from_bounds(left, bottom, right, top, transform=dst_transform)
        .round_offsets()
        .round_lengths()
    )
    return win.intersection(Window(0, 0, width, height))


def output_exists(name: str) -> bool:
    """Checks if output file exists and is not empty (min 100KB for safety)."""
    full_path: str = f"{name}.tif"
    if os.path.exists(full_path):
        if (os.path.getsize(full_path) / 1024) > 100:
            return True
    return False


def write_tiff_rgb(ds_arr: np.ndarray, profile: Dict[str, Any], name: str) -> None:
    """Writes an RGB array to a GeoTIFF."""
    profile.update(
        photometric="RGB", count=3, dtype=rio.uint8, compress="deflate", driver="GTiff"
    )
    with rio.open(f"{name}.tif", "w", **profile) as dds:
        dds.write(ds_arr)
        dds.close()


def write_mask(name: str, profile: Dict[str, Any]) -> None:
    """Generates and writes an alpha mask based on zero-pixel values."""
    profile.update(compress="deflate")
    with rio.open(f"{name}.tif", "r+", **profile) as dds:
        mask = np.ones((dds.height, dds.width), dtype=np.uint8)
        mask[dds.read(1) == 0] = 0
        dds.write_mask(mask)
        dds.close()
