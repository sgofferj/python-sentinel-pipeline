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

import constants as c
from osgeo import gdal
import rasterio as rio
from rasterio.windows import from_bounds, Window
from rasterio.warp import transform_bounds
import numpy as np
from datetime import datetime, timedelta, timezone
import json
import os
import psutil
import time
import threading
import subprocess

gdal.UseExceptions()

# --- CUDA Acceleration ---
try:
    import cupy as cp
    HAS_CUDA = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
    if HAS_CUDA:
        # Check if we can actually touch the device
        cp.cuda.Device(0).use()
        print(f"CUDA Accelerated: {cp.cuda.Device(0).compute_capability}")
except ImportError:
    HAS_CUDA = False

def gpu_calc_idx(ba, bb, alpha_mask):
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
    
    denom = ba_g + bb_g
    idx_g = cp.full_like(ba_g, -1.0, dtype=cp.float32)
    
    valid = (denom != 0) & (mask_g > 0)
    idx_g[valid] = (ba_g[valid] - bb_g[valid]) / denom[valid]
    
    res = cp.asnumpy(idx_g)
    
    del ba_g, bb_g, mask_g, denom, idx_g
    m_pool.free_all_blocks()
    return res

# ----- Performance Logging -----------------------------------------

class PerformanceLogger:
    def __init__(self):
        self.logfile = None
        self.start_time = None
        self.step_start = None
        self.process = psutil.Process(os.getpid())
        self.cpu_samples = []
        self.mem_samples = []
        self.gpu_mem_samples = []
        self.gpu_util_samples = []
        self.monitor_active = False
        self._monitor_thread = None

    def start_run(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        os.makedirs(c.DIRS['S1S2_LOGS'], exist_ok=True)
        self.logfile = os.path.join(c.DIRS['S1S2_LOGS'], f"pipeline_{timestamp}.log")
        self.start_time = time.time()
        with open(self.logfile, "w") as f:
            f.write(f"--- Pipeline Run Started at {datetime.now().isoformat()} ---\n")
            if HAS_CUDA:
                f.write(f"Hardware: NVIDIA GPU (CUDA {cp.cuda.Device(0).compute_capability}) ACTIVE\n")
            else:
                f.write("Hardware: CPU Only\n")
            f.write("Step | Duration(s) | Peak Mem(MB) | Avg CPU% | Peak CPU% | Peak GPU Mem | Peak GPU Util\n")
            f.write("-" * 100 + "\n")
        self._start_monitoring()

    def _start_monitoring(self):
        self.monitor_active = True
        def monitor():
            while self.monitor_active:
                try:
                    # Recursive tracking of self and all children
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
                    
                    if HAS_CUDA:
                        # Simple GPU stats via nvidia-smi query
                        try:
                            res = subprocess.check_output(
                                ["nvidia-smi", "--query-gpu=memory.used,utilization.gpu", "--format=csv,noheader,nounits"], 
                                encoding="utf-8"
                            )
                            mem, util = res.strip().split(',')
                            self.gpu_mem_samples.append(float(mem))
                            self.gpu_util_samples.append(float(util))
                        except:
                            pass
                except:
                    pass
                time.sleep(0.5)
        self._monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._monitor_thread.start()

    def start_step(self, name):
        self.step_name = name
        self.step_start = time.time()
        self.cpu_samples = []
        self.mem_samples = []
        self.gpu_mem_samples = []
        self.gpu_util_samples = []
        print(f"\n>>> Starting Step: {name}")

    def end_step(self):
        duration = time.time() - self.step_start
        peak_mem = max(self.mem_samples) if self.mem_samples else 0
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        peak_cpu = max(self.cpu_samples) if self.cpu_samples else 0
        
        peak_gpu_mem = max(self.gpu_mem_samples) if self.gpu_mem_samples else 0
        peak_gpu_util = max(self.gpu_util_samples) if self.gpu_util_samples else 0
        
        log_line = f"{self.step_name} | {duration:.2f}s | {peak_mem:.2f}MB | {avg_cpu:.1f}% | {peak_cpu:.1f}% | {peak_gpu_mem:.0f}MiB | {peak_gpu_util:.0f}%\n"
        with open(self.logfile, "a") as f:
            f.write(log_line)
        print(f"<<< Step {self.step_name} finished in {duration:.2f}s (Peak Mem: {peak_mem:.2f}MB)")

    def stop_run(self):
        self.monitor_active = False
        total_duration = time.time() - self.start_time
        with open(self.logfile, "a") as f:
            f.write("-" * 100 + "\n")
            f.write(f"--- Pipeline Run Finished at {datetime.now().isoformat()} ---\n")
            f.write(f"Total Duration: {total_duration:.2f}s\n")
        print(f"\n*** Pipeline Run Complete. Total time: {total_duration:.2f}s")

    def log_info(self, msg):
        with open(self.logfile, "a") as f:
            f.write(f"INFO: {msg}\n")

# Global singleton
perf_logger = PerformanceLogger()

# ----- General helper functions ------------------------------------


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0)."""
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "off", "0"):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


def getBoxes(boxes):
    try:
        result = json.loads(boxes)
    except:
        result = [boxes]
    return result


def this_moment():
    result = datetime.now(timezone.utc)
    return result.strftime("%Y-%m-%dT%H:%M:%SZ")


def yesterday(frmt="%Y-%m-%d", string=True):
    yesterday = datetime.now() - timedelta(1)
    if string:
        return yesterday.strftime(frmt)
    return yesterday


def normalize(ds):
    """Normalize an array percentiles"""
    dmin, dmax = np.percentile(ds, (c.S2_PCT_MIN, c.S2_PCT_MAX))
    dsn = (ds.astype(float) - dmin) / (dmax - dmin)
    dsn = np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)
    return dsn


def normalizeminmax(ds):
    """Normalize an array percentiles"""
    dmin, dmax = np.min(ds), np.max(ds)
    dsn = (ds.astype(float) - dmin) / (dmax - dmin)
    dsn = np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)
    return dsn


def scaleOnes(ds):
    """Scale an array from -1 - 1 to 0-255"""
    dmin, dmax = -1, 1
    dsn = (ds.astype(float) - dmin) / (dmax - dmin)
    dsn = np.maximum(np.minimum(dsn * 255, 255), 0).astype(np.uint8)
    return dsn


def get_window(dst_crs, dst_transform, width, height, box):
    west, south, east, north = map(float, box.split(","))
    left, bottom, right, top = transform_bounds(
        rio.CRS.from_epsg(4326),
        dst_crs,
        west,
        south,
        east,
        north,
        densify_pts=21,  # helps with curvy reprojection edges
    )
    win = from_bounds(left, bottom, right, top, transform=dst_transform)
    win = win.round_offsets().round_lengths()
    full = Window(0, 0, width, height)
    win = win.intersection(full)
    return win


def outputExists(name) -> bool:
    """Checks if output file exists and is not empty (min 100KB for safety)."""
    full_path = f"{name}.tif"
    if os.path.exists(full_path):
        size_kb = os.path.getsize(full_path) / 1024
        if size_kb > 100:
            return True
    return False


def writeTiffRGB(ds, profile, name):
    profile.update(
        photometric="RGB",
        count=3,
        dtype=rio.uint8,
        compress="deflate",
        driver="GTiff",
    )
    with rio.open(f"{name}.tif", "w", **profile) as dds:
        dds.write(ds)
        dds.close()


def writeMask(name, profile):
    profile.update(compress="deflate")
    with rio.open(f"{name}.tif", "r+", **profile) as dds:
        mask = np.ones((dds.height, dds.width), dtype=np.uint8)
        mask[dds.read(1) == 0] = 0
        dds.write_mask(mask)
        dds.close()
        del mask
