#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# s1_calibrator.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Sentinel-1 GRD Radiometric Calibration module.
Handles Sigma0 calibration and thermal noise removal using high-concurrency math.
Uses GDAL for robust georeferencing (GCPs).
"""

import gc
import glob
import os
import queue
import threading
from typing import Any, Dict, List, Tuple

import numpy as np
import rasterio as rio
from lxml import etree
from osgeo import gdal
from rasterio.windows import Window
from scipy.interpolate import interp1d

import functions as func

# --- CUDA Acceleration ---
try:
    import cupy as cp

    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False


class S1Calibrator:  # pylint: disable=too-few-public-methods
    """
    S1Calibrator handles radiometric calibration and thermal noise removal
    for Sentinel-1 GRD products using a memory-efficient, multi-threaded approach.
    """

    def __init__(self, safe_path: str) -> None:
        self.safe_path: str = os.path.abspath(safe_path)
        self.manifest_path: str = os.path.join(self.safe_path, "manifest.safe")
        self.annotation_dir: str = os.path.join(self.safe_path, "annotation")
        self.calibration_dir: str = os.path.join(self.annotation_dir, "calibration")

        if not os.path.exists(self.manifest_path):
            raise ValueError(f"manifest.safe not found in: {self.safe_path}")

    def _get_xml_files(self, pol: str) -> Tuple[str, str]:
        """Finds the calibration and noise XML files for a polarization."""
        pol = pol.lower()
        cal_files = glob.glob(
            os.path.join(self.calibration_dir, f"calibration-s1?-iw-grd-{pol}-*.xml")
        )
        noise_files = glob.glob(
            os.path.join(self.calibration_dir, f"noise-s1?-iw-grd-{pol}-*.xml")
        )
        if not cal_files or not noise_files:
            raise FileNotFoundError(f"Could not find XML components for polarization: {pol}")
        return cal_files[0], noise_files[0]

    def _get_subdataset_string(self, polarization: str) -> str:
        """Constructs the GDAL subdataset string for the manifest."""
        return (
            f"SENTINEL1_CALIB:UNCALIB:{self.manifest_path}:"
            f"IW_{polarization.upper()}:AMPLITUDE"
        )

    def _parse_calibration_xml(self, cal_xml: str) -> List[Dict[str, Any]]:
        """Parses the calibration XML to extract Sigma0 vectors."""
        tree = etree.parse(cal_xml)
        root = tree.getroot()
        vectors = []
        for vector_node in root.xpath("//calibrationVector"):
            line = int(vector_node.find("line").text)
            pixel_indices = np.fromstring(vector_node.find("pixel").text, sep=" ", dtype=int)
            sigma_nought = np.fromstring(vector_node.find("sigmaNought").text, sep=" ", dtype=float)
            vectors.append({"line": line, "pixels": pixel_indices, "sigma": sigma_nought})
        return vectors

    def _parse_noise_xml(self, noise_xml: str) -> List[Dict[str, Any]]:
        """Parses the noise XML to extract thermal noise vectors."""
        tree = etree.parse(noise_xml)
        root = tree.getroot()
        vectors = []
        noise_nodes = root.xpath("//noiseVector") or root.xpath("//noiseRangeVector")
        for vector_node in noise_nodes:
            line_node = vector_node.find("line")
            pixel_node = vector_node.find("pixel")
            noise_val_node = vector_node.find("noiseLut") or vector_node.find("noiseRangeLut")
            if (
                line_node is not None
                and pixel_node is not None
                and noise_val_node is not None
            ):
                line = int(line_node.text)
                pixel_indices = np.fromstring(pixel_node.text, sep=" ", dtype=int)
                noise_values = np.fromstring(noise_val_node.text, sep=" ", dtype=float)
                vectors.append({"line": line, "pixels": pixel_indices, "noise": noise_values})
        return vectors

    # pylint: disable=too-many-arguments,too-many-locals,too-many-statements
    def calibrate(
        self,
        polarization: str,
        output_path: str,
        block_size: int = 1024,
        build_ov: bool = True,
        workers: int = 4,
    ) -> None:
        """
        Performs calibration and noise removal.
        Uses GDAL Translate to ensure GCPs are perfectly preserved.
        """
        cal_xml, noise_xml = self._get_xml_files(polarization)
        sds_string = self._get_subdataset_string(polarization)
        cal_vectors = self._parse_calibration_xml(cal_xml)
        noise_vectors = self._parse_noise_xml(noise_xml)

        # 1. Create the output file with proper metadata using GDAL Translate
        # This copies GCPs, CRS, etc. perfectly from the subdataset.
        print(f"Initializing {os.path.basename(output_path)} with source metadata...", flush=True)
        gdal.Translate(
            output_path, 
            sds_string, 
            outputType=gdal.GDT_Float32,
            creationOptions=["TILED=YES", "COMPRESS=DEFLATE", "BIGTIFF=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"]
        )

        with rio.open(output_path, "r+") as dst:
            width: int = dst.width
            height: int = dst.height
            
            # --- INTERPOLATION PREP ---
            lines = np.array([v["line"] for v in cal_vectors])
            
            if HAS_CUDA:
                print("Using CUDA for LUT Interpolation and Calibration.", flush=True)
                grid_vals_sigma = []
                grid_vals_noise = []
                for i, v in enumerate(cal_vectors):
                    f_s = interp1d(v["pixels"], v["sigma"], kind="linear", fill_value="extrapolate")
                    grid_vals_sigma.append(f_s(np.arange(width)))
                    nv = next((n for n in noise_vectors if n["line"] == v["line"]), noise_vectors[min(i, len(noise_vectors)-1)])
                    f_n = interp1d(nv["pixels"], nv["noise"], kind="linear", fill_value="extrapolate")
                    grid_vals_noise.append(f_n(np.arange(width)))
                
                g_lines = cp.array(lines, dtype=cp.float32)
                g_lut_sigma = cp.array(grid_vals_sigma, dtype=cp.float32)
                g_lut_noise = cp.array(grid_vals_noise, dtype=cp.float32)
                del grid_vals_sigma, grid_vals_noise
            else:
                print("GPU Unavailble: Falling back to CPU for LUT Interpolation.", flush=True)
                grid_values_s = []
                grid_values_n = []
                for v in cal_vectors:
                    f = interp1d(v["pixels"], v["sigma"], kind="linear", fill_value="extrapolate")
                    grid_values_s.append(f(np.arange(width)))
                for v in noise_vectors:
                    f = interp1d(v["pixels"], v["noise"], kind="linear", fill_value="extrapolate")
                    grid_values_n.append(f(np.arange(width)))
                
                cal_func = interp1d(lines, np.array(grid_values_s), axis=0, kind="linear", fill_value="extrapolate")
                noise_func = interp1d(np.array([v["line"] for v in noise_vectors]), np.array(grid_values_n), axis=0, kind="linear", fill_value="extrapolate")
                del grid_values_s, grid_values_n

            read_queue: queue.Queue = queue.Queue(maxsize=2)
            write_queue: queue.Queue = queue.Queue(maxsize=2)

            def reader_thread() -> None:
                try:
                    # Open source subdataset for reading original DN
                    with rio.open(sds_string) as t_src:
                        for row_off in range(0, height, block_size):
                            rows = min(block_size, height - row_off)
                            window = Window(0, row_off, width, rows)
                            dn = t_src.read(1, window=window).astype(np.float32)
                            
                            if not HAS_CUDA:
                                current_lines = np.arange(row_off, row_off + rows)
                                cal_block = cal_func(current_lines).astype(np.float32)
                                noise_block = noise_func(current_lines).astype(np.float32)
                                read_queue.put((window, dn, cal_block, noise_block), timeout=120)
                            else:
                                read_queue.put((window, dn, row_off, rows), timeout=120)
                        read_queue.put(None, timeout=120)
                except Exception as e:
                    print(f"\nCRITICAL: Reader thread failed: {e}", flush=True)
                    read_queue.put(None)

            def writer_thread(dst_handle: Any) -> None:
                try:
                    while True:
                        item = write_queue.get(timeout=120)
                        if item is None:
                            write_queue.task_done()
                            break
                        window, sigma0 = item
                        dst_handle.write(sigma0, 1, window=window)
                        write_queue.task_done()
                except Exception as e:
                    print(f"\nCRITICAL: Writer thread failed: {e}", flush=True)

            t_read = threading.Thread(target=reader_thread, daemon=True)
            t_write = threading.Thread(target=writer_thread, args=(dst,), daemon=True)
            t_read.start(); t_write.start()

            while True:
                try:
                    item = read_queue.get(timeout=120)
                except queue.Empty:
                    print("\nCRITICAL: Reader thread timed out (Deadlock?).", flush=True)
                    break
                    
                if item is None:
                    write_queue.put(None, timeout=120); read_queue.task_done()
                    break
                
                try:
                    if HAS_CUDA:
                        window, dn, row_off, rows = item
                        m_pool = cp.get_default_memory_pool()
                        g_dn = cp.array(dn)
                        g_valid = g_dn > 0
                        target_lines = cp.arange(row_off, row_off + rows, dtype=cp.float32)
                        
                        def gpu_interp_2d(lut):
                            idx = cp.searchsorted(g_lines, target_lines) - 1
                            idx = cp.clip(idx, 0, len(lines) - 2)
                            x0 = g_lines[idx]; x1 = g_lines[idx+1]
                            weight = (target_lines - x0) / (x1 - x0)
                            y0 = lut[idx]; y1 = lut[idx+1]
                            return y0 + weight[:, cp.newaxis] * (y1 - y0)

                        g_cal = gpu_interp_2d(g_lut_sigma)
                        g_noise = gpu_interp_2d(g_lut_noise)
                        
                        g_sigma0 = cp.zeros_like(g_dn)
                        # Ensure valid pixels are NEVER absolute 0 to preserve nodata=0 meaning
                        g_sigma0[g_valid] = cp.maximum((cp.square(g_dn[g_valid]) - g_noise[g_valid]) / cp.square(g_cal[g_valid]), 1e-9)
                        
                        sigma0 = cp.asnumpy(g_sigma0)
                        del g_dn, g_valid, g_cal, g_noise, g_sigma0, target_lines
                        m_pool.free_all_blocks()
                    else:
                        window, dn, cal_block, noise_block = item
                        valid_mask = dn > 0
                        sigma0 = np.zeros_like(dn)
                        sigma0[valid_mask] = np.maximum((np.square(dn[valid_mask]) - noise_block[valid_mask]) / np.square(cal_block[valid_mask]), 1e-9)

                    write_queue.put((window, sigma0), timeout=120)
                except Exception as e:
                    print(f"\nCRITICAL: Calibration loop failed: {e}", flush=True)
                    break
                
                print(f"Processed strip starting at line {window.row_off}/{height}", end="\r", flush=True)
                read_queue.task_done()

            t_read.join(); t_write.join()

            if build_ov:
                func.perf_logger.start_step(f"S1 Internal Overviews: {os.path.basename(output_path)}")
                dst.build_overviews([2, 4, 8, 16, 32, 64], rio.enums.Resampling.average)
                func.perf_logger.end_step()

        gc.collect()
        print(f"\nCalibration complete: {output_path}", flush=True)
