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
"""

import gc
import glob
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import rasterio as rio
from lxml import etree
from rasterio.windows import Window
from scipy.interpolate import interp1d

import functions as func

# --- CUDA Acceleration ---
try:
    import cupy as cp

    HAS_CUDA: bool = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False


class S1Calibrator:
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

    def _interpolate_vectors(self, vectors: List[Dict[str, Any]], total_pixels: int, key: str) -> Any:
        """Interpolates sparse vectors to a full-image grid."""
        if not vectors:
            raise ValueError(f"No vectors found for key: {key}")
        lines = np.array([v["line"] for v in vectors])
        grid_values = []
        for v in vectors:
            f = interp1d(v["pixels"], v[key], kind="linear", fill_value="extrapolate")
            grid_values.append(f(np.arange(total_pixels)))
        full_lut_func = interp1d(
            lines, np.array(grid_values), axis=0, kind="linear", fill_value="extrapolate"
        )
        del grid_values
        return full_lut_func

    def calibrate(
        self,
        polarization: str,
        output_path: str,
        block_size: int = 1024,
        build_ov: bool = True,
        workers: int = 4,
    ) -> None:
        """
        Performs calibration and noise removal using a multi-threaded windowed approach.
        """
        cal_xml, noise_xml = self._get_xml_files(polarization)
        sds_string = self._get_subdataset_string(polarization)
        cal_vectors = self._parse_calibration_xml(cal_xml)
        noise_vectors = self._parse_noise_xml(noise_xml)

        with rio.open(sds_string) as src:
            width: int = src.width
            height: int = src.height
            print("Preparing LUT interpolation...", flush=True)
            cal_func = self._interpolate_vectors(cal_vectors, width, "sigma")
            noise_func = self._interpolate_vectors(noise_vectors, width, "noise")

            profile = src.profile.copy()
            profile.update(
                dtype=rio.float32,
                count=2,
                driver="GTiff",
                compress="deflate",
                tiled=True,
                blockxsize=256,
                blockysize=256,
                nodata=None,
                num_threads=workers,
            )
            if "transform" in profile:
                del profile["transform"]

            write_lock = threading.Lock()

            def process_window(row_off: int) -> None:
                rows = min(block_size, height - row_off)
                window = Window(0, row_off, width, rows)
                with rio.open(sds_string) as t_src:
                    dn = t_src.read(1, window=window).astype(np.float32)

                valid_mask = dn > 0
                current_lines = np.arange(row_off, row_off + rows)
                cal_block = cal_func(current_lines).astype(np.float32)
                noise_block = noise_func(current_lines).astype(np.float32)

                if HAS_CUDA:
                    m_pool = cp.get_default_memory_pool()
                    dn_g = cp.array(dn)
                    valid_g = cp.array(valid_mask)
                    cal_g = cp.array(cal_block)
                    noise_g = cp.array(noise_block)

                    sigma0_g = cp.zeros_like(dn_g)
                    sigma0_g[valid_g] = cp.maximum(
                        (cp.square(dn_g[valid_g]) - noise_g[valid_g])
                        / cp.square(cal_g[valid_g]),
                        1e-9,
                    )

                    alpha_g = cp.zeros_like(dn_g)
                    alpha_g[valid_g] = 255.0

                    sigma0 = cp.asnumpy(sigma0_g)
                    alpha_block = cp.asnumpy(alpha_g)
                    del dn_g, valid_g, cal_g, noise_g, sigma0_g, alpha_g
                    m_pool.free_all_blocks()
                else:
                    sigma0 = np.zeros_like(dn)
                    sigma0[valid_mask] = np.maximum(
                        (np.square(dn[valid_mask]) - noise_block[valid_mask])
                        / np.square(cal_block[valid_mask]),
                        1e-9,
                    )
                    alpha_block = np.zeros_like(dn)
                    alpha_block[valid_mask] = 255.0

                with write_lock:
                    dst.write(sigma0, 1, window=window)
                    dst.write(alpha_block, 2, window=window)
                print(f"Processed strip starting at line {row_off}/{height}", end="\r", flush=True)

            with rio.open(output_path, "w", **profile) as dst:
                if src.gcps and src.gcps[0]:
                    gcp_list, gcp_crs = src.gcps
                    dst.gcps = (gcp_list, gcp_crs or rio.crs.CRS.from_epsg(4326))
                    dst.crs = gcp_crs or rio.crs.CRS.from_epsg(4326)

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    executor.map(process_window, range(0, height, block_size))

                if build_ov:
                    func.perf_logger.start_step(
                        f"S1 Internal Overviews: {os.path.basename(output_path)}"
                    )
                    dst.build_overviews([2, 4, 8, 16, 32, 64], rio.enums.Resampling.average)
                    func.perf_logger.end_step()

        del cal_func, noise_func
        gc.collect()
        print(f"\nCalibration complete: {output_path}", flush=True)
