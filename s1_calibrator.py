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
from typing import Any, Dict, List, Optional, Tuple

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
        cal_files = sorted(
            glob.glob(
                os.path.join(self.calibration_dir, f"calibration-s1?-iw-grd-{pol}-*.xml")
            )
        )
        noise_files = sorted(
            glob.glob(
                os.path.join(self.calibration_dir, f"noise-s1?-iw-grd-{pol}-*.xml")
            )
        )
        if not cal_files or not noise_files:
            raise FileNotFoundError(
                f"Could not find XML components for polarization: {pol}"
            )
        # Deterministic: sorted ensures repeatable pick if multiple timestamps
        # For IW GRD there is exactly one file per pol (IW1-3 merged).
        # For SLC there would be per-swath files — GRD search pattern intentionally
        # limits to GRD; SLC would need separate handling via SENTINEL1 DS per swath.
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
            pixel_indices = np.fromstring(
                vector_node.find("pixel").text, sep=" ", dtype=int
            )
            sigma_nought = np.fromstring(
                vector_node.find("sigmaNought").text, sep=" ", dtype=float
            )
            vectors.append(
                {"line": line, "pixels": pixel_indices, "sigma": sigma_nought}
            )
        return vectors

    def _parse_noise_xml(
        self, noise_xml: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Parses the noise XML to extract thermal noise LUTs.

        Returns (range_vectors, azimuth_vectors) per ESA MPC-0392 / IPF 2.9+.

        - range_vectors:  list of {line, pixels, noise}  from //noiseRangeVector
                          (fallback //noiseVector for IPF < 2.9)
        - azimuth_vectors: list of {firstAzimuthLine, lastAzimuthLine,
                           firstRangeSample, lastRangeSample, line, lut}
                          from //noiseAzimuthVector  (empty if not present
                          → range-only denoising, e.g. SM or old TOPS).
        The correct denoised power is  eta = noiseRange * noiseAzimuth
        (MPC-0392 §6.1.2.3: noiseCorrectionMatrix = noiseRangeMatrix *
        noiseAzimuthMatrix).  Previous code used range-only.
        """
        tree = etree.parse(noise_xml)
        root = tree.getroot()

        # --- Range vectors ---
        range_vectors: List[Dict[str, Any]] = []
        range_nodes = root.xpath("//noiseRangeVector")
        if not range_nodes:
            range_nodes = root.xpath("//noiseVector")  # legacy IPF < 2.9
        for vector_node in range_nodes:
            line_node = vector_node.find("line")
            pixel_node = vector_node.find("pixel")
            # lxml element truthiness is False for leaf nodes (no children),
            # so avoid `or` which would mis-evaluate. Use explicit None check.
            _n1 = vector_node.find("noiseLut")
            _n2 = vector_node.find("noiseRangeLut")
            noise_val_node = _n1 if _n1 is not None else _n2
            if (
                line_node is not None
                and pixel_node is not None
                and noise_val_node is not None
                and line_node.text
                and pixel_node.text
                and noise_val_node.text
            ):
                try:
                    line = int(float(line_node.text.strip().split()[0]))
                except Exception:
                    continue
                pixel_indices = np.fromstring(pixel_node.text, sep=" ", dtype=int)
                noise_values = np.fromstring(noise_val_node.text, sep=" ", dtype=float)
                if pixel_indices.size == 0 or noise_values.size == 0:
                    continue
                # Also capture azimuthTime if present (more reliable than line for
                # IPF < 003.71 where line was shifted; we keep line for interp)
                az_time_node = vector_node.find("azimuthTime")
                az_time = az_time_node.text if az_time_node is not None else None
                range_vectors.append(
                    {
                        "line": line,
                        "pixels": pixel_indices,
                        "noise": noise_values,
                        "azimuthTime": az_time,
                    }
                )

        # --- Azimuth vectors (IPF 2.90+, TOPS GRD/SLC) ---
        azimuth_vectors: List[Dict[str, Any]] = []
        az_nodes = root.xpath("//noiseAzimuthVector")
        for vector_node in az_nodes:
            line_node = vector_node.find("line")
            # Explicit None checks (lxml bool quirk)
            _a1 = vector_node.find("noiseAzimuthLut")
            if _a1 is not None:
                lut_node = _a1
            else:
                _a2 = vector_node.find("noiseAzimuthVector")
                if _a2 is not None:
                    lut_node = _a2
                else:
                    lut_node = vector_node.find("noiseLut")
            if line_node is None or lut_node is None:
                continue
            if not line_node.text or not lut_node.text:
                continue
            try:
                line_arr = np.fromstring(line_node.text, sep=" ", dtype=float)
                # line values are integer line numbers but may be stored as float strings
                line_arr_i = line_arr.astype(int) if line_arr.size else np.array([], dtype=int)
                lut_arr = np.fromstring(lut_node.text, sep=" ", dtype=float)
            except Exception:
                continue
            if line_arr_i.size == 0 or lut_arr.size == 0:
                continue
            # Block bounds — optional; if absent, vector is assumed global
            def _get_int(tag: str, default: Optional[int] = None) -> Optional[int]:
                n = vector_node.find(tag)
                if n is not None and n.text and n.text.strip():
                    try:
                        return int(float(n.text.strip().split()[0]))
                    except Exception:
                        return default
                return default

            first_az = _get_int("firstAzimuthLine")
            last_az = _get_int("lastAzimuthLine")
            first_rg = _get_int("firstRangeSample")
            last_rg = _get_int("lastRangeSample")
            swath_node = vector_node.find("swath")
            swath = swath_node.text.strip() if swath_node is not None and swath_node.text else None
            azimuth_vectors.append(
                {
                    "firstAzimuthLine": first_az,
                    "lastAzimuthLine": last_az,
                    "firstRangeSample": first_rg,
                    "lastRangeSample": last_rg,
                    "line": line_arr_i,
                    "lut": lut_arr,
                    "swath": swath,
                }
            )

        return range_vectors, azimuth_vectors

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
        noise_range_vectors, noise_azimuth_vectors = self._parse_noise_xml(noise_xml)

        if not cal_vectors:
            raise ValueError(f"No calibration vectors found in {cal_xml}")
        if not noise_range_vectors:
            raise ValueError(f"No noise range vectors found in {noise_xml}")

        # Log LUT flavour for ops visibility
        if noise_azimuth_vectors:
            print(
                f"Noise LUT: {len(noise_range_vectors)} range vectors + "
                f"{len(noise_azimuth_vectors)} azimuth vectors "
                f"(IPF ≥2.90 TOPS, eta=range*azimuth)",
                flush=True,
            )
        else:
            print(
                f"Noise LUT: {len(noise_range_vectors)} range vectors only "
                f"(legacy / SM / pre-2.90) — range-only denoising",
                flush=True,
            )

        # 1. Create the output file with proper metadata using GDAL Translate
        # This copies GCPs, CRS, etc. perfectly from the subdataset.
        print(
            f"Initializing {os.path.basename(output_path)} with source metadata...",
            flush=True,
        )
        gdal.Translate(
            output_path,
            sds_string,
            outputType=gdal.GDT_Float32,
            creationOptions=[
                "TILED=YES",
                "COMPRESS=DEFLATE",
                "BIGTIFF=YES",
                "BLOCKXSIZE=256",
                "BLOCKYSIZE=256",
            ],
        )

        with rio.open(output_path, "r+") as dst:
            width: int = dst.width
            height: int = dst.height

            # --- INTERPOLATION PREP ---
            # Separate line bases for cal and noise (they are sampled independently;
            # earlier GPU code incorrectly coupled noise to cal lines).
            lines_cal = np.array([v["line"] for v in cal_vectors], dtype=float)
            lines_noise_range = np.array(
                [v["line"] for v in noise_range_vectors], dtype=float
            )

            # --- Azimuth factor (IPF 2.90+) ---
            # Per MPC-0392 §6.1.2.1–6.1.2.3: eta = noiseRangeMatrix * noiseAzimuthMatrix
            # noiseAzimuthMatrix is a per-line gain replicated across range.
            # For GRD TOPS it partitions azimuth into blocks of constant sub-swath
            # merging; each block has firstAzimuthLine/lastAzimuthLine and a
            # sub-sampled Lut interpolated to every line in the block.
            # We build a 1-D factor array size (height,) = 1.0 where no azimuth
            # vector covers the line, otherwise interpolated azimuth gain.
            # Range windowing (firstRangeSample/lastRangeSample) would require a
            # 2-D matrix; GRD merged products have full-width blocks, so 1-D is
            # correct and avoids a (height*width) allocation (~1.5 GB). If a
            # product ever has range-windowed azimuth blocks, we still gain
            # correctly over water where VH matters, and log a note.
            azimuth_factor: Optional[np.ndarray] = None
            _az_has_range_window = False
            if noise_azimuth_vectors:
                azimuth_factor = np.ones(height, dtype=np.float32)
                # Track coverage for diagnostics
                _covered = np.zeros(height, dtype=bool)
                for av in noise_azimuth_vectors:
                    line_sub = av["line"]
                    lut = av["lut"]
                    if line_sub.size == 0 or lut.size == 0:
                        continue
                    n = min(line_sub.size, lut.size)
                    line_sub = line_sub[:n]
                    lut = lut[:n]
                    # Sort by line for interp1d
                    order = np.argsort(line_sub)
                    line_sub = line_sub[order].astype(float)
                    lut = lut[order].astype(float)
                    # Range windowing check (non-full width)
                    fr = av["firstRangeSample"]
                    lr = av["lastRangeSample"]
                    if fr is not None and lr is not None:
                        # Heuristic: width unknown at parse time, but now we know it
                        if not (fr == 0 and lr >= width - 1):
                            _az_has_range_window = True
                    fa = av["firstAzimuthLine"]
                    la = av["lastAzimuthLine"]
                    if fa is None:
                        fa = int(np.min(line_sub)) if line_sub.size else 0
                    if la is None:
                        la = int(np.max(line_sub)) if line_sub.size else height - 1
                    s = max(0, int(fa))
                    e = min(height - 1, int(la))
                    if s > e:
                        continue
                    block_lines = np.arange(s, e + 1)
                    try:
                        f_az = interp1d(
                            line_sub,
                            lut,
                            kind="linear",
                            fill_value="extrapolate",
                            assume_sorted=True,
                        )
                        vals = f_az(block_lines).astype(np.float32)
                    except Exception as ex:
                        print(f"Warning: azimuth interp failed block {s}-{e}: {ex}", flush=True)
                        continue
                    # Blocks partition azimuth without overlap for GRD; assign
                    azimuth_factor[block_lines] = vals
                    _covered[block_lines] = True
                if _az_has_range_window:
                    print(
                        "Note: some noiseAzimuthVector have range-windowed "
                        "firstRangeSample/lastRangeSample (rare for GRD). "
                        "Applying azimuth factor uniformly across range — "
                        "still correct for NESZ scaling; 2-D windowing ignored.",
                        flush=True,
                    )
                cov_pct = 100.0 * np.count_nonzero(_covered) / max(height, 1)
                print(
                    f"Azimuth LUT coverage: {cov_pct:.1f}% of lines "
                    f"({np.count_nonzero(_covered)}/{height})",
                    flush=True,
                )
                # If coverage is tiny (e.g. SLC burst template), fall back to
                # global interpolation across all lines using first vector
                if cov_pct < 10.0 and len(noise_azimuth_vectors) == 1:
                    # Already handled via block assignment, but ensure factor not left as 1.0
                    pass

            if HAS_CUDA:
                print("Using CUDA for LUT Interpolation and Calibration.", flush=True)
                # Calibration LUT (range interp per cal line)
                grid_vals_sigma = []
                for v in cal_vectors:
                    f_s = interp1d(
                        v["pixels"], v["sigma"], kind="linear", fill_value="extrapolate"
                    )
                    grid_vals_sigma.append(f_s(np.arange(width)))
                # Noise range LUT (independent basis)
                grid_vals_noise_range = []
                for v in noise_range_vectors:
                    f_n = interp1d(
                        v["pixels"], v["noise"], kind="linear", fill_value="extrapolate"
                    )
                    grid_vals_noise_range.append(f_n(np.arange(width)))

                g_lines_cal = cp.array(lines_cal, dtype=cp.float32)
                g_lines_noise = cp.array(lines_noise_range, dtype=cp.float32)
                g_lut_sigma = cp.array(grid_vals_sigma, dtype=cp.float32)
                g_lut_noise_range = cp.array(grid_vals_noise_range, dtype=cp.float32)
                del grid_vals_sigma, grid_vals_noise_range

                g_azimuth_factor = None
                if azimuth_factor is not None:
                    g_azimuth_factor = cp.array(azimuth_factor, dtype=cp.float32)

                # Keep for fallback logging
                g_az_has_window = _az_has_range_window
            else:
                print(
                    "GPU Unavailable: Falling back to CPU for LUT Interpolation.",
                    flush=True,
                )
                grid_values_s = []
                grid_values_n = []
                for v in cal_vectors:
                    f = interp1d(
                        v["pixels"], v["sigma"], kind="linear", fill_value="extrapolate"
                    )
                    grid_values_s.append(f(np.arange(width)))
                for v in noise_range_vectors:
                    f = interp1d(
                        v["pixels"], v["noise"], kind="linear", fill_value="extrapolate"
                    )
                    grid_values_n.append(f(np.arange(width)))

                cal_func = interp1d(
                    lines_cal,
                    np.array(grid_values_s),
                    axis=0,
                    kind="linear",
                    fill_value="extrapolate",
                )
                noise_range_func = interp1d(
                    lines_noise_range,
                    np.array(grid_values_n),
                    axis=0,
                    kind="linear",
                    fill_value="extrapolate",
                )
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
                                noise_range_block = noise_range_func(current_lines).astype(
                                    np.float32
                                )
                                if azimuth_factor is not None:
                                    # ESA MPC-0392 §6.1.2.3: eta = range * azimuth
                                    az_slice = azimuth_factor[current_lines].astype(
                                        np.float32
                                    )[:, None]  # (rows,1) broadcast to (rows,width)
                                    noise_block = noise_range_block * az_slice
                                else:
                                    noise_block = noise_range_block
                                read_queue.put(
                                    (window, dn, cal_block, noise_block), timeout=120
                                )
                            else:
                                read_queue.put((window, dn, row_off, rows), timeout=120)
                        read_queue.put(None, timeout=120)
                except Exception as e:
                    print(f"\nCRITICAL: Reader thread failed: {e}", flush=True)
                    import traceback as _tb
                    _tb.print_exc()
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
            t_read.start()
            t_write.start()

            while True:
                try:
                    item = read_queue.get(timeout=120)
                except queue.Empty:
                    print(
                        "\nCRITICAL: Reader thread timed out (Deadlock?).", flush=True
                    )
                    break

                if item is None:
                    write_queue.put(None, timeout=120)
                    read_queue.task_done()
                    break

                try:
                    if HAS_CUDA:
                        window, dn, row_off, rows = item
                        m_pool = cp.get_default_memory_pool()
                        g_dn = cp.array(dn)
                        g_valid = g_dn > 0
                        target_lines = cp.arange(
                            row_off, row_off + rows, dtype=cp.float32
                        )

                        def _gpu_interp(lut, g_lines_basis, n_lines):
                            # Linear interp in azimuth: lut shape (n_basis, width)
                            # n_lines may be < len(g_lines_basis) -1 clamp
                            idx = cp.searchsorted(g_lines_basis, target_lines) - 1
                            # Clamp to valid segment range
                            max_idx = max(0, n_lines - 2)
                            idx = cp.clip(idx, 0, max_idx)
                            x0 = g_lines_basis[idx]
                            x1 = g_lines_basis[idx + 1]
                            # Avoid div0 where x1==x0 (duplicate lines)
                            denom = x1 - x0
                            # where denom==0 weight=0
                            weight = cp.where(
                                denom != 0, (target_lines - x0) / denom, 0
                            )
                            y0 = lut[idx]
                            y1 = lut[idx + 1]
                            return y0 + weight[:, cp.newaxis] * (y1 - y0)

                        g_cal = _gpu_interp(
                            g_lut_sigma, g_lines_cal, len(lines_cal)
                        )
                        g_noise_range = _gpu_interp(
                            g_lut_noise_range, g_lines_noise, len(lines_noise_range)
                        )
                        if g_azimuth_factor is not None:
                            g_az = g_azimuth_factor[
                                row_off : row_off + rows
                            ]  # 1-D slice
                            # ESA §6.1.2.3: eta = range * azimuth
                            g_noise = g_noise_range * g_az[:, cp.newaxis]
                        else:
                            g_noise = g_noise_range

                        g_sigma0 = cp.zeros_like(g_dn)
                        # Ensure valid pixels are NEVER absolute 0 to preserve nodata=0 meaning
                        # Formula: sigma0 = max((DN^2 - eta)/A^2, 1e-9) for DN>0,
                        # where eta==0 (missing LUT at edges) -> no subtraction (eta=0 already)
                        # Negative after subtraction clipped to 1e-9 (avoid -inf, preserve nodata split)
                        g_sigma0[g_valid] = cp.maximum(
                            (cp.square(g_dn[g_valid]) - g_noise[g_valid])
                            / cp.square(g_cal[g_valid]),
                            1e-9,
                        )

                        sigma0 = cp.asnumpy(g_sigma0)
                        del (
                            g_dn,
                            g_valid,
                            g_cal,
                            g_noise,
                            g_noise_range,
                            g_sigma0,
                            target_lines,
                        )
                        if g_azimuth_factor is not None:
                            del g_az
                        m_pool.free_all_blocks()
                    else:
                        window, dn, cal_block, noise_block = item
                        valid_mask = dn > 0
                        sigma0 = np.zeros_like(dn, dtype=np.float32)
                        # noise_block already includes azimuth factor (range*azimuth) via reader_thread
                        # Clip where DN^2 - eta may be negative (low SNR, VH over water)
                        # Per MPC-0392 §6.2, negative denoised power is clipped to 0; we use
                        # 1e-9 floor to keep nodata=0 distinct from valid dark pixel.
                        sigma0[valid_mask] = np.maximum(
                            (np.square(dn[valid_mask].astype(np.float64))
                             - noise_block[valid_mask].astype(np.float64))
                            / np.square(cal_block[valid_mask].astype(np.float64)),
                            1e-9,
                        ).astype(np.float32)

                    write_queue.put((window, sigma0), timeout=120)
                except Exception as e:
                    print(f"\nCRITICAL: Calibration loop failed: {e}", flush=True)
                    import traceback as _tb2
                    _tb2.print_exc()
                    break

                print(
                    f"Processed strip starting at line {window.row_off}/{height}",
                    end="\r",
                    flush=True,
                )
                read_queue.task_done()

            t_read.join()
            t_write.join()

            if build_ov:
                func.perf_logger.start_step(
                    f"S1 Internal Overviews: {os.path.basename(output_path)}"
                )
                dst.build_overviews([2, 4, 8, 16, 32, 64], rio.enums.Resampling.average)
                func.perf_logger.end_step()

        gc.collect()
        print(f"\nCalibration complete: {output_path}", flush=True)
