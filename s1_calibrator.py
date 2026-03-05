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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import glob
import numpy as np
import rasterio as rio
from rasterio.windows import Window
from lxml import etree
from scipy.interpolate import interp1d
import gc


class S1Calibrator:
    """
    S1Calibrator handles radiometric calibration and thermal noise removal
    for Sentinel-1 GRD products using a memory-efficient, windowed approach.
    """

    def __init__(self, safe_path):
        self.safe_path = os.path.abspath(safe_path)
        self.manifest_path = os.path.join(self.safe_path, "manifest.safe")
        self.annotation_dir = os.path.join(self.safe_path, "annotation")
        self.calibration_dir = os.path.join(self.annotation_dir, "calibration")

        if not os.path.exists(self.manifest_path):
            raise ValueError(f"manifest.safe not found in: {self.safe_path}")

    def _get_xml_files(self, pol):
        """Finds the calibration and noise XML files for a polarization."""
        pol = pol.lower()
        cal_files = glob.glob(
            os.path.join(self.calibration_dir, f"calibration-s1?-iw-grd-{pol}-*.xml")
        )
        noise_files = glob.glob(
            os.path.join(self.calibration_dir, f"noise-s1?-iw-grd-{pol}-*.xml")
        )

        if not cal_files or not noise_files:
            raise FileNotFoundError(
                f"Could not find XML components for polarization: {pol}"
            )

        return cal_files[0], noise_files[0]

    def _get_subdataset_string(self, polarization):
        """Constructs the GDAL subdataset string for the manifest."""
        return f"SENTINEL1_CALIB:UNCALIB:{self.manifest_path}:IW_{polarization.upper()}:AMPLITUDE"

    def _parse_calibration_xml(self, cal_xml):
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

    def _parse_noise_xml(self, noise_xml):
        """Parses the noise XML to extract thermal noise vectors."""
        tree = etree.parse(noise_xml)
        root = tree.getroot()
        vectors = []
        noise_nodes = root.xpath("//noiseVector") or root.xpath("//noiseRangeVector")
        for vector_node in noise_nodes:
            line_node = vector_node.find("line")
            pixel_node = vector_node.find("pixel")
            noise_val_node = vector_node.find("noiseLut") or vector_node.find(
                "noiseRangeLut"
            )
            if (
                line_node is not None
                and pixel_node is not None
                and noise_val_node is not None
            ):
                line = int(line_node.text)
                pixel_indices = np.fromstring(pixel_node.text, sep=" ", dtype=int)
                noise_values = np.fromstring(noise_val_node.text, sep=" ", dtype=float)
                vectors.append(
                    {"line": line, "pixels": pixel_indices, "noise": noise_values}
                )
        return vectors

    def _interpolate_vectors(self, vectors, total_pixels, key):
        """Interpolates sparse vectors to a full-image grid."""
        if not vectors:
            raise ValueError(f"No vectors found for key: {key}")

        lines = np.array([v["line"] for v in vectors])
        grid_values = []
        for v in vectors:
            f = interp1d(v["pixels"], v[key], kind="linear", fill_value="extrapolate")
            grid_values.append(f(np.arange(total_pixels)))

        full_lut_func = interp1d(
            lines,
            np.array(grid_values),
            axis=0,
            kind="linear",
            fill_value="extrapolate",
        )
        del grid_values
        return full_lut_func

    def calibrate(self, polarization, output_path, block_size=1024):
        """
        Performs calibration and denoising using DN > 0 for the alpha mask.
        """
        cal_xml, noise_xml = self._get_xml_files(polarization)
        sds_string = self._get_subdataset_string(polarization)

        print(f"Opening subdataset: {sds_string}")

        cal_vectors = self._parse_calibration_xml(cal_xml)
        noise_vectors = self._parse_noise_xml(noise_xml)

        with rio.open(sds_string) as src:
            profile = src.profile.copy()
            width = src.width
            height = src.height

            print("Preparing LUT interpolation...")
            cal_func = self._interpolate_vectors(cal_vectors, width, "sigma")
            noise_func = self._interpolate_vectors(noise_vectors, width, "noise")

            # Update profile for Float32 Sigma0 + Alpha Mask
            profile.update(
                dtype=rio.float32,
                count=2,
                driver="GTiff",
                compress="deflate",
                tiled=True,
                blockxsize=256,
                blockysize=256,
                nodata=None,
                num_threads=2,
            )

            if "transform" in profile:
                del profile["transform"]

            with rio.open(output_path, "w", **profile) as dst:
                if src.gcps and src.gcps[0]:
                    gcp_list, gcp_crs = src.gcps
                    if gcp_crs is None:
                        gcp_crs = rio.crs.CRS.from_epsg(4326)
                    dst.gcps = (gcp_list, gcp_crs)
                    dst.crs = gcp_crs

                for row_off in range(0, height, block_size):
                    rows = min(block_size, height - row_off)
                    window = Window(0, row_off, width, rows)

                    dn = src.read(1, window=window).astype(np.float32)
                    valid_mask = dn > 0

                    current_lines = np.arange(row_off, row_off + rows)
                    cal_block = cal_func(current_lines)
                    noise_block = noise_func(current_lines)

                    sigma0 = np.zeros_like(dn)
                    if np.any(valid_mask):
                        sigma0[valid_mask] = np.maximum(
                            (np.square(dn[valid_mask]) - noise_block[valid_mask])
                            / np.square(cal_block[valid_mask]),
                            1e-9,
                        )

                    # Create alpha block based on valid data
                    alpha_block = np.zeros_like(dn, dtype=np.float32)
                    alpha_block[valid_mask] = 255.0

                    dst.write(sigma0, 1, window=window)
                    dst.write(alpha_block, 2, window=window)

                    print(
                        f"Processed strip starting at line {row_off}/{height}", end="\r"
                    )

                gc.collect()

                print("\nBuilding internal overviews...")
                overviews = [2, 4, 8, 16, 32, 64]
                dst.build_overviews(overviews, rio.enums.Resampling.average)
                dst.update_tags(ns="rio_overview", resampling="average")

        del cal_func
        del noise_func
        gc.collect()

        print(f"\nCalibration complete: {output_path}")
