#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# _class.py from https://github.com/sgofferj/python-sentinel-pipeline/copernicus
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Copernicus Data Space Ecosystem (CDSE) OData v1 API Connector.
Handles authentication, product search, metadata retrieval, and downloads.
"""

import re
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote

import requests as req


class connect:  # pylint: disable=invalid-name
    """
    Functions to find and download satellite products from EU Copernicus Data Hub
    using the OData API.
    """

    def __init__(self, username: str, password: str) -> None:
        url: str = (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        )
        data: Dict[str, str] = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        r = req.post(url, data=data, timeout=30)
        self.status: int = r.status_code
        if self.status != 200:
            self.error: str = r.text
        else:
            rj = r.json()
            self.token: str = rj["access_token"]
            self.refresh_token: str = rj["refresh_token"]

    def refreshToken(self) -> None:  # pylint: disable=invalid-name
        """Refreshes the OIDC access token."""
        url: str = (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        )
        data: Dict[str, str] = {
            "client_id": "cdse-public",
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        r = req.post(url, data=data, timeout=30)
        self.status = r.status_code
        if self.status != 200:
            self.error = r.text
        else:
            rj = r.json()
            self.token = rj["access_token"]
            self.refresh_token = rj["refresh_token"]

    def get_metadata(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves metadata for a specific product ID using OData."""
        url: str = (
            f"https://catalogue.dataspace.copernicus.eu/odata/v1/"
            f"Products({product_id})?$expand=Attributes"
        )
        r = req.get(url, timeout=30)
        if r.status_code != 200:
            print(f"OData Metadata Error {r.status_code}: {r.text}", flush=True)
            return None

        item = r.json()
        cloud_cover = 0
        for attr in item.get("Attributes", []):
            if attr.get("Name") == "cloudCover":
                cloud_cover = attr.get("Value", 0)
                break

        raw_footprint: str = item.get("Footprint", "")
        if raw_footprint:
            # Clean footprint: geography'SRID=4326;POLYGON((...))' -> POLYGON((...))
            clean_footprint = re.sub(r"geography'SRID=4326;(.+)'", r"\1", raw_footprint)
        else:
            clean_footprint = ""

        return {
            "id": item["Id"],
            "properties": {
                "title": item["Name"],
                "cloudCover": cloud_cover,
                "startDate": item["ContentDate"]["Start"],
                "footprint": clean_footprint,
            },
        }

    def productSearch(  # pylint: disable=invalid-name,too-many-arguments,too-many-locals
        self,
        collections: str,
        maxRecords: Optional[int] = None,  # pylint: disable=invalid-name
        productType: Optional[str] = None,  # pylint: disable=invalid-name
        sortOrder: str = "desc",  # pylint: disable=invalid-name
        sortParam: str = "ContentDate/Start",  # pylint: disable=invalid-name
        startDate: Optional[str] = None,  # pylint: disable=invalid-name
        geometry: Optional[str] = None,
        box: Optional[str] = None,
        cloudCover: Optional[float] = None,  # pylint: disable=invalid-name
        sensorMode: Optional[str] = None,  # pylint: disable=invalid-name
    ) -> Tuple[int, Dict[str, Any]]:
        """Searches for products using OData API and returns resto-compatible GeoJSON."""

        coll_map: Dict[str, str] = {
            "Sentinel1": "SENTINEL-1",
            "Sentinel2": "SENTINEL-2",
            "Sentinel3": "SENTINEL-3",
            "Sentinel5P": "SENTINEL-5P",
        }
        odata_coll: str = coll_map.get(collections, collections)

        filters: List[str] = [f"Collection/Name eq '{odata_coll}'"]

        if startDate:
            if len(startDate) == 10:
                startDate += "T00:00:00.000Z"
            # CDSE OData expects timestamps WITHOUT quotes in the filter
            filters.append(f"ContentDate/Start gt {startDate}")

        if productType:
            filters.append(
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq "
                f"'productType' and att/Value eq '{productType}')"
            )

        if sensorMode:
            filters.append(
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq "
                f"'operationalMode' and att/Value eq '{sensorMode}')"
            )

        if cloudCover is not None:
            filters.append(
                "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq "
                f"'cloudCover' and att/Value le {cloudCover})"
            )

        spatial_filter: Optional[str] = None
        if box:
            try:
                coords: List[str] = box.split(",")
                # OData Intersects needs SRID=4326 prefix and unquoted geography literal
                wkt: str = (
                    f"POLYGON(({coords[0]} {coords[1]},{coords[2]} {coords[1]},"
                    f"{coords[2]} {coords[3]},{coords[0]} {coords[3]},"
                    f"{coords[0]} {coords[1]}))"
                )
                spatial_filter = f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        elif geometry:
            spatial_filter = f"OData.CSC.Intersects(area=geography'SRID=4326;{geometry}')"

        if spatial_filter:
            filters.append(spatial_filter)

        filter_query: str = " and ".join(filters)
        url: str = (
            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
            f"$filter={quote(filter_query)}&$expand=Attributes"
        )

        # Mapping sort orders
        sort_map: Dict[str, str] = {"descending": "desc", "ascending": "asc"}
        odata_order: str = sort_map.get(sortOrder, sortOrder)

        # Mapping sort parameters
        param_map: Dict[str, str] = {
            "startDate": "ContentDate/Start",
            "completionDate": "ContentDate/End",
        }
        odata_param: str = param_map.get(sortParam, sortParam)

        if odata_param:
            url += f"&$orderby={odata_param} {odata_order}"
        if maxRecords:
            url += f"&$top={maxRecords}"

        r = req.get(url, timeout=60)
        if r.status_code != 200:
            print(f"OData Search Error {r.status_code}: {r.text}", flush=True)
            return r.status_code, {"features": []}

        odata_data: Dict[str, Any] = r.json()
        resto_compat: Dict[str, List[Dict[str, Any]]] = {"features": []}

        for item in odata_data.get("value", []):
            cc_val: Union[float, int] = 0
            for attr in item.get("Attributes", []):
                if attr.get("Name") == "cloudCover":
                    cc_val = attr.get("Value", 0)
                    break

            raw_ft: str = item.get("Footprint", "")
            if raw_ft:
                clean_ft = re.sub(r"geography'SRID=4326;(.+)'", r"\1", raw_ft)
            else:
                clean_ft = ""

            feat: Dict[str, Any] = {
                "id": item["Id"],
                "properties": {
                    "title": item["Name"],
                    "cloudCover": cc_val,
                    "startDate": item["ContentDate"]["Start"],
                    "footprint": clean_ft,
                },
            }
            resto_compat["features"].append(feat)

        return r.status_code, resto_compat

    def getS2Utm(self, name: str) -> Optional[str]:  # pylint: disable=invalid-name
        """Gets the UTM grid from a Sentinel 2 dataset name"""
        result: Optional[re.Match] = re.search(
            r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name
        )
        return result.groups()[0] if result else None

    def download(self, uuid: str, filename: str, directory: str = ".") -> bool:
        """Downloads a dataset from Copernicus."""
        url: str = f"https://download.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
        headers: Dict[str, str] = {"Authorization": f"Bearer {self.token}"}

        print(f"Downloading {filename}...", flush=True)
        r = req.get(url, headers=headers, stream=True, timeout=60)
        r.raise_for_status()

        with open(f"{directory}/{filename}.zip", "wb") as file:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                file.write(chunk)
        return True
