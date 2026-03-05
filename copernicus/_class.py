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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import requests as req
import re
from urllib.parse import quote


class connect:
    """Functions to find and download satellite products from EU Copernicus Data Hub using OData API"""

    def __init__(self, username, password):
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        r = req.post(url, data=data)
        self.status = r.status_code
        if self.status != 200:
            self.error = r.text
        else:
            rj = r.json()
            self.token = rj["access_token"]
            self.refresh_token = rj["refresh_token"]

    def refreshToken(self):
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        data = {
            "client_id": "cdse-public",
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        r = req.post(url, data=data)
        self.status = r.status_code
        if self.status != 200:
            self.error = r.text
        else:
            rj = r.json()
            self.token = rj["access_token"]
            self.refresh_token = rj["refresh_token"]

    def productSearch(
        self,
        collections,
        maxRecords=None,
        productType=None,
        sortOrder="desc",
        sortParam="ContentDate/Start",
        startDate=None,
        geometry=None,
        box=None,
        cloudCover=None,
        sensorMode=None,
    ):
        """Searches for products using OData API and returns resto-compatible GeoJSON."""
        
        coll_map = {
            "Sentinel1": "SENTINEL-1",
            "Sentinel2": "SENTINEL-2",
            "Sentinel3": "SENTINEL-3",
            "Sentinel5P": "SENTINEL-5P"
        }
        odata_coll = coll_map.get(collections, collections)
        
        filters = [f"Collection/Name eq '{odata_coll}'"]
        
        if startDate:
            if len(startDate) == 10:
                startDate += "T00:00:00.000Z"
            # CDSE OData expects timestamps WITHOUT quotes in the filter
            filters.append(f"ContentDate/Start gt {startDate}")

        if productType:
            filters.append(f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/Value eq '{productType}')")
            
        if sensorMode:
            filters.append(f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/Value eq '{sensorMode}')")

        if cloudCover is not None:
            filters.append(f"Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/Value le {cloudCover})")

        spatial_filter = None
        if box:
            try:
                c = box.split(",")
                # OData Intersects needs SRID=4326 prefix and unquoted geography literal
                wkt = f"POLYGON(({c[0]} {c[1]},{c[2]} {c[1]},{c[2]} {c[3]},{c[0]} {c[3]},{c[0]} {c[1]}))"
                spatial_filter = f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
            except:
                pass
        elif geometry:
            spatial_filter = f"OData.CSC.Intersects(area=geography'SRID=4326;{geometry}')"
            
        if spatial_filter:
            filters.append(spatial_filter)

        filter_query = " and ".join(filters)
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter={quote(filter_query)}&$expand=Attributes"
        
        # Mapping sort orders
        sort_map = {"descending": "desc", "ascending": "asc"}
        odata_order = sort_map.get(sortOrder, sortOrder)
        
        # Mapping sort parameters
        param_map = {"startDate": "ContentDate/Start", "completionDate": "ContentDate/End"}
        odata_param = param_map.get(sortParam, sortParam)
        
        if odata_param:
            url += f"&$orderby={odata_param} {odata_order}"
        if maxRecords:
            url += f"&$top={maxRecords}"

        r = req.get(url)
        if r.status_code != 200:
            print(f"OData Search Error {r.status_code}: {r.text}")
            return r.status_code, {"features": []}
        
        odata_data = r.json()
        resto_compat = {"features": []}
        
        for item in odata_data.get("value", []):
            cc = 0
            for attr in item.get("Attributes", []):
                if attr.get("Name") == "cloudCover":
                    cc = attr.get("Value", 0)
                    break
            
            # Clean footprint: geography'SRID=4326;POLYGON((...))' -> POLYGON((...))
            raw_footprint = item.get("Footprint")
            if raw_footprint:
                clean_footprint = re.sub(r"geography'SRID=4326;(.+)'", r"\1", raw_footprint)
            else:
                clean_footprint = ""
            
            feat = {
                "id": item["Id"],
                "properties": {
                    "title": item["Name"],
                    "cloudCover": cc,
                    "startDate": item["ContentDate"]["Start"],
                    "footprint": clean_footprint
                }
            }
            resto_compat["features"].append(feat)
            
        return r.status_code, resto_compat

    def getS2Utm(self, name):
        """Gets the UTM grid from a Sentinel 2 dataset name"""
        result = re.search(r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name)
        return result.groups()[0] if result else None

    def download(self, uuid, filename, directory="."):
        """Downloads a dataset from Copernicus"""
        url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        print(f"Downloading {filename}...")
        r = req.get(url, headers=headers, stream=True)
        r.raise_for_status()
        
        with open(f"{directory}/{filename}.zip", "wb") as file:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                file.write(chunk)
        return True
