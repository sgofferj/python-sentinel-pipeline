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
import asyncio


class connect:
    """Functions to find and download satellite products from EU Copernicus Data Hub"""

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
            self.token = r.json()["access_token"]
            self.refresh_token = r.json()["refresh_token"]

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
            self.token = r.json()["access_token"]
            self.refresh_token = r.json()["refresh_token"]

    def productSearch(
        self,
        collections,
        completionDate=None,
        maxRecords=None,
        productType=None,
        published=None,
        publishedAfter=None,
        publishedBefore=None,
        sortOrder=None,
        sortParam=None,
        startDate=None,
        updated=None,
        lat=None,
        lon=None,
        radius=None,
        geometry=None,
        box=None,
        cloudCover=None,
    ):
        """Collections:"""
        """ Sentinel1 or SENTINEL-1 """
        """ Sentinel2 or SENTINEL-2 """
        """ Sentinel3 or SENTINEL-3 """
        """ Sentinel5P or SENTINEL-5P """
        """ Sentinel6 or SENTINEL-6 """
        """ Sentinel1RTC or SENTINEL-1-RTC """

        amp = ""
        url = f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/{collections}/search.json?"
        if completionDate:
            url += f"{amp}completionDate={completionDate}"
            amp = "&"
        if maxRecords:
            url += f"{amp}maxRecords={maxRecords}"
            amp = "&"
        if productType:
            url += f"{amp}productType={productType}"
            amp = "&"
        if published:
            url += f"{amp}published={published}"
            amp = "&"
        if publishedAfter:
            url += f"{amp}publishedAfter={publishedAfter}"
            amp = "&"
        if publishedBefore:
            url += f"{amp}publishedBefore={publishedBefore}"
            amp = "&"
        if sortOrder:
            url += f"{amp}sortOrder={sortOrder}"
            amp = "&"
        if sortParam:
            url += f"{amp}sortParam={sortParam}"
            amp = "&"
        if startDate:
            url += f"{amp}startDate={startDate}"
            amp = "&"
        if updated:
            url += f"{amp}updated={updated}"
            amp = "&"
        if lat and lon:
            url += f"{amp}lat={lat}&lon={lon}"
            amp = "&"
        if lat and lon and radius:
            url += f"&radius={radius}"
            amp = "&"
        if geometry:
            url += f"{amp}geometry={geometry}"
            amp = "&"
        if box:
            url += f"{amp}box={box}"
            amp = "&"
        if cloudCover:
            url += f"{amp}cloudCover=[0,{cloudCover}]"
            amp = "&"
        r = req.get(url)
        if r.status_code != 200:
            return r.status_code, r.text
        else:
            return r.status_code, r.json()

    def getS2Utm(self, name):
        """Gets the UTM grid from a Sentinel 2 dataset name"""
        result = re.search(r"S2._......_\d+T\d+_\w\d+_\w\d+_(.*)_\d+T\d+.SAFE", name)
        utm = result.groups()[0]
        return utm

    def download(self, uuid, filename, directory="."):
        """Downloads a dataset from Copernicus"""
        url = f"https://download.dataspace.copernicus.eu/download/{uuid}"
        headers = {"Authorization": f"Bearer {self.token}"}
        r = req.get(url, headers=headers, stream=True)
        with open(f"{directory}/{filename}.zip", "wb") as file:
            for chunk in r.iter_content(chunk_size=10 * 1024):
                file.write(chunk)
            file.close()
        del r
