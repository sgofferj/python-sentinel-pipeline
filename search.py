#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# search.py from https://github.com/sgofferj/python-sentinel-pipeline/copernicus
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

import copernicus as cop
import functions as func
from dotenv import load_dotenv
import os

load_dotenv()

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)


S2_CLOUDCOVER = os.getenv("S2_CLOUDCOVER", default=5)
S2_BOX = os.getenv("S2_BOX")
S2_STARTDATE = os.getenv("S2_STARTDATE", default=func.yesterday())
S2_MAXRECORDS = os.getenv("S2_MAXRECORDS", default=5)
S2_SORTPARAM = os.getenv("S2_SORTPARAM", default="startDate")
S2_SORTORDER = os.getenv("S2_SORTORDER", default="descending")
S2_PRODUCTTYPE = os.getenv("S2_PRODUCTTYPE", default="L2A")
S2_CLIP = os.getenv("S2_CLIP", default=True)


def S2_search(boxes):
    result = {}

    for box in boxes:
        print(f"Searching for products in box {box}...")
        status, searchResult = mycop.productSearch(
            "Sentinel2",
            cloudCover=S2_CLOUDCOVER,
            box=box,
            startDate=S2_STARTDATE,
            maxRecords=S2_MAXRECORDS,
            sortParam=S2_SORTPARAM,
            sortOrder=S2_SORTORDER,
            productType=S2_PRODUCTTYPE,
        )

        print(f'Found {len(searchResult["features"])} products since {S2_STARTDATE}.')

        filelist = []
        for feature in searchResult["features"]:
            fileID = feature["id"]
            fileName = feature["properties"]["title"]
            print(
                feature["properties"]["published"],
                fileName,
                feature["properties"]["cloudCover"],
                fileID,
            )
            filelist.append({fileID: fileName})
        result.update({box: filelist})
        print()
    return result


if __name__ == "__main__":
    print("----- Search-pipeline only -----")
    boxes = func.getBoxes(S2_BOX)
    searchresult = S2_search(boxes)
    print("\n----- Result -----")
    for box in searchresult:
        print(f"Search box: {box}")
        print("Files:")
        for files in searchresult[box]:
            for fileid in files:
                print(fileid, files[fileid])
        print()
