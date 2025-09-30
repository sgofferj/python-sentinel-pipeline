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
import constants as c
from dotenv import load_dotenv
import os
import json

load_dotenv()

BOX = os.getenv("BOX")

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)


USE_LOG = func.strtobool(os.getenv("USE_LOG", default=True))

S1_STARTDATE = os.getenv("S1_STARTDATE", default=func.yesterday())
S1_MAXRECORDS = os.getenv("S1_MAXRECORDS", default=1)
S1_SORTPARAM = os.getenv("S1_SORTPARAM", default="startDate")
S1_SORTORDER = os.getenv("S1_SORTORDER", default="descending")
S1_PRODUCTTYPE = os.getenv("S1_PRODUCTTYPE", default="GRD")
S1_CLIP = os.getenv("S1_CLIP", default=True)


S2_CLOUDCOVER = os.getenv("S2_CLOUDCOVER", default=5)
S2_STARTDATE = os.getenv("S2_STARTDATE", default=func.yesterday())
S2_MAXRECORDS = os.getenv("S2_MAXRECORDS", default=5)
S2_SORTPARAM = os.getenv("S2_SORTPARAM", default="startDate")
S2_SORTORDER = os.getenv("S2_SORTORDER", default="descending")
S2_PRODUCTTYPE = os.getenv("S2_PRODUCTTYPE", default="L2A")
S2_CLIP = os.getenv("S2_CLIP", default=True)


def readlog(sat):
    if USE_LOG:
        logfile = f"{c.DLDIR}/{sat}_last.json"
        if os.path.exists(logfile):
            with open(logfile) as f:
                d = json.load(f)
                f.close()
            return d
        else:
            return False
    else:
        return False


def writelog(sat, data):
    if USE_LOG:
        logfile = f"{c.DLDIR}/{sat}_last.json"
        data = {"time": func.this_moment(), "files": data}
        d = json.dumps(data, indent=4)
        with open(logfile, "w") as f:
            f.write(d)
            f.close()


def search_s1(boxes):
    result = {}
    files = 0
    log = []

    lastlog = readlog("s1")
    if lastlog != False:
        startdate = lastlog["time"]
    else:
        startdate = S1_STARTDATE

    for box in boxes:
        print(f"Searching for products in box {box}...")
        status, searchResult = mycop.productSearch(
            "Sentinel1",
            box=box,
            startDate=startdate,
            maxRecords=S1_MAXRECORDS,
            sortParam=S1_SORTPARAM,
            sortOrder=S1_SORTORDER,
            productType=S1_PRODUCTTYPE,
        )

        filelist = []
        for feature in searchResult["features"]:
            fileID = feature["id"]
            fileName = feature["properties"]["title"]
            print(f"{fileName}")
            if lastlog != False:
                if fileID in lastlog["files"]:
                    print("File found in log - not using it.")
                else:
                    filelist.append({fileID: fileName})
                    log.append(fileID)
                    files += 1
            else:
                filelist.append({fileID: fileName})
                log.append(fileID)
                files += 1
        result.update({box: filelist})
        print(f'Found {len(searchResult["features"])} products since {startdate}.')
        print()
    writelog("s1", log)
    return files, result


def search_s2(boxes):
    result = {}
    files = 0
    log = []

    lastlog = readlog("s2")
    if lastlog != False:
        startdate = lastlog["time"]
    else:
        startdate = S2_STARTDATE

    for box in boxes:
        print(f"Searching for products in box {box}...")
        status, searchResult = mycop.productSearch(
            "Sentinel2",
            cloudCover=S2_CLOUDCOVER,
            box=box,
            startDate=startdate,
            maxRecords=S2_MAXRECORDS,
            sortParam=S2_SORTPARAM,
            sortOrder=S2_SORTORDER,
            productType=S2_PRODUCTTYPE,
        )

        filelist = []
        for feature in searchResult["features"]:
            fileID = feature["id"]
            fileName = feature["properties"]["title"]
            print(f"{fileName}, {feature["properties"]["cloudCover"]}% cloud cover")
            if lastlog != False:
                if fileID in lastlog["files"]:
                    print("File found in log - not using it.")
                else:
                    filelist.append({fileID: fileName})
                    log.append(fileID)
                    files += 1
            else:
                filelist.append({fileID: fileName})
                log.append(fileID)
                files += 1
        result.update({box: filelist})
        print(f'Found {len(searchResult["features"])} products since {startdate}.')
        print()
    writelog("s2", log)
    return files, result


if __name__ == "__main__":
    print("----- Search-pipeline only -----")
    boxes = func.getBoxes(BOX)
    print("Sentinel 1")
    search_s1(boxes)
    print("Sentinel 2")
    search_s2(boxes)
