# python-sentinel-pipeline

(C) 2025 Stefan Gofferje

Licensed under the GNU General Public License V3 or later.

> [!CAUTION]
> This project is under heavy development and currently not ready to be used without Python knowledge.
> I will make this a docker container at a later point but for the moment the project should be
> considered more like a tech preview than a ready product.

> [!WARNING]
> Sentinel 2 datafiles are BIG. For Sentinel 2 processing you will need at least 16GB of RAM. Also make sure,
> you have ample disk space!

## Description

This project is a fully automatic pipeline to download a set of satellite images from
the [ESA Copernicus Dataspace Ecosystem (CDSE)](https://dataspace.copernicus.eu/) and do the post processing.

## Features

### Sentinel 1 pipeline

- Automatic search and download of imagery from a bounding box or list of bounding boxes
- Automatic postprocessing
  - Contrast normalization by percentile
- Automatic creation of
  - VV and VH greyscale images
  - VV/VH and VH/VV ratio pseudocolor images
  - VV\*VH product pseudocolor images
  - VV-VH difference pseudocolor images

### Sentinel 2 pipeline

- Automatic search and download of imagery from a bounding box or list of bounding boxes
- Automatic postprocessing
  - Contrast normalization by percentile
- Automatic creation of
  - True color images
  - NIR false color images (red=NIR, green=green, blue=blue)
  - Atmospheric penetration images (red=2190nm, green=1610nm, blue=NIR)
  - NDVI images

## Configuration

The following values are supported and can be provided either as environment variables or through an .env-file.

### Connection

| Variable            | Default | Mandatory | Purpose       |
| ------------------- | ------- | --------- | ------------- |
| COPERNICUS_USERNAME | empty   | yes       | CDSE username |
| COPERNICUS_PASSWORD | empty   | yes       | CDSE password |

### General

| Variable  | Default | Mandatory | Purpose                                                                                                                                                                                                                                                  |
| --------- | ------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| BOX       | empty   | yes       | Box from which to search products in format East, South, West, North. E.g. "24.461060, 60.081284, 25.455322, 60.348696". Suitable boxes can be created e.g. with [bbox finder](http://bboxfinder.com).                                                   |
| USE_LOG   | True    | no        | Read and write search log. If True, the search will read the date and time of the last search from the log and use that as startDate. It will also read the list of file UUIDs from the last search and ignore them if they come up in the search again. |
| PIPELINES | "S1,S2" | no        | Which pipelines to run, S1 = Sentinel 1, S2 = Sentinel 2                                                                                                                                                                                                 |

### Sentinel 1 pipeline

| Variable       | Default           | Mandatory | Purpose                                                                                                                                                             |
| -------------- | ----------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| S1_MAXRECORDS  | 5                 | no        | Maximum amount of products to return                                                                                                                                |
| S1_PROCESSES   | "VV,VH,RATIOVVVH" | no        | Which post processing processes to run. VV = VV band greyscale, VH = VH band greyscale, RATIOVVVH = pseudocolor VV,VH,VV/VH, RATIOVHVV = pseudocolor VV,VH,VH/VV    |
| S1_PRODUCTTYPE | GRD               | no        | Which product type to search for. Stay away from SLC data! The files are extremely huge and I have not been able to get them to process without running out of RAM. |
| S1_SORTORDER   | descending        | no        | Which direction the results should be sorted                                                                                                                        |
| S1_SORTPARAM   | startDate         | no        | Which parameter to sort the results by                                                                                                                              |
| S1_STARTDATE   | yesterday         | no        | Start date of the search                                                                                                                                            |

### Sentinel 2 pipeline

| Variable       | Default             | Mandatory | Purpose                                                                                                                                                                    |
| -------------- | ------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| S2_CLOUDCOVER  | 5                   | no        | Maximum cloud cover                                                                                                                                                        |
| S2_MAXRECORDS  | 5                   | no        | Maximum amount of products to return                                                                                                                                       |
| S2_PROCESSES   | "TCI,NIRFC,AP,NDVI" | no        | Which post processing processes to run. TCI = True color image, NIRFC = NIR false color image, AP = atmospheric penetration, NDVI = Normalized Difference Vegetation Index |
| S2_PRODUCTTYPE | L2A                 | no        | Which product type to search for. L2A gets the best results. L1C works too but the image quality isn't that great.                                                         |
| S2_SORTORDER   | descending          | no        | Which direction the results should be sorted                                                                                                                               |
| S2_SORTPARAM   | startDate           | no        | Which parameter to sort the results by                                                                                                                                     |
| S2_STARTDATE   | yesterday           | no        | Start date of the search                                                                                                                                                   |

## Usage

Assuming you read the caution above, here is how you can try the project out.

> [!NOTE]
> The instructions are for Linux only because I don't really use Windows

1. Make sure, Python 3, pip and libgdal are installed
2. Clone the repo to a convenient place
3. Create a virtual environment with `python -m venv venv`
4. Activate the virtual environment with `source venv/bin/activate`
5. Install the required python modules with `pip install -r requirements.txt`
6. Copy .env.example to .env and edit according to your needs
7. Test the pipeline with `python search.py`. If that goes through and shows you results,
   you can run the pipeline, otherwise, check your config.
8. Run the pipeline with `python pipelines.py`
