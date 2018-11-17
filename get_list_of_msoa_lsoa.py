import pandas as pd
import geopandas
import os
from utils import get_zipped_shapefile_from_url_and_return_local_path

## We should have 34,753 LSOAs and 7,201 MSOAS in total from the 2011 census
## From https://www.ons.gov.uk/methodology/geography/ukgeographies/censusgeography

def get_lsoas():
    """
    Return a pandas dataframe of all lsoa11 codes,
    Columns in df are
    """

    # We want the lat/lng coordinates of each LSOA.  Seems most sensible to use population weighted centroids
    # Lower Layer Super Output Areas (December 2011) Population Weighted Centroids http://geoportal1-ons.opendata.arcgis.com/datasets/b7c49538f0464f748dd7137247bbc41c_0
    # Url is the url for the shapefile
    url = "https://opendata.arcgis.com/datasets/b7c49538f0464f748dd7137247bbc41c_0.zip"
    shapefile_path = get_zipped_shapefile_from_url_and_return_local_path(url, "temp_lsoa/")
    lsoas = geopandas.read_file(shapefile_path)
    assert(len(lsoas) == 34753)

    lsoas["lng"] = lsoas["geometry"].x
    lsoas["lat"] = lsoas["geometry"].y

    lsoas = lsoas[["lsoa11cd", "lat", "lng"]]
    lsoas = lsoas.rename(columns={"lsoa11cd": "geography_id"})
    lsoas["geography_type"] = "lsoa11cd"

    return lsoas


def get_msoas():

    # We want the lat/lng coordinates of each MSOA.  Seems most sensible to use population weighted centroids
    # Middle Layer Super Output Areas (December 2011) Population Weighted Centroids http://geoportal.statistics.gov.uk/datasets/b0a6d8a3dc5d4718b3fd62c548d60f81_0
    # Url is the url for the shapefile
    url = "https://opendata.arcgis.com/datasets/b0a6d8a3dc5d4718b3fd62c548d60f81_0.zip"
    shapefile_path = get_zipped_shapefile_from_url_and_return_local_path(url, "temp_msoa/")
    msoas = geopandas.read_file(shapefile_path)
    assert(len(msoas) == 7201)

    msoas["lng"] = msoas["geometry"].x
    msoas["lat"] = msoas["geometry"].y

    msoas = msoas[["msoa11cd", "lat", "lng"]]
    msoas = msoas.rename(columns={"msoa11cd": "geography_id"})
    msoas["geography_type"] = "msoa11cd"

    return msoas

