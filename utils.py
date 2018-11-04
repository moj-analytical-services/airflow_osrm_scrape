import tempfile
from urllib.request import urlretrieve
import zipfile
import os
import geopandas

def get_zipped_shapefile_from_url_and_return_local_path(url, unzip_to_dir):

    # We're going to use temporary files to download and unzip the ONS data.  This just means these temp files will be deleted for us at the end of the session
    zipped_shp = urlretrieve(url, "shapefiles.zip")

    with zipfile.ZipFile(zipped_shp[0],"r") as zip_ref:
        zip_ref.extractall(unzip_to_dir)

    os.remove("shapefiles.zip")

    shapefile_name = [f for f in os.listdir(unzip_to_dir) if f.endswith("shp")][0]
    shapefile_path = os.path.join(unzip_to_dir, shapefile_name)
    return shapefile_path
