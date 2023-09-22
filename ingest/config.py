import logging
import os
from azure.messaging.webpubsubservice import WebPubSubServiceClient
from rio_cogeo.profiles import cog_profiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

raw_folder = "raw"
datasets_folder = "datasets"

attribution = "United Nations Development Programme (UNDP)"

# account_url = os.getenv("ACCOUNT_URL")
# assert account_url is not None, f"ACCOUNT_URL env var is not set"
# container_name = os.getenv("CONTAINER_NAME")
# assert container_name is not None, f"CONTAINER_NAME env var is not set"
# azure_storage_access_key = os.getenv("AZURE_ACCESS_KEY")
# assert azure_storage_access_key is not None, f"AZURE_ACCESS_KEY env var is not set"
# connection_string = os.getenv("CONNECTION_STRING")
# assert connection_string is not None, f"CONNECTION_STRING env var is not set"
# account_name = os.getenv("ACCOUNT_NAME")
# assert account_name is not None, f"ACCOUNT_NAME env var is not set"
#
# os.environ["AZURE_STORAGE_ACCESS_KEY"] = azure_storage_access_key
# os.environ["AZURE_STORAGE_ACCOUNT"] = account_name
# os.environ["AZURE_STORAGE_CONNECTION_STRING"] = connection_string
AZURE_WEBPUBSUB_GROUP_NAME = 'datapipeline'
GDAL_ARCHIVE_FORMATS = {
    ".zip": "vsizip",
    ".gz": "vsigzip",
    ".tar": "vsitar",
    ".tgz": "vsitar",
    ".7z": "vsi7z",
}

OUT_FORMATS = {'.pmtiles': 'PMTiles', '.tif':'GeoTIFF', 'tiff':'GeoTIFF'}

"""
GeoTIFF (.tif, .tiff)

NetCDF (.nc)

Arc/Info ASCII Grid File (.aig, .asc, .sgr, .grd)

Erdas Imagine (.raw, .bl)

ESRI Shapefile (zipped) (.zip)

GeoJSON (.geojson)

PMTILES (.pmtiles)

MBTILES (.mbtiles)

ESRI File Geodatabase (OpenFileGDB) (.gdb)

ESRI File Geodatabase (FileGDB) (.gdb) uses ESRI SDK


GeoPackage (.gpkg)

"""

ALLOWED_GDAL_FORMATS = [
    ".tif",
    ".tiff",
    ".gtif",
    ".gtiff",
    ".nc",
    ".nc4",
    ".aig",
    ".asc",
    ".sgr",
    ".grd",
]


def gdal_configs(config={}, profile="zstd"):
    """Generates a config dict and output profile for file."""
    # Format creation option (see gdalwarp `-co` option)
    """
                            
        creationOptions=["BLOCKSIZE=256", "OVERVIEWS=IGNORE_EXISTING", "COMPRESS=ZSTD",     
                                       "PREDICTOR = YES", "OVERVIEW_RESAMPLING=NEAREST", "BIGTIFF=YES",     
                                            "TARGET_SRS=EPSG:3857", "RESAMPLING=NEAREST"])

    """
    output_profile = cog_profiles.get(profile)

    output_profile.update({"BIGTIFF": "YES", "blockxsize": 256, "blockysize": 256})

    config["GDAL_NUM_THREADS"] = "ALL_CPUS"
    config["GDAL_DISABLE_READDIR_ON_OPEN"] = "TRUE"
    config["RESAMPLING"] = "NEAREST"
    config["OVERVIEWS"] = "IGNORE_EXISTING"
    config["OVERVIEW_RESAMPLING"] = "NEAREST"
    config["PREDICTOR"] = "YES"
    config["TARGET_SRS"] = "EPSG:3857"
    config["GDAL_TIFF_INTERNAL_MASK"] = True
    config["OGR_ORGANIZE_POLYGONS"] = "DEFAULT" # for parsing ArcGIS GDB correctly

    # config["GDAL_TIFF_OVR_BLOCKSIZE"] = "128"
    config[
        "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"
    ] = "YES"  # necessary to write files to AZ directkly using rio

    return config, output_profile


def get_azurewebsubpub_client_token(group_name=AZURE_WEBPUBSUB_GROUP_NAME, hub='Hub', minutes_to_expire=60):
    AZURE_WEBPUBSUB_CONNECTION_STRING = os.environ.get('AZURE_WEBPUBSUB_CONNECTION_STRING')
    assert AZURE_WEBPUBSUB_CONNECTION_STRING not in ('', None), f'AZURE_WEBPUBSUB_CONNECTION_STRING env var is not valid'
    # create a websubpub service client
    with WebPubSubServiceClient.from_connection_string(
            connection_string=AZURE_WEBPUBSUB_CONNECTION_STRING,
            hub=hub
        ) as service_client:
        token = service_client.get_client_access_token(
            user_id='geohub-data-pipeline',
            roles=[f"webpubsub.joinLeaveGroup.{group_name}",
                   f"webpubsub.sendToGroup.{group_name}"],
            minutes_to_expire=minutes_to_expire
        )
        return token


def setup_env_vars():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    assert connection_string is not None, f"AZURE_STORAGE_CONNECTION_STRING env var is not set"
    account_name = connection_string.split(';')[1].split('=')[1]
    os.environ["AZURE_STORAGE_ACCOUNT"] = account_name
    service_bus_connection = os.environ['SERVICE_BUS_CONNECTION_STRING']
    queue_name = service_bus_connection.split(';')[-1].split('=')[1]
    os.environ['SERVICE_BUS_QUEUE_NAME'] = queue_name