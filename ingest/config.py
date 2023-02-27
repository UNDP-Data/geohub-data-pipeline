import logging
import os

from rio_cogeo.profiles import cog_profiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

account_url = os.getenv("ACCOUNT_URL")
assert account_url is not None, f'ACCOUNT_URL env var is not set'
container_name = os.getenv("CONTAINER_NAME")
assert container_name is not None, f'CONTAINER_NAME env var is not set'
azure_storage_access_key = os.getenv("AZURE_ACCESS_KEY")
assert azure_storage_access_key is not None, f'AZURE_ACCESS_KEY env var is not set'
connection_string = os.getenv('CONNECTION_STRING')
assert connection_string is not None, f'CONNECTION_STRING env var is not set'

def gdal_configs(config={}, profile="deflate"):
    """Generates a config dict and output profile for file."""
    # Format creation option (see gdalwarp `-co` option)
    output_profile = cog_profiles.get(profile)
    output_profile.update(dict(BIGTIFF="IF_SAFER"))

    # Dataset Open option (see gdalwarp `-oo` option)
    config["GDAL_NUM_THREADS"] = "ALL_CPUS"
    config["GDAL_TIFF_INTERNAL_MASK"] = True
    config["GDAL_TIFF_OVR_BLOCKSIZE"] = "128"

    return config, output_profile