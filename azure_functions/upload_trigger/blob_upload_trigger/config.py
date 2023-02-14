import logging
import os

from rio_cogeo.profiles import cog_profiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


account_url = os.environ.get("ACCOUNT_URL")
container_name = os.environ.get("CONTAINER_NAME")
credential_string = os.environ.get("CREDENTIAL_STRING")

azure_account = os.environ.get("ACCOUNT_NAME")
azure_storage_access_key = os.environ.get("AZURE_ACCESS_KEY")


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
