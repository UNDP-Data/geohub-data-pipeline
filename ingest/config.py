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

os.environ['AZURE_STORAGE_ACCESS_KEY'] = azure_storage_access_key
os.environ['AZURE_STORAGE_ACCOUNT'] = os.getenv('ACCOUNT_NAME')
os.environ['AZURE_STORAGE_CONNECTION_STRING'] = connection_string

def gdal_configs(config={}, profile='zstd'):
    """Generates a config dict and output profile for file."""
    # Format creation option (see gdalwarp `-co` option)
    '''
                            
        creationOptions=["BLOCKSIZE=256", "OVERVIEWS=IGNORE_EXISTING", "COMPRESS=ZSTD",     
                                       "PREDICTOR = YES", "OVERVIEW_RESAMPLING=NEAREST", "BIGTIFF=YES",     
                                            "TARGET_SRS=EPSG:3857", "RESAMPLING=NEAREST"])

    '''
    output_profile = cog_profiles.get(profile)



    output_profile.update({
                            'BIGTIFF':'YES',
                            'blockxsize':256,
                            'blockysize':256
                            }
                          )


    config["GDAL_NUM_THREADS"] = "ALL_CPUS"
    config['RESAMPLING']='NEAREST'
    config['OVERVIEW_RESAMPLING']='NEAREST'
    config['PREDICTOR']='YES'
    config['TARGET_SRS']='EPSG:3857'
    config["GDAL_TIFF_INTERNAL_MASK"] = True
    #config["GDAL_TIFF_OVR_BLOCKSIZE"] = "128"
    config["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES" # necessary to write files to AZ directkly using rio

    return config, output_profile
