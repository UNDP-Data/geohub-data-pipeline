import json
import logging
import time

import rasterio
from osgeo import gdal
from rio_cogeo import cog_info
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logger = logging.getLogger(__name__)


def ingest_raster():
    # Define input and output paths
    input_path = "b:/UNDP/dev/geohub-data-pipeline/ingest/test/GRAY_50M_SR.tif"
    output_path = "rasterio_cog.tif"

    config, output_profile = gdal_configs()

    try:
        start_time = time.time()
        with rasterio.open(input_path, "r") as src_dataset:
            if src_dataset.colorinterp:
                cog_translate(
                    source=src_dataset,
                    dst_path=output_path,
                    dst_kwargs=output_profile,
                    config=config,
                    web_optimized=True,
                    forward_ns_tags=True,
                    forward_band_tags=True,
                    use_cog_driver=True,
                )
            else:
                for bandindex in src_dataset.indexes:
                    logger.info(f"Converting band {bandindex} from {input_path}")
                    cog_translate(
                        source=src_dataset,
                        dst_path=output_path,
                        indexes=[bandindex],
                        dst_kwargs=output_profile,
                        config=config,
                        web_optimized=True,
                        forward_ns_tags=True,
                        forward_band_tags=True,
                        use_cog_driver=False,
                    )

            # logger.info(json.dumps(json.loads(cog_info(out_cog_dataset_path).json()), indent=4) )
            # exit()
            end_time = time.time()
            print("Elapsed time: {:.2f} seconds".format(end_time - start_time))

    except Exception as e:
        logger.error(f"Error creating COG from {input_path}: {e}. Uploading error blob")


def test_gdal_translate():
    # Define input and output paths
    input_path = "b:/UNDP/dev/geohub-data-pipeline/ingest/test/GRAY_50M_SR.tif"
    output_path = "gdal_cog.tif"

    # Open the input dataset

    config, output_profile = gdal_configs()

    # Start timer
    start_time = time.time()

    # Translate the dataset to COG
    gdal.Translate(
        output_path,
        input_path,
        format="COG",
        creationOptions=[
            "BLOCKSIZE=256",
            "OVERVIEWS=IGNORE_EXISTING",
            "COMPRESS=ZSTD",
            "PREDICTOR = YES",
            "OVERVIEW_RESAMPLING=NEAREST",
            "BIGTIFF=YES",
            "TARGET_SRS=EPSG:3857",
            "RESAMPLING=NEAREST",
        ],
    )

    # End timer
    end_time = time.time()

    # Close the input dataset
    src_ds = None

    # Print elapsed time
    print("Elapsed time: {:.2f} seconds".format(end_time - start_time))


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
    config["RESAMPLING"] = "NEAREST"
    config["OVERVIEWS"] = "IGNORE_EXISTING"
    config["OVERVIEW_RESAMPLING"] = "NEAREST"
    config["PREDICTOR"] = "YES"
    config["TARGET_SRS"] = "EPSG:3857"
    config["GDAL_TIFF_INTERNAL_MASK"] = True
    # config["GDAL_TIFF_OVR_BLOCKSIZE"] = "128"
    config[
        "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"
    ] = "YES"  # necessary to write files to AZ directkly using rio

    return config, output_profile


ingest_raster()
test_gdal_translate()
