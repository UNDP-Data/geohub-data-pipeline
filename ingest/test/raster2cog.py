import asyncio
import inspect
import json
import logging
import os
import time

import rasterio
from azure.storage.blob import BlobServiceClient as BlobServiceClientSync
from azure.storage.blob.aio import BlobServiceClient as BlobServiceClientAsync
from osgeo import gdal
from rio_cogeo import cog_info
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles

from ingest.config import account_url, connection_string, container_name


def flush_cache():
    """
    Flush Linux VM caches. Useful for doing meaningful tmei measurements for
    NetCDF or similar libs.
    Needs sudo password
    :return: bool, True if success, False otherwise
    """
    logger.debug(
        'Clearing the OS cache using sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches'
    )
    # ret = os.system('echo %s | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"' % passwd)
    # ret = os.popen('sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', 'w').write(passwd)
    ret = os.popen(
        'sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', "w"
    )  # for docker
    return not bool(ret)


def timeit(func=None, loops=1, verbose=False, clear_cache=False, sudo_passwd=None):
    # print 0, func, loops, verbose, clear_cache, sudo_passwd
    if func != None:
        # if clear_cache:
        #     assert sudo_passwd, 'sudo_password argument is needed to clear the kernel cache'

        def inner(*args, **kwargs):
            sums = 0.0
            mins = 1.7976931348623157e308
            maxs = 0.0
            logger.debug("====%s Timing====" % func.__name__)
            for i in range(0, loops):
                if clear_cache:
                    flush_cache()
                t0 = time.time()
                if inspect.iscoroutinefunction(func):
                    result = asyncio.run(func(*args, **kwargs))
                else:
                    result = func(*args, **kwargs)
                dt = time.time() - t0
                mins = dt if dt < mins else mins
                maxs = dt if dt > maxs else maxs
                sums += dt
                if verbose == True:
                    logger.info(
                        "\t%r ran in %2.9f sec on run %s" % (func.__name__, dt, i)
                    )
            logger.info("%r min run time was %2.9f sec" % (func.__name__, mins))
            logger.info("%r max run time was %2.9f sec" % (func.__name__, maxs))
            logger.info(
                "%r avg run time was %2.9f sec in %s runs"
                % (func.__name__, sums / loops, loops)
            )
            logger.debug("==== end ====")
            return result

        return inner
    else:

        def partial_inner(func):
            return timeit(func, loops, verbose, clear_cache, sudo_passwd)

        return partial_inner


@timeit(loops=10)
def ingest_raster():
    start_time = time.time()

    # Define input and output paths
    input_path = "/data/NE1_HR_LC_SR_W_DR.tif"
    # output_path = (
    #     "/vsiaz/userdata/c0ffc5cd6bdb861a394143c4e57e45d6/datasets/rasterio_cog.tif"
    # )
    output_path = "/data/rasterio_asynccog.tif"
    config, output_profile = gdal_configs()

    try:
        # start_time = time.time()
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
            # end_time = time.time()
            # print("Elapsed time: {:.2f} seconds".format(end_time - start_time))

        with BlobServiceClientSync.from_connection_string(
            connection_string
        ) as blob_service_client:
            out_az_path = (
                "c0ffc5cd6bdb861a394143c4e57e45d6/datasets/rasterio_syncupload_cog.tif"
            )
            with blob_service_client.get_blob_client(
                container=container_name, blob=out_az_path
            ) as blob_client:
                with open(output_path, "rb") as upload_gdalfile:
                    blob_client.upload_blob(
                        upload_gdalfile, overwrite=True, max_concurrency=8
                    )

    except Exception as e:
        logger.error(f"Error creating COG from {input_path}: {e}. Uploading error blob")

    return time.time() - start_time


@timeit(loops=10)
def test_gdal_translate_direct():
    # Define input and output paths
    input_path = "/data/GRAY_50M_SR.tif"
    output_path = "c0ffc5cd6bdb861a394143c4e57e45d6/datasets/gdal_direct_cog.tif"

    # Open the input dataset

    config, output_profile = gdal_configs()

    # Start timer
    # start_time = time.time()

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

    # # End timer
    # end_time = time.time()

    # Close the input dataset
    src_ds = None

    # Print elapsed time
    # print("Elapsed time: {:.2f} seconds".format(end_time - start_time))


@timeit(loops=10)
def test_gdal_translate_sync_upload():
    # Define input and output paths
    input_path = "/data/GRAY_50M_SR.tif"
    output_path = "/data/gdal_synccog.tif"

    # Open the input dataset

    config, output_profile = gdal_configs()

    # Start timer
    # start_time = time.time()

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

    # # End timer
    # end_time = time.time()

    # Close the input dataset
    src_ds = None
    blob_service_client = BlobServiceClientSync(
        account_url=account_url,
        credential=connection_string,
        max_single_put_size=4 * 1024 * 1024,
        max_single_get_size=4 * 1024 * 1024,
    )
    out_az_path = "c0ffc5cd6bdb861a394143c4e57e45d6/datasets/gdal_az_syncupload_cog.tif"
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=out_az_path
    )

    with open("/data/gdal_cog.tif", "rb") as upload_gdalfile:
        blob_client.upload_blob(upload_gdalfile, overwrite=True)


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
    config["NUM_THREADS"] = "ALL_CPUS"
    config["TARGET_SRS"] = "EPSG:3857"
    config["GDAL_TIFF_INTERNAL_MASK"] = True
    # config["GDAL_TIFF_OVR_BLOCKSIZE"] = "128"
    config[
        "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"
    ] = "YES"  # necessary to write files to AZ directly using rio

    return config, output_profile


@timeit(loops=10)
async def run_test_gdal_translate_async_upload():
    input_path = "/data/NE1_HR_LC_SR_W_DR.tif"
    output_path = "/data/gdal_asynccog.tif"
    start_time = time.time()

    # Run gdal.Translate function
    gdal.Translate(
        output_path,
        input_path,
        format="COG",
        creationOptions=[
            "NUM_THREADS=ALL_CPUS",
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

    logger.info(f"Is the cog valid? {cog_validate(output_path)[0]}")

    async with BlobServiceClientAsync.from_connection_string(
        connection_string
    ) as blob_service_client:
        out_az_path = (
            "c0ffc5cd6bdb861a394143c4e57e45d6/datasets/gdal_az_asyncupload_cog.tif"
        )
        async with blob_service_client.get_blob_client(
            container=container_name, blob=out_az_path
        ) as blob_client:
            with open(output_path, "rb") as upload_gdalfile:
                await blob_client.upload_blob(upload_gdalfile, overwrite=True)


async def run_tests(num_tests: int):
    gdal_times = []
    rasterio_times = []

    for i in range(num_tests):
        # Delete output file if it exists
        if os.path.exists("/data/gdal_asynccog.tif"):
            os.remove("/data/gdal_asynccog.tif")
        if os.path.exists("/data/rasterio_asynccog.tif"):
            os.remove("/data/rasterio_asynccog.tif")
        async with BlobServiceClientAsync.from_connection_string(
            connection_string
        ) as blob_service_client:
            out_gdal_path = (
                "c0ffc5cd6bdb861a394143c4e57e45d6/datasets/gdal_az_asyncupload_cog.tif"
            )
            async with blob_service_client.get_blob_client(
                container=container_name, blob=out_gdal_path
            ) as blob_client:
                if blob_client.exists():
                    blob_client.delete_blob()
            out_rasterio_path = (
                "c0ffc5cd6bdb861a394143c4e57e45d6/datasets/rasterio_asyncupload_cog.tif"
            )
            async with blob_service_client.get_blob_client(
                container=container_name, blob=out_rasterio_path
            ) as blob_client:
                if blob_client.exists():
                    blob_client.delete_blob()
        logger.info(f"Running test {i+1}")

        gdal_time_elapsed = await run_test_gdal_translate_async_upload()
        logger.info(f"Gdal run {i+1} time elapsed: {gdal_time_elapsed} seconds")

        rasterio_time_elapsed = ingest_raster()
        logger.info(f"Rasterio run {i+1} time elapsed: {rasterio_time_elapsed} seconds")

        gdal_times.append(gdal_time_elapsed)
        rasterio_times.append(rasterio_time_elapsed)

    avg_gdal_time = sum(gdal_times) / len(gdal_times)
    logger.info(f"All gdal times: {gdal_times}")
    logger.info(f"Average gdal time elapsed: {avg_gdal_time} seconds")
    avg_rasterio_time = sum(rasterio_times) / len(rasterio_times)
    logger.info(f"All times: {avg_rasterio_time}")
    logger.info(f"Average rasterio time elapsed: {avg_rasterio_time} seconds")


if __name__ == "__main__":
    # docker compose build
    # docker run --rm --env-file .env -v b:/UNDP/dev/geohub-data-pipeline/ingest/test/data:/data -it geohub-data-pipeline-app:latest python -m ingest.test.raster2cog

    import logging

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ingest_raster()
    # asyncio.run(run_tests(10))
    # asyncio.run(run_test_gdal_translate_async_upload())
    # test_gdal_translate_sync_upload()
    # test_gdal_translate_direct()
