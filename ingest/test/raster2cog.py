import json
import logging
import time
import os
import rasterio
from osgeo import gdal
from rio_cogeo import cog_info
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles



def flush_cache():
    """
    Flush Linux VM caches. Useful for doing meaningful tmei measurements for
    NetCDF or similar libs.
    Needs sudo password
    :return: bool, True if success, False otherwise
    """
    logger.debug('Clearing the OS cache using sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches')
    #ret = os.system('echo %s | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"' % passwd)
    #ret = os.popen('sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', 'w').write(passwd)
    ret = os.popen('sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', 'w') # for docker
    return not bool(ret)

def timeit(func=None,loops=1,verbose=False, clear_cache=False, sudo_passwd=None):
    #print 0, func, loops, verbose, clear_cache, sudo_passwd
    if func != None:
        # if clear_cache:
        #     assert sudo_passwd, 'sudo_password argument is needed to clear the kernel cache'

        def inner(*args,**kwargs):
            sums = 0.0
            mins = 1.7976931348623157e+308
            maxs = 0.0
            logger.debug('====%s Timing====' % func.__name__)
            for i in range(0,loops):
                if clear_cache:
                    flush_cache()
                t0 = time.time()
                result = func(*args,**kwargs)
                dt = time.time() - t0
                mins = dt if dt < mins else mins
                maxs = dt if dt > maxs else maxs
                sums += dt
                if verbose == True:
                    logger.debug('\t%r ran in %2.9f sec on run %s' %(func.__name__,dt,i))
            logger.debug('%r min run time was %2.9f sec' % (func.__name__,mins))
            logger.debug('%r max run time was %2.9f sec' % (func.__name__,maxs))
            logger.info('%r avg run time was %2.9f sec in %s runs' % (func.__name__,sums/loops,loops))
            logger.debug('==== end ====')
            return result

        return inner
    else:
        def partial_inner(func):
            return timeit(func,loops,verbose, clear_cache, sudo_passwd)
        return partial_inner



@timeit(loops=10)
def ingest_raster():
    # Define input and output paths
    input_path = "/data/GRAY_50M_SR.tif"
    output_path = "/data/rasterio_cog.tif"

    config, output_profile = gdal_configs()

    try:
        #start_time = time.time()
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
            #end_time = time.time()
            #print("Elapsed time: {:.2f} seconds".format(end_time - start_time))

    except Exception as e:
        logger.error(f"Error creating COG from {input_path}: {e}. Uploading error blob")

@timeit(loops=10)
def test_gdal_translate():
    # Define input and output paths
    input_path = "/data/GRAY_50M_SR.tif"
    output_path = "/data/gdal_cog.tif"

    # Open the input dataset

    config, output_profile = gdal_configs()

    # Start timer
    #start_time = time.time()

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
    #print("Elapsed time: {:.2f} seconds".format(end_time - start_time))


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
    ] = "YES"  # necessary to write files to AZ directly using rio

    return config, output_profile
if __name__ == '__main__':
    #docker compose build
    #docker run --rm  -v /work/py/geohub-data-pipeline/ingest/test/data:/data -it geohub-data-pipeline_app:latest python -m ingest.test.raster2cog

    import logging
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ingest_raster()
    test_gdal_translate()
