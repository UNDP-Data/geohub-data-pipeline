import rasterio
from rasterio.io import MemoryFile
from rasterio.session import AzureSession
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles


def main():

    print("starting processing")
    raster_path = r"\path\to\local\geo.tif"
    process_test(raster_path)


def _translate_test(
    src_path, dst_path, profile="deflate", profile_options={}, **options
):
    """Convert image to COG."""
    # Format creation option (see gdalwarp `-co` option)
    output_profile = cog_profiles.get(profile)
    output_profile.update(dict(BIGTIFF="IF_SAFER"))
    output_profile.update(profile_options)

    # Dataset Open option (see gdalwarp `-oo` option)
    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_OVR_BLOCKSIZE="128",
    )

    cog_translate(
        src_path,
        dst_path,
        output_profile,
        config=config,
        in_memory=False,
        quiet=True,
        **options,
    )
    return True


def process_test(
    raster_path,
    profile="deflate",
    profile_options={},
    **options,
):

    dst_path = r"\path\to\output\geo.tif"

    with rasterio.open(raster_path, "r") as file:

        is_valid, errors, warnings = cog_validate(raster_path)

        if is_valid and not (warnings or errors):
            print("is already a valid cog")
            return True
        else:
            print("beginnning translating")
            _translate_test(
                file,
                dst_path,
                profile=profile,
                profile_options=profile_options,
                **options,
            )

    return True


if __name__ == "__main__":
    main()
