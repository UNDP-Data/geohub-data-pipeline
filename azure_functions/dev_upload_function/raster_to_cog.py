import rasterio
from rasterio.io import MemoryFile
from rio_cogeo.cogeo import cog_translate, cog_validate

from .azure_clients import azure_container_client, rasterio_az_session
from .config import gdal_configs, logging

logger = logging.getLogger(__name__)


def translate_upload(src_file, dst_path, container_client, config={}, **options):
    """Convert image to COG and upload to azure."""

    config, output_profile = gdal_configs(config=config)

    with MemoryFile() as mem_dst:
        # Important, we pass `mem_dst.name` as output dataset path
        cog_translate(
            src_file,
            mem_dst.name,
            output_profile,
            config=config,
            in_memory=True,
            quiet=True,
            **options,
        )

        blob_client = container_client.get_blob_client(dst_path)
        blob_client.upload_blob(mem_dst.name, overwrite=True)

    return True


def raster_process(
    raster_path,
    profile="deflate",
    **options,
):
    """Opens the uploaded raster and attempts to translate if necesarry."""

    dst_path = r"new_directory" + raster_path
    with rasterio.Env(session=rasterio_az_session):
        with rasterio.open(raster_path, "r") as file:

            is_valid, errors, warnings = cog_validate(raster_path)

            if is_valid and not (warnings or errors):
                logger.info(
                    f"{raster_path} is already a valid cog. No translation needed"
                )
                return True
            else:
                logger.info(f"Beginning translation of {raster_path}")
                translate_upload(
                    file,
                    dst_path,
                    azure_container_client,
                    profile=profile,
                    config={"session": rasterio_az_session},
                    **options,
                )

    return True
