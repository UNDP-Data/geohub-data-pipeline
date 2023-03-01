import json
import os.path
import sys

import rasterio
from rasterio.io import MemoryFile
from rio_cogeo.cogeo import cog_translate, cog_validate, cog_info

from .config import gdal_configs, logging
from .utils import prepare_filename, upload_file

logger = logging.getLogger(__name__)


def translate_upload(src_data, dst_path, config={}, **options):
    """Convert image to COG and upload to azure."""

    config, output_profile = gdal_configs(config=config)

    logging.info(f"Translating {src_data} to COG")
    with MemoryFile() as mem_dst:
        # Important, we pass `mem_dst.name` as output dataset path
        cog_translate(
            src_data,
            mem_dst.name,
            output_profile,
            config=config,
            in_memory=True,
            quiet=True,
            **options,
        )

        upload_file(mem_dst, dst_path)
    return True


def raster_ingest(
    raster_path,
    **options,
):
    """Opens the uploaded raster and attempts to translate if necesarry."""

    logging.info(f"Validating {raster_path} as COG")
    dst_path = prepare_filename(raster_path, directory="datasets")
    logging.info(f'ratster_path exists: {os.path.exists(raster_path) }')
    logging.info(f'dst_path={dst_path}')
    with rasterio.open(raster_path, "r") as src_dataset:

        is_valid, errors, warnings = cog_validate(raster_path)

        if is_valid and not (warnings or errors):
            logger.info(f"{raster_path} is already a valid cog. No translation needed")
            upload_file(src_dataset, dst_path)
            return True
        else:
            translate_upload(
                src_dataset,
                dst_path,
                **options,
            )

    return True


def ingest_raster(vsiaz_blob_path=None):
    #is_valid, errors, warnings = cog_validate(vsiaz_blob_path)
    config, output_profile = gdal_configs()
    #logger.info(f'using COG profile {json.dumps(dict(output_profile), indent=4)} and config {json.dumps(dict(config), indent=4)}')
    _, file_name = os.path.split(vsiaz_blob_path)
    with rasterio.open(vsiaz_blob_path, "r") as src_dataset:
        for bandindex in src_dataset.indexes:
            dname = vsiaz_blob_path.replace('/working/', '/datasets/')
            fname, ext = os.path.splitext(file_name)
            out_cog_dataset_path = f"{dname}/{fname}_band{bandindex}{ext}"
            logger.info(f"Converting band {bandindex} from {vsiaz_blob_path.replace('/vsiaz/', '')}")
            cog_translate(source=src_dataset,
                          dst_path=out_cog_dataset_path,
                          indexes=[bandindex],
                          dst_kwargs=output_profile,
                          config=config,
                          web_optimized=True,
                          forward_ns_tags=True,
                          forward_band_tags=True,
                          use_cog_driver=True
                          )

            #logger.info(json.dumps(json.loads(cog_info(out_cog_dataset_path).json()), indent=4) )



