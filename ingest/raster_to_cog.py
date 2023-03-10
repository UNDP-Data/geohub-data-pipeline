from pathlib import Path

import rasterio
from rio_cogeo.cogeo import cog_translate

from ingest.config import datasets_folder, gdal_configs, logging, raw_folder
from ingest.ingest_exceptions import RasterUploadError
from ingest.utils import upload_error_blob, upload_ingesting_blob

logger = logging.getLogger(__name__)


async def ingest_raster(vsiaz_blob_path: str):
    # is_valid, errors, warnings = cog_validate(vsiaz_blob_path)
    config, output_profile = gdal_configs()
    # logger.info(f'using COG profile {json.dumps(dict(output_profile), indent=4)} and config {json.dumps(dict(config), indent=4)}')
    path = Path(vsiaz_blob_path)
    dname = str(path).replace(f"/{raw_folder}/", f"/{datasets_folder}/")
    fname, ext = path.name.rsplit(".", 1)
    try:
        with rasterio.open(vsiaz_blob_path, "r") as src_dataset:
            out_cog_dataset_path = f"{dname}/{fname}.tif"
            logger.info(f"Creating COG {out_cog_dataset_path}")
            await upload_ingesting_blob(out_cog_dataset_path)
            cog_translate(
                source=src_dataset,
                dst_path=out_cog_dataset_path,
                dst_kwargs=output_profile,
                config=config,
                web_optimized=True,
                forward_ns_tags=True,
                forward_band_tags=True,
                use_cog_driver=True,
            )
            logger.info(f"COG created: {out_cog_dataset_path}.")
            # logger.info(json.dumps(json.loads(cog_info(out_cog_dataset_path).json()), indent=4) )
    except RasterUploadError as e:
        logger.error(
            f"Error creating COG {out_cog_dataset_path}: {e}. Uploading error blob"
        )
        await upload_error_blob(out_cog_dataset_path)
