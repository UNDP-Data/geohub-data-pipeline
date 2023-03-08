import json
import os.path
from pathlib import Path

import rasterio
from azure.storage.blob.aio import BlobServiceClient
from rio_cogeo.cogeo import cog_translate

from ingest.config import connection_string, container_name, gdal_configs, logging

logger = logging.getLogger(__name__)


async def ingest_raster(vsiaz_blob_path: str):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # is_valid, errors, warnings = cog_validate(vsiaz_blob_path)
    config, output_profile = gdal_configs()
    # logger.info(f'using COG profile {json.dumps(dict(output_profile), indent=4)} and config {json.dumps(dict(config), indent=4)}')
    path = Path(vsiaz_blob_path)
    dname = str(path).replace("/raw/", "/datasets/")
    fname, ext = path.name.rsplit(".", 1)

    with rasterio.open(vsiaz_blob_path, "r") as src_dataset:
        for bandindex in src_dataset.indexes:
            out_cog_dataset_path = f"{dname}/{fname}_band{bandindex}.{ext}"
            logger.info(f"Creating COG {out_cog_dataset_path}")

            ingesting_cog_path = f"{out_cog_dataset_path}.ingesting"
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=ingesting_cog_path
            )
            await blob_client.upload_blob(b"ingesting", overwrite=True)

            logger.info(
                f"Converting band {bandindex} from {vsiaz_blob_path.replace('/vsiaz/', '')}"
            )
            cog_translate(
                source=src_dataset,
                dst_path=out_cog_dataset_path,
                indexes=[bandindex],
                dst_kwargs=output_profile,
                config=config,
                web_optimized=True,
                forward_ns_tags=True,
                forward_band_tags=True,
                use_cog_driver=True,
            )

            await blob_client.delete_blob()
            logger.info(f"COG created: {out_cog_dataset_path}. Ingesting file deleted.")
            # logger.info(json.dumps(json.loads(cog_info(out_cog_dataset_path).json()), indent=4) )
