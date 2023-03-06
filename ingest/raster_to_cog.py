import json
import os.path

import rasterio
from rio_cogeo.cogeo import cog_info, cog_translate, cog_validate

from ingest.config import gdal_configs, logging

logger = logging.getLogger(__name__)


def ingest_raster(vsiaz_blob_path: str):
    # is_valid, errors, warnings = cog_validate(vsiaz_blob_path)
    config, output_profile = gdal_configs()
    # logger.info(f'using COG profile {json.dumps(dict(output_profile), indent=4)} and config {json.dumps(dict(config), indent=4)}')
    _, file_name = os.path.split(vsiaz_blob_path)
    with rasterio.open(vsiaz_blob_path, "r") as src_dataset:
        for bandindex in src_dataset.indexes:
            dname = vsiaz_blob_path.replace("/working/", "/datasets/")
            fname, ext = os.path.splitext(file_name)
            out_cog_dataset_path = f"{dname}/{fname}_band{bandindex}{ext}"
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

            # logger.info(json.dumps(json.loads(cog_info(out_cog_dataset_path).json()), indent=4) )
