import logging
import os

from fastapi import APIRouter, HTTPException

from ingest.azure_clients import copy_raw2working
from ingest.config import datasets_folder, raw_folder
from ingest.raster_to_cog import ingest_raster
from ingest.utils import gdal_open
from ingest.vector_to_tiles import ingest_vector

logger = logging.getLogger(__name__)

app_router = APIRouter()
# silence azure logger
azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azlogger.setLevel(logging.WARNING)


@app_router.get("/ingest")
async def ingest(blob_path: str, token=None):
    """
    Ingest a geospatial data file potentially containing multiple layers
    into geohub
    Follows exactly https://github.com/UNDP-Data/geohub/discussions/545

    """
    logger.info(f"Starting to ingest {blob_path}")
    # 1 create the datasets folder that will hold all physical files. It does not make
    # sense to physically create the folder because folders are not real in azure
    dataset_folder = blob_path.replace(
        f"/{raw_folder}/", f"/{datasets_folder}/"
    )  # the folder name  will contain the extension
    logger.debug(dataset_folder)

    path = f"/vsiaz/{blob_path}"
    nrasters, nvectors = gdal_open(path)

    # 2 ingest
    if nrasters > 0:
        ingest_raster(vsiaz_blob_path=path)
    if nvectors > 0:
        await ingest_vector(vsiaz_blob_path=path)

    return f"Finished ingesting {blob_path}"
