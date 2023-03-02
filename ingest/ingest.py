import logging
import os

from fastapi import APIRouter, HTTPException

from .azure_clients import copy_raw2working
from .raster_to_cog import ingest_raster
from .utils import gdal_open
from .vector_to_tiles import ingest_vector

logger = logging.getLogger(__name__)

app_router = APIRouter()
# silence azure logger
azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azlogger.setLevel(logging.WARNING)


@app_router.get("/ingest1")
async def ingest(blob_path: str, token=None):
    """
    Ingest a geospatial data file potentially containing multiple layers
    into geohub
    Follows exactly https://github.com/UNDP-Data/geohub/discussions/545

    """
    logger.info(f"Starting to ingest {blob_path}")
    # 1 copy data file to working folder
    if not "/raw/" in blob_path:
        return HTTPException(
            status_code=400, detail=f"blob_path is not located in raw folder"
        )
    try:
        working_blob_path = await copy_raw2working(raw_blob_path=blob_path)
    except Exception as ce:
        # import traceback
        # import io
        # s = io.StringIO()
        # traceback.print_exc(file=s)
        # logger.info(s.getvalue())
        raise HTTPException(
            status_code=500,
            detail=f"Could not copy {blob_path} to working directory. {ce}",
        )

    # 2 create the datasets folder that will hold all physical files. It does not make
    # sense to physically create the folder because folders are not real in azure
    dataset_folder = blob_path.replace(
        "/raw/", "/datasets/"
    )  # the folder name  will contain the extension
    logger.info(dataset_folder)
    path = f"/vsiaz/{working_blob_path}"
    nrasters, nvectors = gdal_open(path)

    # 3 ingest
    if nrasters > 0:
        ingest_raster(vsiaz_blob_path=path)
    if nvectors > 0:
        await ingest_vector(vsiaz_blob_path=path)

    return f"Finished ingesting {blob_path}"
