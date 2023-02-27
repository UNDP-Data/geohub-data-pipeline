import logging

from fastapi import APIRouter, HTTPException

from .config import logging
from .raster_to_cog import raster_ingest
from .utils import prepare_file, gdal_open
from .vector_to_tiles import vector_ingest
from .azure_clients import copy_raw2working
import json
logger = logging.getLogger(__name__)

app_router = APIRouter()
#silence azure logger
azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azlogger.setLevel(logging.WARNING)





@app_router.get("/ingest1")
async def ingest(blob_path:str=None):
    """
    Ingest a geospatial data file potentially containing multiple layers
    into geohub
    Follows exactly https://github.com/UNDP-Data/geohub/discussions/545

    """
    logger.info(f'Starting to ingest {blob_path}')
    #1 copy data file to working folder
    if not '/raw/' in blob_path:
        return HTTPException(status_code=400, detail=f'blob_path is not located in raw folder')
    try:
        working_blob_path = await copy_raw2working(raw_blob_path=blob_path)
    except Exception as ce:
        raise HTTPException(status_code=500, detail=f'Could not copy {blob_path} to working directory')

    #2 create the datasets folder tah will hold all physical files. It does not make
    # sense to physically create the folder because folders are not real in azure

    dataset_folder = blob_path.replace('/raw/', '/datasets') # the folder name  will contain the extension
    gdal_open(working_blob_path)
    #3
    return f'Finished ingesting {blob_path}'


@app_router.get("/ingest")
async def main(filename):
    logging.info(f"Reading {filename}")
    raster_layers, vector_layers, local_path = prepare_file(filename)

    if not raster_layers or vector_layers:
        logging.error("File is not readable in a GIS")

    if raster_layers > 0 and vector_layers > 0:
        logging.info("File contains both raster and vector data, beginning ingesting")
        return "both"

    elif raster_layers > 0:
        logging.info("File contains raster data, beginning ingesting")
        raster_ingest(local_path)

    elif vector_layers > 0:
        logging.info("File contains vector data, beginning ingesting")
        vector_ingest(local_path)

    else:
        logging.error("No readable data")

    return "Completed ingestion"
