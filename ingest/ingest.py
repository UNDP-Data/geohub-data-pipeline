import logging

from fastapi import APIRouter

from .config import logging
from .raster_to_cog import raster_ingest
from .utils import prepare_file
from .vector_to_tiles import vector_ingest
from .azure_clients import copy_to_working

logger = logging.getLogger(__name__)

app_router = APIRouter()
#silence azure logger
azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azlogger.setLevel(logging.WARNING)





@app_router.get("/ingest1")
async def ingest(blob_path:str=None):
    logger.info(f'Starting to ingest {blob_path}')
    #1 copy data file to working folder

    copied_blob_path = await copy_to_working(src_blob_path=blob_path)



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
