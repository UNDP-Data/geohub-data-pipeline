from .config import logging

logger = logging.getLogger(__name__)


from .raster_to_cog import raster_ingest
from .utils import prepare_file
from .vector_to_tiles import vector_ingest


def ingest(filename):
    logging.info(f"Ingesting {filename}")
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
