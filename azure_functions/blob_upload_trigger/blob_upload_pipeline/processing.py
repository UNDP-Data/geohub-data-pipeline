from .config import logging

logger = logging.getLogger(__name__)


from .raster_to_cog import raster_process
from .utils import prepare_file
from .vector_to_tiles import vector_process


def process(filename):
    logging.info(f"Processing {filename}")
    raster_layers, vector_layers, local_path = prepare_file(filename)

    if not raster_layers or vector_layers:
        logging.error("File is not readable in a GIS")

    if raster_layers > 0 and vector_layers > 0:
        return "both"

    elif raster_layers > 0:
        raster_process(local_path)

    elif vector_layers > 0:
        vector_process(local_path)

    else:
        logging.error("No readable data")
