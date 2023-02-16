from .config import logging

logger = logging.getLogger(__name__)


from .raster_to_cog import raster_process
from .utils import prepare_file
from .vector_to_tiles import vector_process


def process(filename):
    logging.info(f"Processing {filename}")
    raster_layers, vector_layers, working_path = prepare_file(filename)

    if not raster_layers or vector_layers:
        logging.error("File is not readable in a GIS")

    if raster_layers > 0 and vector_layers > 0:
        return "both"

    elif raster_layers > 0:
        raster_process(working_path)

    elif vector_layers > 0:
        vector_process(working_path)

    else:
        logging.error("No readable data")


# def test_process(tmp_path):
#     from pathlib import Path

#     input_path = tmp_path / "input" / "test.shp"
#     blob_path = Path("https://geohub.blob.core.windows.net/test/test.shp")
#     download_data(blob_path, input_path)
#     assert input_path.is_file()
#     assert input_path.stat().st_size > 0

#     process(input_path)

#     assert (tmp_path / "output" / "test.mbtiles").is_file()
