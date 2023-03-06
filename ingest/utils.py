import logging

from fastapi import HTTPException
from osgeo import gdal

from ingest.config import connection_string

logger = logging.getLogger(__name__)


def gdal_open(filename):

    logger.info(f"Opening {filename} with GDAL")
    gdal.SetConfigOption("AZURE_STORAGE_CONNECTION_STRING", connection_string)
    dataset = gdal.OpenEx(filename, gdal.GA_ReadOnly)

    if dataset is None:
        raise HTTPException(
            status_code=400, detail=f"{filename} does not contain GIS data"
        )

    nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
    dataset = None
    return nrasters, nvectors
