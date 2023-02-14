import os
from tempfile import TemporaryDirectory

from osgeo import gdal

from .azure_clients import azure_container_client


def download_file(blob_path):
    blob_client = azure_container_client.get_blob_client(blob_path)
    with TemporaryDirectory() as tmp_dir:
        tmp_filename = os.path.join(tmp_dir, blob_path)
        with open(tmp_filename, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

        raster_layers, vector_layers = open_file(tmp_filename)

    return raster_layers, vector_layers


def open_file(filename):
    dataset = gdal.Open(filename, gdal.GA_ReadOnly)

    return dataset.RasterCount, dataset.GetLayerCount()
