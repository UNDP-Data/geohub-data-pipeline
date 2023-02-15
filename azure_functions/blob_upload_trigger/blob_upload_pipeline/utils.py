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


def upload_file(filename, blob_path, azure_container_client):
    blob_client = azure_container_client.get_blob_client(blob_path)
    with open(filename, mode="rb") as sample_blob:
        blob_client.upload_blob(sample_blob, overwrite=True)

    return True


def prepare_filename(file_path, to_working=False):
    """Prepare filename for upload to azure."""
    filename = file_path.rsplit("/", 1)[-1]
    if to_working:
        dst_path = file_path.split("/raw/")[0] + r"/working/" + filename
        return dst_path
    else:
        dst_path = file_path.split("/working/")[0] + r"/datasets/" + filename
        return dst_path


def copy_to_working(filename):
    """Copy uploaded file to working directory."""
    dst_path = prepare_filename(filename, to_working=True)
    upload_file(filename, dst_path, azure_container_client)
    return dst_path
