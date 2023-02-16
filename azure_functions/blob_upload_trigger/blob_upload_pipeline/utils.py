import logging
import tempfile

from osgeo import gdal

from .azure_clients import azure_container_client


def prepare_file(blob_path):

    cleaned_path = prepare_filename(blob_path, directory="raw")
    blob_client = azure_container_client()
    download_stream = blob_client.get_blob_client(cleaned_path).download_blob()

    tempFilePath = tempfile.gettempdir()
    tmp_filename = tempfile.NamedTemporaryFile()
    tmp_filename.write(download_stream.readall())

    working_path = copy_to_working(tmp_filename.name, cleaned_path)
    raster_layers, vector_layers = gdal_open(tmp_filename.name)

    return raster_layers, vector_layers, working_path


def gdal_open(filename):
    dataset = gdal.Open(filename, gdal.GA_ReadOnly)

    return dataset.RasterCount, dataset.GetLayerCount()


def upload_file(dst_path, streamed_file):
    blob_client = azure_container_client().get_blob_client(dst_path)
    blob_client.upload_blob(streamed_file, overwrite=True)

    return True


def prepare_filename(file_path, directory):
    """Prepare filename for upload to azure."""
    filename = file_path.rsplit("/", 1)[-1]
    if directory == "raw":
        dst_path = file_path.split("/", 1)[1]
        return dst_path
    elif directory == "working":
        dst_path = file_path.split("/raw/")[0] + r"/working/" + filename
        return dst_path
    else:
        dst_path = file_path.split("/working/")[0] + r"/datasets/" + filename
        return dst_path


def copy_to_working(streamed_file, file_name):
    """Copy uploaded file to working directory."""
    dst_path = prepare_filename(file_name, directory="working")
    upload_file(dst_path, streamed_file)
    return dst_path
