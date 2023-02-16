import logging
import tempfile

from osgeo import gdal

from .azure_clients import azure_container_client


def prepare_file(blob_path):

    cleaned_path = prepare_filename(blob_path, directory="raw")

    logging.info(f"Creating Azure client and downloading new blob. {blob_path}")
    blob_client = azure_container_client()
    download_stream = blob_client.get_blob_client(cleaned_path).download_blob()

    tempFilePath = tempfile.gettempdir()
    tmp_filename = tempfile.NamedTemporaryFile()

    with open(tmp_filename.name, mode="wb") as download_file:
        download_file.write(download_stream.readall())

    copy_to_working(tmp_filename.name, cleaned_path)
    raster_layers, vector_layers = gdal_open(tmp_filename.name)

    return raster_layers, vector_layers, tmp_filename.name


def gdal_open(filename):

    logging.info(f"Opening {filename} with GDAL")
    dataset = gdal.Open(filename, gdal.GA_ReadOnly)

    return dataset.RasterCount, dataset.GetLayerCount()


def upload_file(streamed_file, dst_path):

    logging.info(f"Uploading {dst_path} to Azure")
    blob_client = azure_container_client().get_blob_client(dst_path)
    blob_client.upload_blob(streamed_file, overwrite=True)

    return True


def prepare_filename(file_path, directory):
    """Prepare filename for upload to azure."""

    logging.info(f"Preparing filename for {file_path}")
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

    logging.info(f"Copying {file_name} to working directory in Azure")
    dst_path = prepare_filename(file_name, directory="working")
    upload_file(streamed_file, dst_path)

    return True
