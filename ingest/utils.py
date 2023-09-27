import os
from urllib.parse import urlparse
from ingest.config import (
    datasets_folder,
    raw_folder,
    GDAL_ARCHIVE_FORMATS
)
from osgeo import gdal
def chop_blob_url(blob_url: str) -> str:
    """
    Safely extract relative path of the blob from its url using urllib
    """
    return urlparse(blob_url).path[1:]  # 1 is to exclude the start slash/path separator
    # because the os.path.join disregards any args that start with path sep


def prepare_arch_path(src_path: str = None) -> str:
    assert os.path.isabs(src_path), f'{src_path} has tot be an absolute path'

    _, ext = os.path.splitext(src_path)

    if ext in GDAL_ARCHIVE_FORMATS:
        arch_driver = GDAL_ARCHIVE_FORMATS[ext]
        return os.path.join(os.path.sep, arch_driver, src_path[1:])
    else:
        return src_path


def prepare_vsiaz_path(blob_path: str) -> str:
    """
    Compose the relative path of a blob so is can be opened by GDAL
    """
    _, ext = os.path.splitext(blob_path)

    if ext in GDAL_ARCHIVE_FORMATS:
        arch_driver = GDAL_ARCHIVE_FORMATS[ext]
        prefix = f'/{arch_driver}/vsiaz'
    else:
        prefix = '/vsiaz'

    return os.path.join(prefix, blob_path)


def get_dst_blob_path(blob_path: str, file_name=None) -> str:
    dst_blob = blob_path.replace(f"/{raw_folder}/", f"/{datasets_folder}/")
    file_name = file_name or blob_path.split("/")[-1]
    return f"{dst_blob}/{file_name}"


def get_azure_blob_path(blob_url=None, local_path=None):
    _, file_name = os.path.split(local_path)
    raw_blob_path = chop_blob_url(blob_url)
    datasets_blob_path = get_dst_blob_path(blob_path=raw_blob_path, file_name=file_name)
    container_name, *rest, blob_name = datasets_blob_path.split("/")

    return container_name, os.path.join(*rest, blob_name)


def get_local_cog_path(src_path: str = None, dst_folder: str = None, band=None):
    folders, fname = os.path.split(src_path)
    fname_without_ext, ext = os.path.splitext(fname)
    if src_path.count(':') == 2:
        _, rpath, fname_without_ext = src_path.split(':')
        folders, _ = os.path.split(rpath)
        if '"' in fname_without_ext: fname_without_ext = fname_without_ext.replace('"', '')
        if "'" in fname_without_ext: fname_without_ext = fname_without_ext.replace("'", '')

    if not band:
        return f'{os.path.join(dst_folder, f"{fname_without_ext}.tif")}'
    else:
        return f'{os.path.join(dst_folder, f"{fname_without_ext}_band{band}.tif")}'

def compute_progress(offset=30, nchunks=1, ):
    rest = 100-offset
    chunk_progress = rest//nchunks
    rem = rest%nchunks
    progress = [offset+chunk_progress+i*chunk_progress if i < nchunks-1 else rem+offset+chunk_progress+i*chunk_progress for i in range(nchunks)]
    return progress


def get_progress(offset_perc=30, src_path:str = None):
    """
    Given a GDAL data fiel compute layer/rabster band/ subdataset progress list
    @param offset_perc:
    @param src_path:
    @return:
    """
    ds = gdal.OpenEx(src_path, gdal.OF_VECTOR)
    nvector_layers = ds.GetLayerCount()
    del ds
    ds = gdal.OpenEx(src_path, gdal.OF_RASTER)
    nraster_bands = ds.RasterCount
    n_subdatasets = len(ds.GetSubDatasets())
    del ds
    nchunks = nvector_layers+nraster_bands+n_subdatasets
    return compute_progress(offset=offset_perc, nchunks=nchunks)



