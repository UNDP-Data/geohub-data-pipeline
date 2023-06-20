import os
from urllib.parse import urlparse
from ingest.config import (
    datasets_folder,
    raw_folder,
    OUT_FORMATS,
    GDAL_ARCHIVE_FORMATS
)
def is_raster(path: str = None):
    """
    Check if a path is  raster/TIFF
    @param path:
    @return:
    """
    p, file_name = os.path.split(path)
    fname, ext = os.path.splitext(file_name)
    rast_formats = [e for e in OUT_FORMATS if 'tif' in e]
    return os.path.isfile(path) and ext and ext in rast_formats

def is_vector(path: str = None):
    """
    Check if a path is vector/PMTiles
    @param path:
    @return:
    """
    p, file_name = os.path.split(path)
    fname, ext = os.path.splitext(file_name)
    vect_formats = [e for e in OUT_FORMATS if 'tif' not in e]
    return os.path.isfile(path) and ext and ext in vect_formats

def exists_and_isabs(path):
    f, _ = os.path.split(path)[0]
    return os.path.exists(f) and os.path.isabs(f)


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

