import os

from ingest.ingest import sync_ingest
from ingest.processing import process_geo_file
import logging


if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))



    logger.handlers.clear()
    logger.addHandler(sthandler)
    logger.name = __name__
    # fpath = '/data/File_GeoHub_Geodatabase.gdb'
    #fpath = '/vsizip/data/featuredataset.gdb.zip'
    #fpath = '/data/Sample.gpkg'
    #fpath = '/data/Percent_electricity_access_2012.tif'
    #fpath = '/data/devel.tif'
    fpath = '/home/thuha/Pictures/Screenshots/kenya.png'
    process_geo_file(
        blob_url="https://undpgeohub.blob.core.windows.net/userdata/4c9b6906ff63494418aef1efd028be44/raw/kenya.png",
        src_file_path=fpath,
        conn_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
        join_vector_tiles=True
    )
    # sync_ingest(
    #     blob_url="https://undpgeohub.blob.core.windows.net/userdata/test/USA_POPDEN_2020_COG_1km_reclass.tif",
    #     conn_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    # )
