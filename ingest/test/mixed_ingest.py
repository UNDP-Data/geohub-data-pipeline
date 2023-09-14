import os
from multiprocessing import Event
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
    logger.setLevel('INFO')
    fpath = '/data/File_GeoHub_Geodatabase.gdb'
    #fpath = '/vsizip/data/featuredataset.gdb.zip'
    #fpath = '/data/Sample.gpkg'
    #fpath = '/data/Percent_electricity_access_2012.tif'
    #fpath = '/data/devel.tif'
    blob_url='https://undpgeohub.blob.core.windows.net/userdata/test/CP_CDIS_C_PSY.fgb'

    te = Event()
    process_geo_file(blob_url=blob_url,
                     src_file_path='/data/CP_CDIS_C_PSY.fgb',
                     conn_string=os.environ.get('CONNECTION_STRING'),
                     timeout_event=te,join_vector_tiles=True)

