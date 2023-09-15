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
    # silence azure logger
    azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azlogger.setLevel(logging.WARNING)
    sblogger = logging.getLogger("uamqp")
    sblogger.setLevel(logging.WARNING)


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
                     conn_string=os.environ.get('AZURE_STORAGE_CONNECTION_STRING'),
                     timeout_event=te,join_vector_tiles=False)

    # fpath = '/home/thuha/Desktop/data/geohub_data_pipeline/File_GeoHub_Geodatabase.gdb'
    # process_geo_file(
    #     blob_url="https://undpgeohub.blob.core.windows.net/userdata/test/File_GeoHub_Geodatabase.gdb",
    #     src_file_path=fpath,
    #     conn_string=os.environ.get("CONNECTION_STRING"),
    #     join_vector_tiles=True
    # )

