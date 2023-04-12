from ingest.processing import process_geo_file
import logging
import os

if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))



    logger.handlers.clear()
    logger.addHandler(sthandler)
    logger.name = __name__
    fpath = '/data/File_GeoHub_Geodatabase.gdb'
    #fpath = '/data/featuredataset.gdb.zip'
    #fpath = '/data/Sample.gpkg'
    #fpath = '/data/Percent_electricity_access_2012.tif'
    #fpath = '/data/devel.tif'
    process_geo_file(vsiaz_blob_path=fpath, join_vector_tiles=True, timeout_secs=3600)
