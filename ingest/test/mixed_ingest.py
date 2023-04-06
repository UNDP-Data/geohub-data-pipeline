from ingest.processing import process_geo_file
import logging
import os

if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fpath = '/data/File_GeoHub_Geodatabase.gdb'
    #fpath = '/data/featuredataset.gdb.zip'
    #fpath = '/data/Sample.gpkg'
    #fpath = '/data/Percent_electricity_access_2012.tif'
    #fpath = '/data/devel.tif'
    process_geo_file(vsiaz_blob_path=fpath)
