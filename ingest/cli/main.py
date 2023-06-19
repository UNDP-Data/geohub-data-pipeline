import multiprocessing

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
    logger.setLevel(logging.DEBUG)
    fpath = '/data/File_GeoHub_Geodatabase.gdb.zip'
    #fpath = '/vsizip/data/featuredataset.gdb.zip'
    fpath = '/data/Sample.gpkg'
    #fpath = '/data/Percent_electricity_access_2012.tif'
    fpath = '/data/rgbcog.tif'

    try:
        cancel_event = multiprocessing.Event()
        process_geo_file(src_file_path=fpath, dst_directory='/data/out',  join_vector_tiles=True, timeout_event=cancel_event)
    except KeyboardInterrupt:
        logger.info('Cancelling')
        cancel_event.set()
