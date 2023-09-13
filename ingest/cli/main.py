#!/usr/bin/python
import multiprocessing
from ingest.processing import process_geo_file
import argparse
import logging
import sys

if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))
    logger.handlers.clear()
    logger.addHandler(sthandler)
    logger.name = __name__
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description='Convert layers/bands from GDAL supported geospatial data files to COGs/PMtiles.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-src', '--source-file',
                        help='A full absolute path to a geospatial data file.',
                        type=str,  )
    parser.add_argument('-dst', '--destination-directory',
                        help='A full absolute path to a folder where the files will be written.',
                        type=str,  )
    parser.add_argument('-j', '--join-vector-tiles', action='store_true',
                        help='Boolean flag to specify whether to create a multilayer PMtiles file'
                             'in case the source file contains more then one vector layer',
                        default=False
                        ),
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Set log level to debug', default=False
                        )

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.debug:
        logger.setLevel(logging.DEBUG)
    cancel_event = multiprocessing.Event()
    process_geo_file(src_file_path=args.source_file, dst_directory=args.destination_directory,
                     join_vector_tiles=args.join_vector_tiles)#, timeout_event=cancel_event)
