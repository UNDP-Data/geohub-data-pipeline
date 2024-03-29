import asyncio
from ingest.processing import process_geo_file
import logging
import random
import time
from multiprocessing import Event

async def long_async_func():
    try:

        logger.info(f'Starting long async func')
        while True:
            sleep_secs = random.randrange(0,6,1)
            if sleep_secs%2==0:
                logger.info(f'Going to sleep for {sleep_secs}')
            await asyncio.sleep(sleep_secs)
    except asyncio.CancelledError:
        logger.error(f'long async got cancelled')
        raise

    except Exception as e:
        logger.error(f'hl error {e}')


async def main(fpath=None)->None:

    long_fut = asyncio.ensure_future(long_async_func())
    long_fut.set_name('long')

    timeout_event = Event()
    ingest_fut = asyncio.ensure_future(
        asyncio.to_thread(
            process_geo_file, src_file_path=fpath, join_vector_tiles=False, timeout_event=timeout_event
        )
    )
    ingest_fut.set_name('ingest')


    INGEST_TIMEOUT = 200
    done, pending = await asyncio.wait( [long_fut, ingest_fut], return_when=asyncio.FIRST_COMPLETED,
        timeout=INGEST_TIMEOUT,
    )
    if len(done) == 0:
        logger.info(f'Ingest has timed out after {INGEST_TIMEOUT}. {len(done)} task is done  and  {len(pending)} task(s) are pending')
        timeout_event.set()
    logger.debug(f'Handling done tasks')

    for done_future in done:
        try:
            await done_future
        except Exception as e:
            logger.error(f'done future error {e}')
            raise

    logger.debug(f'Cancelling pending tasks')

    for pending_future in pending:
        logger.debug(f'Cancelling {pending_future.get_name()}')
        try:

            pending_future.cancel()
            await pending_future
        except asyncio.CancelledError:
            logger.debug(f'prending future {pending_future.get_name()} has been cancelled')
        except Exception as e:
            logger.error(f'encountered error {e}')
            raise
    #ingest_event.clear()

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
    fpath = '/data/File_GeoHub_Geodatabase.gdb'
    #fpath = "/vsizip/data/featuredataset.gdb.zip"
    fpath = '/data/Sample.gpkg'
    #fpath = '/data/Percent_electricity_access_2012.tif'
    fpath = '/data/devel.tif'
    #fpath='/data/Nairobi_slums_SDI_2016.shp'
    start = time.time()
    asyncio.run(main(fpath=fpath))

    end = time.time()
    logger.info(f'The  whole main lasted {(end-start)}')
