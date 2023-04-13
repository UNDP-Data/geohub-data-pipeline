import asyncio
import json
import logging
import multiprocessing
import os
from io import StringIO
from traceback import print_exc
from ingest.processing import process_geo_file
from azure.servicebus.aio import AutoLockRenewer, ServiceBusClient
from ingest.config import raw_folder, setup_env_vars
from multiprocessing import Event
# from ingest.raster_to_cog import ingest_raster, ingest_raster_sync
from ingest.utils import (
    copy_raw2datasets,
    # gdal_open_async,
    handle_lock,
    prepare_blob_path,
    prepare_vsiaz_path,

)
# from ingest.vector_to_tiles import ingest_vector, ingest_vector_sync

logger = logging.getLogger(__name__)

# silence azure logger
azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azlogger.setLevel(logging.WARNING)
sblogger = logging.getLogger("uamqp")
sblogger.setLevel(logging.WARNING)

setup_env_vars()

INGEST_TIMEOUT = 36  # 1 hours MAX
CONNECTION_STR = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]

async def ingest_message():



    async with ServiceBusClient.from_connection_string(
        conn_str=CONNECTION_STR, logging_enable=True
    ) as servicebus_client:
        async with servicebus_client.get_queue_receiver(
            queue_name=QUEUE_NAME,
            prefetch_count=0,
        ) as receiver:  # get one message without caching
            async with receiver:
                while True:
                    received_msgs = await receiver.receive_messages(
                        max_message_count=1, max_wait_time=5
                    )

                    if not received_msgs:
                        logger.debug(f'No (more) messages to process. Queue "{QUEUE_NAME}" is empty')
                        break

                    for msg in received_msgs:
                        try:
                            msg_str = json.loads(str(msg))
                            blob_path, token = msg_str.split(";")
                            #if not 'ne_110m' in blob_path:continue
                            logger.info(
                                f"Received blob: {blob_path} from queue"
                            )

                            async with AutoLockRenewer() as auto_lock_renewer:
                                auto_lock_renewer.register(
                                    receiver=receiver, renewable=msg
                                )
                                if f"/{raw_folder}/" in blob_path:
                                    """
                                    First, it looks like the max_lock_renewal_duration arg is not working as it should be,
                                    or, I have no idea how to use it properly. So far the queue has been honouring the
                                    lock_time as set in the Azure portal through the web interface.
                                    Second, to ensure a smooth ride, the message is registered and then the lock is renewed
                                    in an infinite loop ten seconds before the lock_time due ot networking. Shorter time lead to
                                    inconsistent behaviour. As a result the ingest is done concurrently using asyncio.wait with lock
                                    renewal and it will usually end first because the lock renewal is infinite.
                                    The asyncio.wait returns when the ingest has completed or an exception has been encountered.
                                    It also uses a hard timeout which should ensure the ingest can not get stuck.
                                    asyncio.wait throws no errors and returns two lists, done and pending, the ingest will be in done and the lock renewal will be
                                    still running in the pending. FOr ths reason, the lock task has to be cancelled disregarding
                                    whether the ingest task was successful or failed.
                                    It might be a good idea for the ingest to return a value but this is not necessary.

                                    The ingest future must be awaited and this is where an exception is thrown in case the ingest task
                                     has failed. In this case the lock  task has to be canceled and the error including the traceback is
                                     extracted and the message is dead lettered.


                                    
                                    """
                                    ingest_event = multiprocessing.Event()
                                    ingest_task = asyncio.ensure_future(
                                        asyncio.to_thread(sync_ingest, blob_path=blob_path, event=ingest_event )
                                    )
                                    ingest_task.set_name('ingest')
                                    lock_task = asyncio.ensure_future(
                                        handle_lock(receiver=receiver, message=msg)
                                    )
                                    lock_task.set_name('lock')

                                    done, pending = await asyncio.wait(
                                        [lock_task, ingest_task],
                                        return_when=asyncio.FIRST_COMPLETED,
                                        timeout=INGEST_TIMEOUT,
                                    )
                                    if len(done) == 0:
                                        logger.info(f'Ingest has timed out after {INGEST_TIMEOUT} seconds.')
                                        ingest_event.set()
                                    logger.debug(f'Handling done tasks')



                                    for ingest_future in done:

                                        try:
                                            res = await ingest_future
                                            #await receiver.complete_message(msg)
                                            # logger.info(f'{str(msg)} completed with result {res}')
                                            # for pending_future in pending:
                                            #     pending_future.cancel()
                                        except Exception as e:
                                            # for pending_future in pending:
                                            #     pending_future.cancel()
                                            with StringIO() as m:
                                                print_exc(
                                                    file=m
                                                )  # exc is extracted using system.exc_info
                                                error_message = m.getvalue()
                                                logger.error(error_message)
                                            # logger.info(
                                            #     f"Pushing {msg} to dead letter sub-queue"
                                            # )
                                            # await receiver.dead_letter_message(
                                            #     msg,
                                            #     reason="ingest error",
                                            #     error_description=error_message,
                                            # )

                                else:
                                    logger.info(
                                        f"Skipping {blob_path} because it is not in the {raw_folder} folder"
                                    )
                                    await receiver.complete_message(msg)
                                    logger.info(f"Completed message for: {blob_path}")

                        except Exception as pe:
                            logger.info(f"Pushing {msg} to deadletter subqueue")
                            with StringIO() as m:
                                print_exc(file=m)
                                em = m.getvalue()
                                logger.error(em)
                            # await receiver.dead_letter_message(
                            #     msg, reason="message parse error", error_description=em
                            # )
                            continue
                        logger.info('tada')

def sync_ingest(blob_path: str, token=None, event=None):
    """
    Ingest a geospatial data file potentially containing multiple layers
    into geohub
    Follows exactly https://github.com/UNDP-Data/geohub/discussions/545

    """
    logger.info(f"Starting to ingest {blob_path}")
    # if the file is a pmtiles file, return without ingesting, copy to datasets
    container_blob_path = prepare_blob_path(blob_path)

    if blob_path.endswith(".pmtiles"):
        asyncio.run(copy_raw2datasets(raw_blob_path=container_blob_path))
    else:
        vsiaz_path = prepare_vsiaz_path(container_blob_path)
        try:

            process_geo_file(vsiaz_blob_path=vsiaz_path, join_vector_tiles=False, event=event)
            logger.info(f"Finished ingesting {blob_path}")
        except Exception as e:
            logger.error(e)
            # csv attempt




#
# async def ingest(blob_path: str, token=None):
#     """
#     Ingest a geospatial data file potentially containing multiple layers
#     into geohub
#     Follows exactly https://github.com/UNDP-Data/geohub/discussions/545
#
#     """
#     logger.info(f"Starting to ingest {blob_path}")
#     # if the file is a pmtiles file, return without ingesting, copy to datasets
#     container_blob_path = prepare_blob_path(blob_path)
#
#     if blob_path.endswith(".pmtiles"):
#         await copy_raw2datasets(raw_blob_path=container_blob_path)
#     else:
#         vsiaz_path = prepare_vsiaz_path(container_blob_path)
#         nrasters, nvectors = await gdal_open_async(vsiaz_path)
#
#         # 2 ingest
#         if nrasters > 0:
#             await ingest_raster(vsiaz_blob_path=vsiaz_path)
#         if nvectors > 0:
#             await ingest_vector(vsiaz_blob_path=vsiaz_path)
#         # csv
#
#     logger.info(f"Finished ingesting {blob_path}")


