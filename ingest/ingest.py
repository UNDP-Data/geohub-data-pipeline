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
import tempfile
from ingest.utils import (
    copy_raw2datasets,
    handle_lock,
    chop_blob_url,
    download_blob_sync,
    upload_timeout_blob
)

logger = logging.getLogger(__name__)

# silence azure logger
azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azlogger.setLevel(logging.WARNING)
sblogger = logging.getLogger("uamqp")
sblogger.setLevel(logging.WARNING)

setup_env_vars()

INGEST_TIMEOUT = 3600  # 1 hours MAX
CONNECTION_STR = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]
AZ_STORAGE_CONN_STR = os.environ['AZURE_STORAGE_CONNECTION_STRING']

async def ingest_message():
    async with ServiceBusClient.from_connection_string(
            conn_str=CONNECTION_STR, logging_enable=True
    ) as servicebus_client:
        async with servicebus_client.get_queue_receiver(
                queue_name=QUEUE_NAME,
                #prefetch_count=0,
        ) as receiver:  # get one message without caching

            while True:
                received_msgs = await receiver.receive_messages(
                    max_message_count=1,
                    max_wait_time=5

                )

                if not received_msgs:
                    logger.info(f'No (more) messages to process. Queue "{QUEUE_NAME}" is empty')
                    break

                for msg in received_msgs:
                    try:
                        msg_str = json.loads(str(msg))
                        blob_url, token = msg_str.split(";")
                        #if not 'Sample' in blob_url: continue
                        logger.info(
                            f"Received blob: {blob_url} from queue"
                        )

                        async with AutoLockRenewer() as auto_lock_renewer:
                            auto_lock_renewer.register(
                                receiver=receiver, renewable=msg
                            )
                            if f"/{raw_folder}/" in blob_url:
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

                                timeout_event = multiprocessing.Event()
                                ingest_task = asyncio.ensure_future(
                                    asyncio.to_thread(sync_ingest, blob_url=blob_url, timeout_event=timeout_event,
                                                      conn_string=AZ_STORAGE_CONN_STR)
                                )
                                ingest_task.set_name('ingest')
                                lock_task = asyncio.ensure_future(
                                    handle_lock(receiver=receiver, message=msg, timeout_event=timeout_event)
                                )
                                lock_task.set_name('lock')

                                done, pending = await asyncio.wait(
                                    [lock_task, ingest_task],
                                    return_when=asyncio.FIRST_COMPLETED,
                                    timeout=INGEST_TIMEOUT,
                                )
                                if len(done) == 0:
                                    logger.info(
                                        f'Ingesting {blob_url} has timed out after {INGEST_TIMEOUT} seconds.')
                                    timeout_event.set()
                                    # upload an blob to the /dataset/{datasetname} folder.
                                    await upload_timeout_blob(blob_url=blob_url, connection_string=AZ_STORAGE_CONN_STR)

                                logger.debug(f'Handling done tasks')

                                for done_future in done:
                                    try:
                                        await done_future
                                        # await receiver.complete_message(msg)
                                    except Exception as e:
                                        with StringIO() as m:
                                            print_exc(file=m)
                                            em = m.getvalue()
                                            logger.error(f'done future error {em}')

                                logger.debug(f'Cancelling pending tasks')

                                for pending_future in pending:
                                    try:
                                        pending_future.cancel()
                                        await pending_future
                                    except asyncio.CancelledError:
                                        logger.debug(
                                            f'Pending future {pending_future.get_name()} has been cancelled')
                                    except Exception as e:
                                        raise

                            else:
                                logger.info(
                                    f"Skipping {blob_url} because it is not in the {raw_folder} folder"
                                )
                                await receiver.complete_message(msg)
                                logger.info(f"Completed message for: {blob_url}")

                    except Exception as pe:  # this  first level might be redundant
                        with StringIO() as m:
                            print_exc(file=m)
                            em = m.getvalue()
                            logger.error(em)
                        logger.info(f"Pushing {msg} to dead-letter sub-queue")

                        # await receiver.dead_letter_message(
                        #     msg, reason="message parse error", error_description=em
                        # )
                        continue


def sync_ingest(blob_url: str = None, token: str = None, timeout_event: multiprocessing.Event = None,
                conn_string: str = None):
    """
    Ingest a geospatial data file potentially containing multiple raster/vector layers
    into geohub
    Follows  https://github.com/UNDP-Data/geohub/discussions/545

    There are some peculiarities about this function:
        1) downlod_blob_sync  returns errors on timeout(signalled using timeout_event argument
        2. process geo file uses the timeout_event to cancel GDAL functions as well as the tippecanoe process
        3. thi function is executed in a separate Thread. By the time the error is received the parent async machinery
            will have finished running. This is why it silences the errors as there is no parent receiver.
            This is by design, because in python is is NOT possible to cleansly STOP a thread except by
            interrupting the functions that run inside the thread which is EXACTLY what is happening

    The reason behind running this function in a separate thread are simple. GDAL/geo API's are not async.
    The reason why the parent machinery is async is simple. Managing concurrent functions in async is much easier then
    using threads. Async funcs can be easily cancelled and they time out in a reasonable way. Also, contrary to intuition,
    the fastest way to download  A SINGLE  data file from azure is not async but  sync using max_concurrency.

    It is by design that the ingest is sync managed by an async machinery. The risk of running truly async ingest is running
    out of memory because multiple data files would have been opened at the same time in RAM.
    This is why in essence the ingest is sequential, run by an async machinery and sync at its core. By combining these
    features and approached a resilient and solid pipeline was produced.

    @param blob_url: the input file stored in Azure (blob)
    @param token:
    @param timeout_event: object used to signal a timeout has occurred
    @param conn_string: info to connect to Azure (download/upload)
    @return: None
    """
    logger.info(f"Starting to ingest {blob_url}")
    # if the file is a pmtiles file, return without ingesting, copy to datasets
    blob_path = chop_blob_url(blob_url)

    if blob_url.endswith(".pmtiles"):
        asyncio.run(copy_raw2datasets(raw_blob_path=blob_path))
    else:
        # vsiaz_path = prepare_vsiaz_path(container_blob_path)
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_data_file = download_blob_sync(
                    local_folder=temp_dir,
                    conn_string=conn_string,
                    src_blob_path=blob_path,
                    timeout_event=timeout_event
                )
                if not temp_data_file:
                    raise Exception(f'Undetected exception has occurred while downloading {blob_path}')
                process_geo_file(blob_url=blob_url,src_file_path=temp_data_file, join_vector_tiles=False, timeout_event=timeout_event,
                                 conn_string=conn_string)
                logger.info(f"Finished ingesting {blob_url}")
        except TimeoutError as te:
            logger.debug(te)
        except Exception as ee:
            with StringIO() as m:
                print_exc(file=m)
                em = m.getvalue()
                logger.error(f'{em}')
