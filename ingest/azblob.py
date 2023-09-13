import asyncio
import logging
import math
import os.path
import threading
import time

from azure.storage.blob.aio import BlobLeaseClient as ABlobLeaseClient, \
    BlobServiceClient as ABlobServiceClient, \
    ContainerClient as AContainerClient
from azure.storage.blob import ContainerClient, BlobServiceClient
import json
import datetime
import multiprocessing
from ingest.utils import (
    chop_blob_url,
    get_dst_blob_path

)

from aiofile import AIOFile

from ingest.ingest_exceptions import ClientRequestError, ResourceNotFoundError

logger = logging.getLogger(__name__)


async def upload_timeout_blob(blob_url: str, connection_string=None, ):
    """

    @param blob_path:
    @param container_name:
    @param connection_string:
    @return:
    """
    raw_blob_path = chop_blob_url(blob_url)
    datasets_blob_path = get_dst_blob_path(blob_path=raw_blob_path)
    container_name, *rest, blob_name = datasets_blob_path.split("/")
    timeout_blob_path = os.path.join(*rest, f'{blob_name}.timeout')

    try:
        # create the blob
        async with ABlobServiceClient.from_connection_string(connection_string) as blob_service_client:
            async with blob_service_client.get_blob_client(
                    container=container_name, blob=timeout_blob_path
            ) as blob_client:
                await blob_client.upload_blob(b"timeout", overwrite=True)
    except (ClientRequestError, ResourceNotFoundError) as e:
        logger.error(f"Failed to upload {timeout_blob_path}: {e}")

async def copy_raw2datasets(raw_blob_path: str, connection_string=None):
    """
    Copy raw_blob to the datasets directory
    """
    container_name, *rest = raw_blob_path.split("/")
    blob_path = "/".join(rest)
    try:
        blob_service_client = ABlobServiceClient.from_connection_string(
            connection_string
        )

        async with blob_service_client:
            container_client = blob_service_client.get_container_client(container_name)

            src_blob = container_client.get_blob_client(blob_path)
            src_props = await src_blob.get_blob_properties()

            async with ABlobLeaseClient(client=src_blob) as lease:
                await lease.acquire(30)

                dst_blob_path = get_dst_blob_path(blob_path)
                logger.info(f"Copying {raw_blob_path} to {dst_blob_path}")
                upload_ingesting_blob(dst_blob_path, container_name=container_name, connection_string=connection_string)
                dst_blob = container_client.get_blob_client(dst_blob_path)

                logger.info(f"Copied {raw_blob_path} to {dst_blob_path}")
                copy = await asyncio.wait_for(
                    dst_blob.start_copy_from_url(src_blob.url),
                    timeout=30,
                )

                dst_props = await dst_blob.get_blob_properties()

                if dst_props.copy.status != "success":
                    await dst_blob.abort_copy(dst_props.copy.id)
                    logger.error(f"Failed to copy {raw_blob_path} to {dst_blob_path}")
                    await upload_error_blob(
                        blob_path,
                        f"Failed to copy {raw_blob_path} to {dst_blob_path}",
                        container_name=container_name,
                        connection_string=connection_string
                    )

    except (ResourceNotFoundError, ClientRequestError) as e:
        logger.error(f"Failed to copy {raw_blob_path}: {e}")
        await upload_error_blob(
            blob_path, 
            f"Failed to copy {raw_blob_path}: {e}",
            container_name=container_name,
            connection_string=connection_string)
    except asyncio.TimeoutError:
        logger.error(f"Copy operation timed out for {raw_blob_path}")
        await upload_error_blob(
            blob_path, 
            f"Copy operation timed out for {raw_blob_path}",
            container_name=container_name,
            connection_string=connection_string
        )


def upload_ingesting_blob(blob_path: str, container_name=None, connection_string=None):
    """
    Upload the ingesting file to the blob
    @param blob_path:
    @param container_name:
    @param connection_string:
    @return:
    """
    if f"/{container_name}/" in blob_path:
        blob_path = blob_path.split(f"/{container_name}/")[-1]
    ingesting_blob_path = f"{blob_path}.ingesting"
    try:
        with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
                with blob_service_client.get_blob_client(container=container_name, blob=ingesting_blob_path) as blob_client:
                    blob_client.upload_blob("", overwrite=True)
    except (ClientRequestError, ResourceNotFoundError) as e:
        logger.error(f"Failed to upload {ingesting_blob_path}: {e}")


async def upload_error_blob(blob_path: str = None, error_message: str = None, container_name: str = None,
                            connection_string=None):
    # handle the case when paths are coming from ingest_raster
    if f"/{container_name}/" in blob_path:
        blob_path = blob_path.split(f"/{container_name}/")[-1]
    error_blob_path = f"{blob_path}.error"
    try:
        # Upload the error message as a blob
        async with ABlobServiceClient.from_connection_string(connection_string) as blob_service_client:
            async with blob_service_client.get_blob_client(
                    container=container_name, blob=error_blob_path
            ) as blob_client:
                await blob_client.upload_blob(error_message.encode("utf-8"), overwrite=True)
    except (ClientRequestError, ResourceNotFoundError) as e:
        logger.error(f"Failed to upload {error_blob_path}: {e}")


async def handle_lock(receiver=None, message=None, timeout_event: multiprocessing.Event = None):
    """
    Renew  the AutolockRenewer lock registered on a servicebus message.
    Long running jobs and with unpredictable execution duration  pose few chalenges.
    First, the network can be disconnected or face other issues and second the renewal operation
    can also fail or take a bit longer. For this reason  to keep an AzureService bus locked
    it is necessary to renew the lock in an infinite loop
    @param receiver, instance of Azure ServiceBusReceiver
    @param message,instance or Azure ServiceBusReceivedMessage

    @return: None
    """

    msg_str = json.loads(str(message))
    blob_path, token = msg_str.split(";")
    logger.debug(f'Starting lock monitoring for {blob_path}')
    while True:
        lu = message.locked_until_utc
        n = datetime.datetime.utcnow()
        d = int((lu.replace(tzinfo=n.tzinfo) - n).total_seconds())
        # logger.debug(f'locked until {lu} utc now is {n} lock expired {message._lock_expired} and will expire in  {d}')
        if d < 10:
            logger.debug('renewing lock')
            try:

                await receiver.renew_message_lock(message=message, )
            except Exception as e:
                timeout_event.is_set()
                # it is questionable whether the exception should be propagated
                raise
        await asyncio.sleep(1)


def upload_content_to_blob(content=None, connection_string: str = None, container_name: str = None,
                           dst_blob_path: str = None, overwrite: bool = True, max_concurrency: int = 8) -> None:
    """
    Uploads the src_path file to Azure dst_blob_path located in container_name
    @param content: str, source file
    @param connection_string: strm the Azure storage account  connection string
    @param container_name: str, container name
    @param dst_blob_path: relative path to the container  where the src_path will be uploaded
    @param overwrite: bool
    @param max_concurrency: 8
    @return:  None
    """
    for attempt in range(1, 4):
        try:
            with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
                with blob_service_client.get_blob_client(container=container_name, blob=dst_blob_path) as blob_client:
                    blob_client.upload_blob(content, overwrite=overwrite, max_concurrency=max_concurrency)
            logger.info(f"Successfully wrote content to {dst_blob_path}")
            break
        except Exception as e:
            if attempt == 3:
                logger.info(f'Failed to upload content to {dst_blob_path} in attempt no {attempt}.')
                raise
            logger.info(f'Failed to upload content to {dst_blob_path} in attempt no {attempt}. Trying again... ')
            continue


def upload_blob(src_path: str = None, connection_string: str = None, container_name: str = None,
                dst_blob_path: str = None, overwrite: bool = True, max_concurrency: int = 8) -> None:
    """
    Uploads the src_path file to Azure dst_blob_path located in container_name
    @param src_path: str, source file
    @param connection_string: strm the Azure storage account  connection string
    @param container_name: str, container name
    @param dst_blob_path: relative path to the container  where the src_path will be uploaded
    @param overwrite: bool
    @param max_concurrency: 8
    @return:  None
    """
    logtrack = []

    def _progress_(current, total) -> None:
        progress = current / total * 100
        rounded_progress = int(math.floor(progress))
        if rounded_progress not in logtrack and rounded_progress % 2 == 0:
            logger.info(f'uploaded - {rounded_progress}%')
            logtrack.append(rounded_progress)

    for attempt in range(1, 4):
        try:
            with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
                with blob_service_client.get_blob_client(container=container_name, blob=dst_blob_path) as blob_client:
                    with open(src_path, "rb") as upload_file:
                        blob_client.upload_blob(upload_file, overwrite=overwrite, max_concurrency=max_concurrency,
                                                progress_hook=_progress_)
                    logger.info(f"Successfully wrote {src_path} to {dst_blob_path}")
                # remove any error
                error_blob_path = f'{dst_blob_path}.error'
                with blob_service_client.get_blob_client(container=container_name,
                                                         blob=error_blob_path) as error_blob_client:
                    if error_blob_client.exists():
                        error_blob_client.delete_blob(delete_snapshots=True)

            break
        except Exception as e:
            if attempt == 3:
                logger.info(f'Failed to upload {src_path} in attempt no {attempt}.')
                raise
            logger.info(f'Failed to upload {src_path} in attempt no {attempt}. Trying again... ')
            continue


async def write(file_handle=None, stream=None, offset=None):
    data = await stream.read()
    return await file_handle.write(data, offset=offset)


async def write_chunked(file_handle=None, stream=None, offset=None, length=None, event=None, chunk_no=None, ):
    nwritten = 0
    async for chunk in stream.chunks():
        chunk_len = len(chunk)
        chunk_offset = offset + chunk_len
        ncwritten = await file_handle.write(chunk, offset=chunk_offset)
        nwritten += ncwritten
        logger.info(f'downloaded {(nwritten / length) * 100:.2f}% in chunk {chunk_no}')

        if event and event.is_set():
            raise TimeoutError(f'Partial download has timed out in chunk {chunk_no}')
    return nwritten


async def download_blob(temp_dir=None, conn_string=None, blob_path=None, event=None, nchunks=5):
    """
    Asynchronously download blob_path into temp_dir. Supports cancellation through event
    argument. Can work in streaming mode or in concurrent chunked mode.

    NB:The chunked mode NEEDS to be tested before using

    @param temp_dir:
    @param conn_string:
    @param blob_path:
    @param event:
    @param nchunks:
    @return:
    """

    async def progress(current, total) -> None:
        n = current / total * 100
        # if n % 10 == 2 and n/10 == 0:
        logger.info(f'download status for {blob_path} - {n:.2f}%')

    container_name, *rest, file_name = blob_path.split("/")
    logger.info(f'{container_name} - {file_name}')
    container_rel_blob_path = os.path.join(*rest, file_name)
    logger.info(f'{container_name} - {container_rel_blob_path}')
    start = time.time()

    dst_file = os.path.join(temp_dir, file_name)
    async with AContainerClient.from_connection_string(conn_string, container_name,
                                                       max_single_get_size=1 * 128 * 1024 * 1024,
                                                       max_chunk_get_size=16 * 1024 * 1024) as cc:
        src_blob = cc.get_blob_client(container_rel_blob_path)
        src_props = await src_blob.get_blob_properties()
        size = src_props.size
        content_type = src_props.content_settings
        logger.info(f'Blob size is {size} {content_type} ')

        if nchunks:
            async with AIOFile(dst_file, 'wb') as dstf:
                chunk_size = size // nchunks
                offsets = [i for i in range(0, size - chunk_size, chunk_size)]
                lengths = [chunk_size] * (nchunks - 1) + [size - chunk_size * (nchunks - 1)]
                tasks = list()
                for chunk_no, item in enumerate(zip(offsets, lengths), start=1):
                    offset, length = item
                    chunk_stream = await cc.download_blob(container_rel_blob_path,
                                                          max_concurrency=1,
                                                          progress_hook=progress,
                                                          offset=offset,
                                                          length=length
                                                          )
                    tasks.append(
                        asyncio.create_task(
                            write_chunked(file_handle=dstf,
                                          stream=chunk_stream,
                                          offset=offset,
                                          length=length,
                                          chunk_no=chunk_no,
                                          event=event
                                          )
                        )
                    )

                results = await asyncio.gather(*tasks)
                assert results == lengths
        else:
            with open(dst_file, 'wb') as dstf:
                await close_container_client(cc=cc, event=event)
                stream = await cc.download_blob(container_rel_blob_path,
                                                max_concurrency=8,
                                                progress_hook=progress,

                                                )
                await stream.readinto(dstf)

    end = time.time()

    logger.info(f'download lasted {(end - start) / 60} minutes ')
    return dst_file


async def close_container_client(cc=None, event=None):
    """
    Monitor the event in an infinite loop and
    closes the client whenever the event is set
    @param cc: instance of  sync ContainerClient
    @param event: multiprocessing.Event
    @return: None
    """
    while True:
        if event and event.is_set():
            if isinstance(cc, AContainerClient):
                await cc.close()
            else:
                cc.close()
            logger.info(f'Cancelling download ')
            raise asyncio.CancelledError()
        await asyncio.sleep(1)
        logger.info('dworking')


def close_cc(cc=None, timeout_event: multiprocessing.Event = None, stop_download: multiprocessing.Event = None):
    """
    Monitor the event in an infinite loop and
    closes the client whenever the event is set
    @param cancel_download:
    @param timeout_event:
    @param ingest_event:
    @param cc: instance of  sync ContainerClient
    @return: None
    """
    while True:
        if stop_download and stop_download.is_set():
            logger.debug('Stop download monitor')
            return
        if (timeout_event and timeout_event.is_set()):
            logger.debug('Timeout has been signalled. Cancelling download')
            cc.close()
            return

        time.sleep(1)


def download_blob_sync(src_blob_path=None, local_folder=None, conn_string=None,
                       timeout_event: multiprocessing.Event = None) -> str:
    """
    Download the src_blob_path into the local_folder
    @param timeout_download: object used  to signal timeout
    @param src_blob_path: str, the full relative path (including the container) to the blob file
    @param local_folder: str, abs path to a local folder where the src_blob_path will be downloaded
    @param conn_string: str, the connections string to the azure storage account
    @param event: object used signal cancellation
    @return: str, the abs path to the downloaded  file

    The  downloading is parallelized and streamed. Because of this it is not possible to
    cancel it. Thus, a  monitor function is also started in a thread that takes three arguments, the ContainerClient instance
    and the event object and raises an asyncio.CancelledError in case the main script has timed out

    """

    logtrack = []

    def _progress_(current, total) -> None:
        progress = current / total * 100
        rounded_progress = int(math.floor(progress))
        if rounded_progress not in logtrack and rounded_progress % 10 == 0:
            logger.info(f'downloaded - {rounded_progress}%')
            logtrack.append(rounded_progress)

    container_name, *rest, file_name = src_blob_path.split("/")
    container_rel_blob_path = os.path.join(*rest, file_name)
    dst_file = os.path.join(local_folder, file_name)
    try:
        with open(dst_file, 'wb') as dstf:
            stop_download = multiprocessing.Event()
            with ContainerClient.from_connection_string(conn_string, container_name) as cc:
                cl = cc.get_blob_client(container_rel_blob_path)
                blob_exists = cl.exists()
                if not blob_exists:
                    # upload error blob
                    raise FileNotFoundError(f'{container_rel_blob_path} does not exist in container "{container_name}"')
                # prepare and start the monitor function in a separate thread
                monitor = threading.Thread(target=close_cc, name='monitor',
                                           kwargs=dict(cc=cc, timeout_event=timeout_event, stop_download=stop_download))
                monitor.start()
                logger.info(f'Downloading {container_rel_blob_path}')
                stream = cc.download_blob(container_rel_blob_path, max_concurrency=8, progress_hook=_progress_)
                stream.readinto(dstf)
                # stop monitor thread
                stop_download.set()
    except AttributeError as ae:
        '''
            This happens on timeout. As the connection is closed the session object from aiohttp becomes None
            and throws this error.
        '''

        if "'request'" in str(ae):
            raise TimeoutError(f'Downloading {src_blob_path} has timed out')

    except Exception as e:  # swallow the error
        raise
    return dst_file
