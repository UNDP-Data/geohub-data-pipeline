import asyncio
import logging
import os.path
import tempfile
import time
from urllib.parse import urlparse
from azure.storage.blob.aio import BlobLeaseClient, BlobServiceClient, ContainerClient as AContainerClient
from azure.storage.blob import ContainerClient
import json
import datetime
import multiprocessing
from functools import wraps
from ingest.config import (
    # account_url,
    # connection_string,
    # container_name,
    datasets_folder,
    raw_folder,
)
from ingest.config import GDAL_ARCHIVE_FORMATS
from ingest.ingest_exceptions import ClientRequestError, ResourceNotFoundError

logger = logging.getLogger(__name__)


def prepare_blob_path(blob_path: str) -> str:
    """
    Safely xtract relative path of the blob from its url using urllib
    """
    return urlparse(blob_path).path[1:] # 1 is to exclude the start slash/path separator
    # because the os.path.join disregards any


def prepare_vsiaz_path(blob_path: str) -> str:
    """
    Compose the relative path of a blob so is can be opened by GDAL
    """
    _, ext = os.path.splitext(blob_path)

    if ext in GDAL_ARCHIVE_FORMATS:
        arch_driver = GDAL_ARCHIVE_FORMATS[ext]
        prefix = f'/{arch_driver}/vsiaz'
    else:
        prefix = '/vsiaz'

    return os.path.join(prefix, blob_path)


def get_dst_blob_path(blob_path: str) -> str:
    dst_blob = blob_path.replace(f"/{raw_folder}/", f"/{datasets_folder}/")
    pmtile_name = blob_path.split("/")[-1]
    return f"{dst_blob}/{pmtile_name}"

#
# async def gdal_open_async(filename):
#     logger.info(f"Opening {filename} with GDAL")
#     #gdal.SetConfigOption("AZURE_STORAGE_CONNECTION_STRING", connection_string)
#     dataset = gdal.OpenEx(filename, gdal.GA_ReadOnly)
#
#     #dataset = await asyncio.to_thread(gdal.OpenEx, filename, gdal.GA_ReadOnly)
#     if dataset is None:
#         logger.error(f"{filename} does not contain GIS data")
#         await upload_error_blob(filename, f"{filename} does not contain GIS data")
#
#
#     nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
#     dataset = None
#
#     return nrasters, nvectors
#
# def gdal_open_sync(filename):
#
#     logger.info(f"Opening {filename}")
#     dataset = gdal.OpenEx(filename, gdal.GA_ReadOnly)
#
#     if dataset is None:
#         logger.error(f"{filename} does not contain GIS data")
#         asyncio.run(upload_error_blob(filename, f"{filename} does not contain GIS data"))
#     nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
#
#     dataset = None
#     return nrasters, nvectors

async def copy_raw2datasets(raw_blob_path: str, connection_string=None):
    """
    Copy raw_blob to the datasets directory
    """
    container_name, *rest = raw_blob_path.split("/")
    blob_path = "/".join(rest)
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        async with blob_service_client:
            container_client = blob_service_client.get_container_client(container_name)

            src_blob = container_client.get_blob_client(blob_path)
            src_props = await src_blob.get_blob_properties()


            async with BlobLeaseClient(client=src_blob) as lease:
                await lease.acquire(30)

                dst_blob_path = get_dst_blob_path(blob_path)
                await upload_ingesting_blob(dst_blob_path)
                dst_blob = container_client.get_blob_client(dst_blob_path)

                logger.info(f"Copying {raw_blob_path} to {dst_blob_path}")
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
                    )

    except (ResourceNotFoundError, ClientRequestError) as e:
        logger.error(f"Failed to copy {raw_blob_path}: {e}")
        await upload_error_blob(blob_path, f"Failed to copy {raw_blob_path}: {e}")
    except asyncio.TimeoutError:
        logger.error(f"Copy operation timed out for {raw_blob_path}")
        await upload_error_blob(
            blob_path, f"Copy operation timed out for {raw_blob_path}"
        )


async def upload_ingesting_blob(blob_path: str, container_name=None, connection_string=None):
    """

    @param blob_path:
    @param container_name:
    @param connection_string:
    @return:
    """
    if f"/{container_name}/" in blob_path:
        blob_path = blob_path.split(f"/{container_name}/")[-1]
    ingesting_blob_path = f"{blob_path}.ingesting"
    try:
        # Upload the ingesting file to the blob
        async with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
            async with blob_service_client.get_blob_client(
                container=container_name, blob=ingesting_blob_path
            ) as blob_client:
                await blob_client.upload_blob(b"ingesting", overwrite=True)
    except (ClientRequestError, ResourceNotFoundError) as e:
        logger.error(f"Failed to upload {ingesting_blob_path}: {e}")


async def upload_error_blob(blob_path:str=None, error_message:str=None, container_name:str=None, connection_string=None):
    # handle the case when paths are coming from ingest_raster
    if f"/{container_name}/" in blob_path:
        blob_path = blob_path.split(f"/{container_name}/")[-1]
    error_blob_path = f"{blob_path}.error"
    try:
        # Upload the error message as a blob
        async with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
            async with blob_service_client.get_blob_client(
                container=container_name, blob=error_blob_path
            ) as blob_client:
                await blob_client.upload_blob(error_message.encode("utf-8"), overwrite=True)
    except (ClientRequestError, ResourceNotFoundError) as e:
        logger.error(f"Failed to upload {error_blob_path}: {e}")

async def handle_lock(receiver=None, message=None):

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
    try:
        msg_str = json.loads(str(message))
        blob_path, token = msg_str.split(";")
        logger.debug(f'Starting lock monitoring for {blob_path}')
        while True:
            lu = message.locked_until_utc
            n = datetime.datetime.utcnow()
            d = int((lu.replace(tzinfo=n.tzinfo)-n).total_seconds())
            logger.debug(f'locked until {lu} utc now is {n} lock expired {message._lock_expired} and will expire in  {d}')
            if d < 10:
                logger.info('renewing lock')
                await receiver.renew_message_lock(message=message,)
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f'hl error {e}')

def upload_blob(src_path=None, connection_string=None, container_name=None,
                dst_blob_path=None,overwrite=True,max_concurrency=8 ):



    with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
        with blob_service_client.get_blob_client(container=container_name, blob=dst_blob_path) as blob_client:
            with open(src_path, "rb") as upload_file:
                blob_client.upload_blob(upload_file, overwrite=overwrite, max_concurrency=max_concurrency)
            logger.info(f"Successfully wrote PMTiles to {dst_blob_path}")


def run_as_process(f):
    """
    Decorator to run any function inside a separate process. Allows to timeout the  execution.
    Ideally the called function handles gracefully the exceptions by wrapping them inside the com_pipe object

    :param f:
    :return:
    """
    @wraps(f)
    def run(*args,  **kwargs):
        try:
            timeout = kwargs.get('timeout') if 'timeout' in kwargs else 1
            recv_end, send_end = multiprocessing.Pipe(False) #  one way only
            kwargs.update({'com_pipe':send_end})
            p = multiprocessing.Process(target=f, name=f.__name__, args=args, kwargs=kwargs)
            p.start()
            p.join(timeout)
            exit_code = p.exitcode
            #logger.info(f'Process {p.name} has exitcode {exit_code}')
            if exit_code is None:
                sec_label = 'seconds' if timeout > 1 else 'second'
                raise multiprocessing.TimeoutError(f'The function {f.__name__} did not complete successfully in {timeout} {sec_label} with kwargs {kwargs}')
            else:
                if exit_code < 0:
                    raise Exception(f'Process  terminated by signal {exit_code}')
                if exit_code > 0:
                    if p.is_alive():
                        p.terminate()
                    raise Exception(f'Some exception occured in function {f.__name__}. The return code is {exit_code}')

                else:
                    r = recv_end.recv()
                    if isinstance(r, Exception):
                        raise r
                    return r


        except KeyboardInterrupt as ke:
            alive = 'and is going to be terminated' if p.is_alive() else ''
            logger.info(f'Process {p.pid} executing {f.__name__} got {ke.__class__.__name__} {alive}')
            if p.is_alive():
                p.terminate()
            raise



    return run

async def write(file_handle=None, stream=None, offset=None):
    data = await stream.read()
    return await file_handle.write(data, offset=offset )


from aiofile import async_open, AIOFile
async def download_blob(temp_dir=None, conn_string=None, blob_path=None, event=None, nchunks=5 ):

    async def progress(current, total)->None:
        n = current/total*100
        #if n % 10 == 2 and n/10 == 0:
        logger.info(f'download status for {blob_path} - {n:.2f}%' )


    container_name, *rest, file_name = blob_path.split("/")
    logger.info(f'{container_name} - {file_name}')
    container_rel_blob_path = os.path.join(*rest,file_name)
    logger.info(f'{container_name} - {container_rel_blob_path}')
    start = time.time()

    dst_file = os.path.join(temp_dir, file_name)
    async with AContainerClient.from_connection_string(conn_string, container_name,
                                                       max_single_get_size=1*128*1024*1024,
                                                       max_chunk_get_size=4*1024*1024 ) as cc:
        src_blob = cc.get_blob_client(container_rel_blob_path)
        src_props = await src_blob.get_blob_properties()
        size = src_props.size
        content_type = src_props.content_settings
        logger.info(f'Blob size is {size} {content_type} ')

        if nchunks:
            async with AIOFile(dst_file, 'wb') as dstf:
                chunk_size = size//nchunks
                offsets = [i for i in range(0, size-chunk_size, chunk_size)]
                lengths = [chunk_size] * (nchunks-1) + [size-chunk_size*(nchunks-1)]
                tasks = list()
                for offset, length in zip(offsets, lengths):
                    chunk_stream=await cc.download_blob(container_rel_blob_path,
                                                 max_concurrency=1,
                                                 progress_hook=progress,
                                                 offset=offset,
                                                 length=length
                                                 )
                    tasks.append(
                            asyncio.create_task(
                                write(file_handle=dstf,stream=chunk_stream,offset=offset
                            )
                        )
                    )
                results = await asyncio.gather(*tasks)
                assert results==lengths
        else:
            with open(dst_file, 'wb') as dstf:
                stream = await cc.download_blob(container_rel_blob_path,
                                                max_concurrency=8,
                                                progress_hook=progress,

                                                )
                stream.readinto(dstf)


    end = time.time()

    logger.info(f'download lasted {(end-start)/60 } minutes ')
    return dst_file
def download_blob_sync(conn_string=None, blob_path=None, in_chunks=False):

    def progress(current, total)->None:
        logger.info(f'download status for {blob_path} - {current/total*100:.2f}%' )


    container_name, *rest, file_name = blob_path.split("/")
    logger.info(f'{container_name} - {file_name}')
    container_rel_blob_path = os.path.join(*rest,file_name)
    logger.info(f'{container_name} - {container_rel_blob_path}')
    start = time.time()
    with tempfile.TemporaryDirectory() as temp_dir:
        dst_file = os.path.join(temp_dir, file_name)
        with ContainerClient.from_connection_string(conn_string, container_name ) as cc:
            with open(dst_file, 'wb') as dstf:
                stream = cc.download_blob(container_rel_blob_path, max_concurrency=8, progress_hook=progress)
                if in_chunks:
                    for chunk in stream.chunks():
                        dstf.write(chunk)
                else:
                    stream.readinto(dstf)
    end = time.time()

    logger.info(f'download lasted {(end-start)/60 } minutes ')



