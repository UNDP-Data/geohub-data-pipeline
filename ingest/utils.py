import asyncio
import logging
import os.path
from urllib.parse import urlparse
from azure.storage.blob.aio import BlobLeaseClient, BlobServiceClient
from osgeo import gdal
import datetime
from ingest.config import (
    account_url,
    connection_string,
    container_name,
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


async def gdal_open_async(filename):
    logger.info(f"Opening {filename} with GDAL")
    #gdal.SetConfigOption("AZURE_STORAGE_CONNECTION_STRING", connection_string)
    dataset = gdal.OpenEx(filename, gdal.GA_ReadOnly)

    #dataset = await asyncio.to_thread(gdal.OpenEx, filename, gdal.GA_ReadOnly)
    if dataset is None:
        logger.error(f"{filename} does not contain GIS data")
        await upload_error_blob(filename, f"{filename} does not contain GIS data")


    nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
    dataset = None

    return nrasters, nvectors

def gdal_open_sync(filename):

    logger.info(f"Opening {filename} with GDAL")
    dataset = gdal.OpenEx(filename, gdal.GA_ReadOnly)

    if dataset is None:
        logger.error(f"{filename} does not contain GIS data")
        loop = asyncio.get_running_loop()

        loop.run_in_executor(None, upload_error_blob,filename, f"{filename} does not contain GIS data")
    nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
    dataset = None
    return nrasters, nvectors

async def copy_raw2datasets(raw_blob_path: str):
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


async def upload_ingesting_blob(blob_path: str):
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


async def upload_error_blob(blob_path:str=None, error_message:str=None):
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
    while True:
        lu = message.locked_until_utc
        n = datetime.datetime.utcnow()
        d = int((lu.replace(tzinfo=n.tzinfo)-n).total_seconds())
        logger.info(f'locked until {lu} utc now is {n} lock expired {message._lock_expired} and will expire in  {d}')
        if d < 10:
            logger.info('renewing lock')
            await receiver.renew_message_lock(message=message,)
        await asyncio.sleep(1)