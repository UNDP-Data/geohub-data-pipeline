import asyncio
import logging
import os

from azure.storage.blob.aio import BlobLeaseClient, BlobServiceClient
from osgeo import gdal

from ingest.config import account_url, connection_string, container_name
from ingest.ingest_exceptions import (
    ClientRequestError,
    CopyOperationError,
    InvalidDataException,
    ResourceNotFoundError,
)

logger = logging.getLogger(__name__)


def prepare_blob_path(blob_path: str) -> str:

    container_blob_path = blob_path.split(account_url)[1]

    return container_blob_path


def prepare_vsiaz_path(blob_path: str) -> str:
    return f"/vsiaz/{blob_path}"


def gdal_open(filename):

    logger.info(f"Opening {filename} with GDAL")
    gdal.SetConfigOption("AZURE_STORAGE_CONNECTION_STRING", connection_string)
    dataset = gdal.OpenEx(filename, gdal.GA_ReadOnly)

    if dataset is None:
        raise InvalidDataException(f"{filename} does not contain GIS data")

    nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
    dataset = None
    return nrasters, nvectors


async def copy_raw2datasets(raw_blob_path: str) -> str:
    """
    Copy raw_blob to the datasets directory
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        container_name, *rest = raw_blob_path.split("/")
        blob_path = "/".join(rest)
        logger.info(f"container_name: {container_name}")
        logger.info(f"blob_path: {blob_path}")

        async with blob_service_client:
            container_client = blob_service_client.get_container_client(container_name)

            src_blob = container_client.get_blob_client(blob_path)
            src_props = await src_blob.get_blob_properties()

            async with BlobLeaseClient(client=src_blob) as lease:
                await lease.acquire(30)

                dst_blob_path = get_dst_blob_path(blob_path)
                logger.info(f"Copying {raw_blob_path} to {dst_blob_path}")
                dst_blob = container_client.get_blob_client(dst_blob_path)

                copy = await asyncio.wait_for(
                    dst_blob.start_copy_from_url(src_blob.url, overwrite=True),
                    timeout=30,
                )

                dst_props = await dst_blob.get_blob_properties()

                if dst_props.copy.status != "success":
                    await dst_blob.abort_copy(dst_props.copy.id)
                    raise CopyOperationError(
                        f"Failed to copy {raw_blob_path} to {dst_blob_path}"
                    )
                else:
                    return os.path.join(container_name, dst_blob_path)

    except (ResourceNotFoundError, ClientRequestError) as e:
        logger.error(f"Failed to copy {raw_blob_path}: {e}")
        raise e

    except asyncio.TimeoutError:
        logger.error(f"Copy operation timed out for {raw_blob_path}")
        raise CopyOperationError(
            f"Copy operation timed out for {raw_blob_path} to {dst_blob_path}"
        )


def get_dst_blob_path(blob_path: str) -> str:
    blob_path.replace("/raw/", "/datasets/")
    pmtile_name = blob_path.split("/")[-1]
    return f"{blob_path}/{pmtile_name}"


async def upload_ingesting_blob(blob_path: str) -> bool:
    ingesting_blob_path = f"{blob_path}.ingesting"
    # Upload the ingesting file to the blob
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    async with blob_service_client.get_blob_client(
        container=container_name, blob=ingesting_blob_path
    ) as blob_client:
        await blob_client.upload_blob(b"ingesting", overwrite=True)
    return True


async def delete_ingesting_blob(blob_path: str) -> bool:
    ingesting_blob_path = f"{blob_path}.ingesting"
    # Upload the ingesting file to the blob
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    async with blob_service_client.get_blob_client(
        container=container_name, blob=ingesting_blob_path
    ) as blob_client:
        await blob_client.delete_blob()
    return True
