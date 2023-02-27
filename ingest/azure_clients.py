from azure.storage.blob import ContainerClient
from rasterio.session import AzureSession
from azure.storage.blob.aio import BlobServiceClient, BlobLeaseClient
import logging
from .config import account_url, azure_storage_access_key, container_name, connection_string
logger = logging.getLogger(__name__)

def azure_container_client():
    container_client = ContainerClient(
        account_url=account_url,
        container_name=container_name,
        credential=azure_storage_access_key,
    )
    return container_client


async def copy_raw2working(raw_blob_path:str=None)->str:
    """
    Copy raw_blob to the working directory
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    logger.info(f'cn: {container_name} {blob_service_client}')
    async with blob_service_client:
        # Instantiate a ContainerClient
        # async for container in blob_service_client.list_containers(include_metadata=True):
        #     logger.info(container)
        try:
            container_client = blob_service_client.get_container_client(container_name)
            print(container_client)
            # [START copy_blob_from_url]
            # Get the blob client with the source blob

            src_blob = container_client.get_blob_client(raw_blob_path)
            src_props = await src_blob.get_blob_properties()

            #lock the blob
            lease = BlobLeaseClient(client=src_blob)
            lease.acquire(-1)
            dst_blob_path = raw_blob_path.replace('raw', 'working')
            dst_blob = container_client.get_blob_client(dst_blob_path)

            # # start copy and check copy status
            copy = await dst_blob.start_copy_from_url(src_blob.url, timeout=3)

            dst_props = await dst_blob.get_blob_properties()

            # # Passing in copy id to abort copy operation
            if dst_props.copy.status != "success":
                await dst_blob.abort_copy(dst_props.copy.id)
            else:
                return dst_blob_path

        except Exception as e:
            if src_props.size != dst_props.size:
                dst_blob.delete_blob(delete_snapshots=True)
            logger.error(f'Failed to copy {raw_blob_path} to {dst_blob_path}')
            raise

        finally:
            if src_props.lease.state == 'leaded':
                lease.break_lease()
            #await blob_service_client.delete_container("copyblobcontainerasync")
            logger.debug(f'Finished copying {raw_blob_path}')