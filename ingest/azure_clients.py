import logging
import os

from azure.storage.blob.aio import BlobLeaseClient, BlobServiceClient

from ingest.config import connection_string, container_name

logger = logging.getLogger(__name__)


async def copy_raw2working(raw_blob_path: str) -> str:
    """
    Copy raw_blob to the working directory
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    src_blob = None
    async with blob_service_client:
        # Instantiate a ContainerClient
        # async for container in blob_service_client.list_containers(include_metadata=True):
        #     logger.info(container)
        try:
            conatainer_name, *rest = raw_blob_path.split("/")
            blob_path = "/".join(rest)
            container_client = blob_service_client.get_container_client(container_name)

            # [START copy_blob_from_url]
            # Get the blob client with the source blob

            src_blob = container_client.get_blob_client(blob_path)
            src_props = await src_blob.get_blob_properties()

            # lock the blob
            if src_props.lease.state != "leased":
                lease = BlobLeaseClient(client=src_blob)
                await lease.acquire(30)

            dst_blob_path = blob_path.replace("/raw/", "/working/")
            dst_blob = container_client.get_blob_client(dst_blob_path)

            # # start copy and check copy status
            copy = await dst_blob.start_copy_from_url(src_blob.url, timeout=30)

            dst_props = await dst_blob.get_blob_properties()

            # # Passing in copy id to abort copy operation
            if dst_props.copy.status != "success":
                await dst_blob.abort_copy(dst_props.copy.id)
                raise Exception(f"failed to copy {raw_blob_path} tgo {dst_blob_path}")
            else:
                return os.path.join(conatainer_name, dst_blob_path)

        except Exception as e:
            if src_props.size != dst_props.size:
                dst_blob.delete_blob(delete_snapshots=True)
            logger.error(f"Failed to copy {raw_blob_path} to {dst_blob_path}")
            raise e

        finally:
            if "lease" in locals() and src_props.lease.state == "leased":
                await lease.release()
            logger.debug(f"Finished copying {raw_blob_path}")
