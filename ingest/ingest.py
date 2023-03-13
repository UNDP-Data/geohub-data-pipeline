import logging
import os
import time

from azure.servicebus.aio import AutoLockRenewer, ServiceBusClient

from ingest.config import raw_folder
from ingest.raster_to_cog import ingest_raster
from ingest.utils import (
    copy_raw2datasets,
    gdal_open,
    prepare_blob_path,
    prepare_vsiaz_path,
)
from ingest.vector_to_tiles import ingest_vector

logger = logging.getLogger(__name__)

# silence azure logger
azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azlogger.setLevel(logging.WARNING)

CONNECTION_STR = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]


async def ingest_message():
    async with ServiceBusClient.from_connection_string(CONNECTION_STR) as client:
        # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
        # Default is None; to receive forever.
        async with client.get_queue_receiver(QUEUE_NAME) as receiver:
            # Receive messages from the queue and begin ingesting the data
            messages = await receiver.receive_messages(
                max_wait_time=5, max_message_count=2
            )
            for msg in messages:
                # ServiceBusReceiver instance is a generator.
                blob_path = str(msg).split(";")[0]
                token = str(msg).split(";")[1]
                logger.info(f"Received message: {blob_path}")
                if f"/{raw_folder}/" in blob_path:
                    await ingest(blob_path, token)
                    await receiver.complete_message(msg)
                    logger.info(f"Completed message for: {blob_path}")
                else:
                    logger.info(
                        f"Skipping {blob_path} because it is not in the {raw_folder} folder"
                    )
                    await receiver.complete_message(msg)
                    logger.info(f"Completed message for: {blob_path}")

    logger.info("Finished receiving messages")


async def ingest(blob_path: str, token=None):
    """
    Ingest a geospatial data file potentially containing multiple layers
    into geohub
    Follows exactly https://github.com/UNDP-Data/geohub/discussions/545

    """
    logger.info(f"Starting to ingest {blob_path}")
    # if the file is a pmtiles file, return without ingesting, copy to datasets
    container_blob_path = prepare_blob_path(blob_path)
    if blob_path.endswith(".pmtiles"):
        await copy_raw2datasets(raw_blob_path=container_blob_path)
    else:
        vsiaz_path = prepare_vsiaz_path(container_blob_path)
        nrasters, nvectors = await gdal_open(vsiaz_path)

        # 2 ingest
        if nrasters > 0:
            await ingest_raster(vsiaz_blob_path=vsiaz_path)
        if nvectors > 0:
            await ingest_vector(vsiaz_blob_path=vsiaz_path)

    logger.info(f"Finished ingesting {blob_path}")
    return True
