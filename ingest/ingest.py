import json
import logging
import os

from azure.servicebus import TransportType
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
sblogger = logging.getLogger("uamqp")
sblogger.setLevel(logging.WARNING)

CONNECTION_STR = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]


async def ingest_message():
    async with ServiceBusClient.from_connection_string(
        conn_str=CONNECTION_STR,
        logging_enable=True,
        transport_type=TransportType.AmqpOverWebsocket,
    ) as servicebus_client:
        async with AutoLockRenewer(max_lock_renewal_duration=3600) as renewer:
            async with servicebus_client.get_queue_receiver(
                queue_name=QUEUE_NAME
            ) as receiver:
                async for msg in receiver:
                    renewer.register(receiver, msg, max_lock_renewal_duration=3600)

                    try:
                        # ServiceBusReceiver instance is a generator.
                        # the message has double quotes, it is better to sue JSON parser to be on the safe side
                        msg_str = json.loads(str(msg))
                        blob_path, token = msg_str.split(";")
                        logger.info(f"Received blob: {blob_path} in message and token {token}")

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
                    except Exception as e:
                        logger.error("Error processing message: ", str(e))
                        raise

    #await servicebus_client.close()


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
        #csv


    logger.info(f"Finished ingesting {blob_path}")
