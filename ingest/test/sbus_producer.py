from dotenv import dotenv_values
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
import random
import asyncio
import logging
cfg = dotenv_values('../../.env')
CONNECTION_STR = cfg['SERVICE_BUS_CONNECTION_STRING_DEV']
QUEUE_NAME='data-upload-dev'
MAX_SLEEP_SECS = 3

async def random_sleep():
    sleep_secs = random.randrange(0,MAX_SLEEP_SECS,1)
    logger.info(f'Producer is going to sleep for {sleep_secs} seconds')
    await asyncio.sleep(sleep_secs)

async def produce():
    i = 0
    async with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True) as servicebus_client:
        async with servicebus_client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
            while True:
                await random_sleep()
                logger.info(f'Pushing message {i}')
                await sender.send_messages(ServiceBusMessage(f'Message no: {i}'))
                i+=1







logging.basicConfig()
logger  = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
asyncio.run(produce())




