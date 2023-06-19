from dotenv import dotenv_values
cfg = dotenv_values('../../.env')
CONNECTION_STR = cfg['SERVICE_BUS_CONNECTION_STRING']
QUEUE_NAME='data-upload'
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
import random
import asyncio
import logging


async def random_sleep():
    sleep_secs = random.randrange(0,6,1) * 60
    logger.info(f'Going to sleep for {sleep_secs}')
    await asyncio.sleep(sleep_secs)

async def produce():
    i = 0
    async with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True) as servicebus_client:
        async with servicebus_client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
            await sender.send_messages(ServiceBusMessage(f'Message no: {i}'))




async def consume():
    async with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True) as servicebus_client:
        async with servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME) as receiver:
            async with receiver:
                # received_msgs = await receiver.peek_messages(max_message_count=2)
                # for msg in received_msgs:
                #     logger.info(str(msg))
                # exit()
                while True:
                    received_msgs = await receiver.receive_messages(
                        max_message_count=1, max_wait_time=5
                    )

                    if not received_msgs:
                        logger.info(f'No (more) messages to process. Queue "{QUEUE_NAME}" is empty')
                        break

                    for msg in received_msgs:
                        logger.info(str(msg))



#asyncio.run(produce())

logging.basicConfig()
logger  = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
asyncio.run(consume())




