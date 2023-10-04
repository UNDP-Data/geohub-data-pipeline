import datetime
import multiprocessing
import time
import os
import json
import os.path
from io import StringIO
# from dotenv import dotenv_values
from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from traceback import print_exc
import random
import asyncio
import logging
from ingest.azblob import download_blob_sync
from ingest.utils import chop_blob_url
# cfg = dotenv_values('../../.env')

CONNECTION_STR = os.getenv('SERVICE_BUS_CONNECTION_STRING')
AZ_STORAGE_CONN_STR = os.environ['AZURE_STORAGE_CONNECTION_STRING']
QUEUE_NAME='data-upload'
MAX_SLEEP_SECS = 300
LOCK_SECS = 10
TZ = datetime.tzinfo
RUN_FOREVER = False
azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azlogger.setLevel(logging.WARNING)
sblogger = logging.getLogger("uamqp")
sblogger.setLevel(logging.WARNING)

async def random_sleep(msg=None):

    if '3' in str(msg):raise Exception('Early planned error')
    sleep_secs = random.randrange(0,MAX_SLEEP_SECS,1)
    logger.info(f'Going to sleep for {sleep_secs} seconds')
    await asyncio.sleep(sleep_secs)
    if '4' in str(msg):raise Exception('Late planned error')
    return  sleep_secs

def random_sleep_sync(msg=None):
    if '3' in str(msg):raise Exception('Early planned error')
    sleep_secs = random.randrange(0,MAX_SLEEP_SECS,1)
    logger.info(f'Going to sleep for {sleep_secs} seconds')
    time.sleep(sleep_secs)
    if '4' in str(msg):raise Exception('Late planned error')
    return  sleep_secs

async def handle_lock(receiver=None, message=None, lock_renew_secs = LOCK_SECS):
    while True:
        lu = message.locked_until_utc
        n = datetime.datetime.utcnow()
        d = int((lu.replace(tzinfo=n.tzinfo)-n).total_seconds())
        #logger.info(f'locked until {lu} utc now is {n} lock expired {message._lock_expired} and will expire in  {d}')
        if d < 10:
            logger.info('renewing lock')
            await receiver.renew_message_lock(message=message,)
        await asyncio.sleep(1)

async def consume():
    async with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True) as servicebus_client:
        async with servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, prefetch_count=0,) as receiver: #get one message without caching
            async with receiver:
                while True:
                    received_msgs = await receiver.receive_messages(max_message_count=1, max_wait_time=5)
                    #received_msgs = await receiver.peek_messages(max_message_count=2,)
                    if not received_msgs:
                        logger.info(f'No messages to process')
                        if not RUN_FOREVER:break
                        else:
                            await random_sleep()
                    bllobs = []
                    for msg in received_msgs:
                        #logger.info(f'Consumer got message {str(msg.message.locked_until_utc)}')
                        msg_str = json.loads(str(msg))

                        blob_url, token, join_vector_tiles_str = msg_str.split(";")
                        logger.info(blob_url)
                        blob_path = chop_blob_url(blob_url)
                        logger.info(blob_path)
                        if blob_path in bllobs:break
                        bllobs.append(blob_path)
                        logger.info(bllobs)

                        continue
                        logger.info(f'Going to lock message {str(msg)} and auto-renew every {LOCK_SECS} secs')
                        async with AutoLockRenewer() as auto_lock_renewer:
                            auto_lock_renewer.register(receiver=receiver, renewable=msg,max_lock_renewal_duration=LOCK_SECS)

                            job = asyncio.ensure_future(asyncio.to_thread( random_sleep_sync, msg=msg))
                            bg = asyncio.ensure_future(handle_lock(receiver=receiver,message=msg ))

                            done, pending = await asyncio.wait([bg, job],return_when=asyncio.FIRST_COMPLETED, timeout=36000)
                            for ingest_future in done:
                                try:
                                    res = await ingest_future
                                    await receiver.complete_message(msg)
                                    logger.info(f'{str(msg)} completed with result {res}')
                                    for pf in pending:
                                        pf.cancel()
                                except Exception as e:
                                    for pf in pending:
                                        pf.cancel()
                                    #err = ingest_future.exception()
                                    with StringIO() as m:
                                        print_exc(file=m)
                                        em = m.getvalue()
                                        logger.error(em)
                                    logger.info(f'Pushing {msg} to deadletter subquee')
                                    await receiver.dead_letter_message(msg,reason='ingest error', error_description=em )








if __name__ == '__main__':
    logging.basicConfig()
    logger  = logging.getLogger(os.path.split(__file__)[1])
    logger.setLevel(logging.INFO)

    asyncio.run(consume())




