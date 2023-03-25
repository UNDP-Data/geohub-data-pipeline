import datetime
import os.path
from io import StringIO
from dotenv import dotenv_values
from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from traceback import print_exc
import random
import asyncio
import logging
cfg = dotenv_values('../../.env')
CONNECTION_STR = cfg['SERVICE_BUS_CONNECTION_STRING_DEV']
QUEUE_NAME='data-upload-dev'
MAX_SLEEP_SECS = 30
LOCK_SECS = 10
TZ = datetime.tzinfo
RUN_FOREVER = False

async def random_sleep(msg=None):

    if '3' in str(msg):raise Exception('Early planned error')
    sleep_secs = random.randrange(0,MAX_SLEEP_SECS,1)
    logger.info(f'Going to sleep for {sleep_secs} seconds')
    await asyncio.sleep(sleep_secs)
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
                    #received_msgs = await receiver.peek_messages(max_message_count=10)
                    if not received_msgs:
                        logger.info(f'No messages to process')
                        if not RUN_FOREVER:break
                        else:
                            await random_sleep()
                    for msg in received_msgs:
                        logger.info(f'Consumer got message {str(msg)}')

                        logger.info(f'Going to lock message {str(msg)} and auto-renew every {LOCK_SECS} secs')
                        async with AutoLockRenewer() as auto_lock_renewer:
                            auto_lock_renewer.register(receiver=receiver, renewable=msg,max_lock_renewal_duration=LOCK_SECS)
                            #bg = asyncio.ensure_future(loop.run_in_executor(None,handle_lock, receiver, msg, LOCK_SECS))
                            job = asyncio.ensure_future(random_sleep(msg=msg))
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




