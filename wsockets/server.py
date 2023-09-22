# Create a websockets client to read ingest messages from the service bus

import asyncio
import os

import websockets
from azure.messaging.webpubsubservice import WebPubSubServiceClient


async def connect():
    service = WebPubSubServiceClient.from_connection_string(os.environ.get("AZURE_WEBPUBSUB_CONNECTION_STRING"),
                                                            hub="Hub")
    token = service.get_client_access_token()
    async with websockets.connect(token.get('url')) as ws:
        print('connected')
        while True:
            print('Received message: ' + await ws.recv())


if __name__ == "__main__":
    # connect()
    asyncio.run(connect())
