import asyncio
import os

import websockets
from azure.messaging.webpubsubservice import WebPubSubServiceClient
from websockets.sync.client import connect


def send_message(message:any):
    with WebPubSubServiceClient.from_connection_string(
            connection_string=os.environ.get("AZURE_WEBPUBSUB_CONNECTION_STRING"), hub="Hub") as service_client:
        res = service_client.send_to_all(message, content_type='application/json')
        print(res)


message = {"user": "a85516c81c0b78d3e89d3f00099b8b15",
           "url": "https://undpgeohub.blob.core.windows.net/userdata/a85516c81c0b78d3e89d3f00099b8b15/raw/wasac-rwasom-2-data-revised_20230912143433.gpkg",
           "progress": 30,
           "stage": 'Ingesting'}

# async def send_message():
#     with WebPubSubServiceClient.from_connection_string(
#             connection_string=os.environ.get("AZURE_WEBPUBSUB_CONNECTION_STRING"), hub="Hub") as service_client:
#         async with websockets.connect(service_client.get_client_access_token().get('url')) as ws:
#             await ws.send("Hello from Joseph 2")


if __name__ == "__main__":
    send_message("Message")
    # asyncio.run(send_message())
