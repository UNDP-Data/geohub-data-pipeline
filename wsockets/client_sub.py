import json

from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubservice import WebPubSubServiceClient
import os
def handle_message(e):
    json_data = json.loads(e.data)
    _, file_name = os.path.split(json_data['url'])
    name, *ext = file_name.split(os.extsep)
    print(file_name,json_data['progress'], json_data.get('layer'))

if __name__ == '__main__':
    AZURE_WEBPUBSUB_CONNECTION_STRING = os.environ.get('AZURE_WEBPUBSUB_CONNECTION_STRING')
    service_client = WebPubSubServiceClient.from_connection_string(
        connection_string=AZURE_WEBPUBSUB_CONNECTION_STRING,
        hub='Hub'
    )
    group_name = 'datapipeline'
    token = service_client.get_client_access_token(
        user_id='geohub-data-pipeline',
        roles=[f"webpubsub.joinLeaveGroup.{group_name}",
               f"webpubsub.sendToGroup.{group_name}"])
    service_client.close()
    client = WebPubSubClient(token['url'])
    client.on("connected", lambda e: print(f"Connection {e.connection_id} is connected"))
    client.on("disconnected", lambda e: print(f"Connection disconnected: {e.message}"))
    client.on("stopped", lambda: print("Client has stopped"))
    client.on("group-message", lambda e: handle_message(e))
    with client:

        # A client needs to join the group it wishes to receive messages from
        client.join_group(group_name)
        while True:
            pass