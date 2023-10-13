import json
import os
import time
import os
from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubservice import WebPubSubServiceClient


def handle_connect(c, group_name):
    print('handle connect')
    # print('before join second')
    # c.join_group(group_name)
    # print('after join third')

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
    #client.on("connected", lambda e: print(f"Connection {e.connection_id} is connected"))
    client.on("connected", lambda e: handle_connect(client, group_name))
    # client.on("disconnected", lambda e: print(f"Connection disconnected: {e.message}"))
    #client.on("stopped", lambda: handle_stop(client))
    print('first')
    message = {"user": "9426cffc00b069908b2868935d1f3e90", "url": "https://undpgeohub.blob.core.windows.net/userdata/9426cffc00b069908b2868935d1f3e90/raw/wasac-rwasom-2-data-revised_20230912143433.gpkg.zip", "cancel": True}
    with client:
        # A client needs to join the group it wishes to receive messages from
        client.join_group(group_name)

        client.send_to_group(group_name, content=json.dumps(message), data_type="json")


    #client.send_to_group(group_name, content=f"we are done", data_type="text")

    print(client._state)