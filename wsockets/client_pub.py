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
    with client:
        # A client needs to join the group it wishes to receive messages from
        client.join_group(group_name)
        for i in range(10):
            print(f'before send message {i}')
            client.send_to_group(group_name, content=f"hello world {i}", data_type="text")
            print(f'after send message {i}')
            time.sleep(1)

    client.send_to_group(group_name, content=f"we are done", data_type="text")

    print(client._state)