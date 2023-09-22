import sys

from azure.messaging.webpubsubservice import WebPubSubServiceClient

service = WebPubSubServiceClient.from_connection_string(connection_string=connection_string, hub='Hub')
token = service.get_client_access_token()
print(token)
# if __name__ == '__main__':
#
#     if len(sys.argv) != 4:
#         print('Usage: python publish.py <connection-string> <hub-name> <message>')
#         exit(1)
#
#     connection_string = sys.argv[1]
#     hub_name = sys.argv[2]
#     message = sys.argv[3]
#
#
#     res = service.send_to_all(message, content_type='text/plain')
#     print(res)