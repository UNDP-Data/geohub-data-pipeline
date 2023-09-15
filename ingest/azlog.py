import logging
from azure.storage.blob import ContentSettings, ContainerClient
from urllib.parse import urlparse
import os
class AzureBlobStorageHandler(logging.Handler):
    def __init__(self, connection_string=None, blob_url=None, log_level=None):
        super().__init__()

        self.blob_url = blob_url
        self.connection_string = connection_string
        self.level = log_level
        self.container_client = None
        self.blob_client = None
        self.blob_name = None
        self.createBlob()
        formatter = logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s\n',
                                      "%Y-%m-%d %H:%M:%S")
        self.setFormatter(formatter)


    def createBlob(self):
        # Create a blob client for the log record
        path = urlparse(self.blob_url).path[1:]
        container_name, *rest, blob_name = path.split(os.path.sep)
        name, ext = os.path.splitext(blob_name)
        self.blob_name = os.path.join(*rest,blob_name.replace(ext, '.log'))
        self.container_client = ContainerClient.from_connection_string(conn_str=self.connection_string, container_name=container_name)
        self.blob_client = self.container_client.get_blob_client(self.blob_name)
        content_settings = ContentSettings(content_type="text/plain")
        self.blob_client.create_append_blob(content_settings)


    def emit(self, record):
        # Write the log record to the blob
        log_data = self.format(record).encode("utf-8")

        self.blob_client.append_block(log_data)

    def __del__(self):
        self.container_client.close()

if __name__ == '__main__':

    logging.basicConfig()
    blob_url = 'https://undpgeohub.blob.core.windows.net/userdata/test/Sample.gpkg'
    logger = logging.getLogger(__file__)
    # silence azure logger
    azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azlogger.setLevel(logging.WARNING)
    sblogger = logging.getLogger("uamqp")
    sblogger.setLevel(logging.WARNING)
    container_client = ContainerClient.from_connection_string(os.environ.get('CONNECTION_STRING'), container_name='userdata')
    # Add the Azure Blob Storage handler to the logger
    level = logging.INFO

    handler = AzureBlobStorageHandler(container_client, blob_url=blob_url)
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                      "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    logger.info(f'Welcome to AZURE logging')


    container_client.close()



