import logging
import datetime
from azure.storage.blob import ContentSettings, ContainerClient
from ingest.utils import chop_blob_url
import os
class AzureBlobStorageHandler(logging.Handler):
    def __init__(self, container_client, blob_url=None):
        super().__init__()

        self.blob_url = blob_url
        self.container_client = container_client
        self.blob_client = None
        self.blob_name = None
        self.createBlob()


    def createBlob(self):
        # Create a blob client for the log record
        path = chop_blob_url(blob_url=self.blob_url)
        container_name, *rest, blob_name = path.split(os.path.sep)
        name, ext = os.path.splitext(blob_name)
        self.blob_name = os.path.join(*rest,blob_name.replace(ext, '.log'))
        self.blob_client = self.container_client.get_blob_client(self.blob_name)
        content_settings = ContentSettings(content_type="text/plain")
        self.blob_client.create_append_blob(content_settings)


    def emit(self, record):
        # Write the log record to the blob
        log_data = self.format(record).encode("utf-8")
        self.blob_client.append_block(log_data)


if __name__ == '__main__':
    # silence azure logger
    azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azlogger.setLevel(logging.WARNING)
    sblogger = logging.getLogger("uamqp")
    sblogger.setLevel(logging.WARNING)
    blob_url = 'https://undpgeohub.blob.core.windows.net/userdata/test/Sample.gpkg'
    logger = logging.getLogger()
    container_client = ContainerClient.from_connection_string(os.environ.get('CONNECTION_STRING'), container_name='userdata')
    # Add the Azure Blob Storage handler to the logger
    level = logging.INFO

    handler = AzureBlobStorageHandler(container_client, blob_url=blob_url)
    handler.setLevel(level)

    frmt = "%(asctime)s | %(levelname)s | in %(name)s | %(message)s\n"
    time_format_str = "%Y-%m-%dT%H:%M:%S"
    formatter = logging.Formatter(frmt, time_format_str)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(level)
    logger.info(f'Welcome to AZURE logging')


    container_client.close()



