import os
import asyncio
from multiprocessing import Event
from ingest.config import raw_folder, setup_env_vars, get_azurewebsubpub_client_token, AZURE_WEBPUBSUB_GROUP_NAME
from ingest.processing import process_geo_file
from ingest.ingest import ingest_message
import logging
from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubclient.models import WebPubSubDataType
if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(
        logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                          "%Y-%m-%d %H:%M:%S"))
    # silence azure logger
    azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azlogger.setLevel(logging.WARNING)
    sblogger = logging.getLogger("uamqp")
    sblogger.setLevel(logging.WARNING)

    logger.handlers.clear()
    logger.addHandler(sthandler)
    logger.name = __name__

    logger.setLevel(logging.DEBUG)
    fpath = '/data/gdp/File_GeoHub_Geodatabase.gdb.zip'
    #fpath = '/data/gdp/a_20231003072549.zip'

    # fpath = '/vsizip/data/featuredataset.gdb.zip'
    fpath = '/data/gdp/Sample1.gpkg'
    fpath = '/data/gdp/Wetlands-WMA 2008-Target-Districts.zip'


    blob_url = 'https://undpgeohub.blob.core.windows.net/userdata/test/CP_CDIS_C_PSY.fgb'
    blob_url = 'https://undpgeohub.blob.core.windows.net/userdata/3ee8a497fdacc1d7b5905048362b7540/raw/Wetlands-WMA%202008-Target-Districts_20231004111757.zip'
    # get  a token valid for
    azure_web_pubsub_client_token = get_azurewebsubpub_client_token(minutes_to_expire=60)
    websocket_client = WebPubSubClient(azure_web_pubsub_client_token['url'], )
    with websocket_client:
        websocket_client.join_group(AZURE_WEBPUBSUB_GROUP_NAME)

        te = Event()

        process_geo_file(blob_url=blob_url,
                         src_file_path=fpath,
                         #conn_string=os.environ.get('AZURE_STORAGE_CONNECTION_STRING'),
                         conn_string=None,
                         timeout_event=te, join_vector_tiles=False, websocket_client=websocket_client)


    #asyncio.run(ingest_message())

