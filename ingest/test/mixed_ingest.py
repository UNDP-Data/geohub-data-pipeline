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

    logger.setLevel('INFO')
    fpath = '/data/gdp/File_GeoHub_Geodatabase.gdb.zip'
    #fpath = '/data/gdp/a_20231003072549.zip'

    # fpath = '/vsizip/data/featuredataset.gdb.zip'
    fpath = '/data//gdp/Sample1.gpkg'
    # fpath = '/data/Percent_electricity_access_2012.tif'
    # fpath = '/data/devel.tif'

    blob_url = 'https://undpgeohub.blob.core.windows.net/userdata/test/CP_CDIS_C_PSY.fgb'
    # get  a token valid for
    azure_web_pubsub_client_token = get_azurewebsubpub_client_token(minutes_to_expire=60)
    websocket_client = WebPubSubClient(azure_web_pubsub_client_token['url'], )
    with websocket_client:
        websocket_client.join_group(AZURE_WEBPUBSUB_GROUP_NAME)

        te = Event()
        #process_geo_file(blob_url='https://undpgeohub.blob.core.windows.net/userdata/9426cffc00b069908b2868935d1f3e90/raw/File_GeoHub_Geodatabase.gdb.zip',
        process_geo_file(blob_url='https://undpgeohub.blob.core.windows.net/userdata/9426cffc00b069908b2868935d1f3e90/raw/Sample1.gpkg',
                         src_file_path=fpath,
                         #conn_string=os.environ.get('AZURE_STORAGE_CONNECTION_STRING'),
                         conn_string=None,
                         timeout_event=te, join_vector_tiles=False, websocket_client=websocket_client)


    #asyncio.run(ingest_message())

    # fpath = '/home/thuha/Desktop/data/geohub_data_pipeline/File_GeoHub_Geodatabase.gdb'
    # process_geo_file(
    #     blob_url="https://undpgeohub.blob.core.windows.net/userdata/test/File_GeoHub_Geodatabase.gdb",
    #     src_file_path=fpath,
    #     conn_string=os.environ.get("CONNECTION_STRING"),
    #     join_vector_tiles=True
    # )
