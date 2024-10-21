import asyncio
import multiprocessing
import argparse
import logging
import sys
import os
import hashlib
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_blob_sas, BlobSasPermissions
from osgeo import gdal
import aiohttp
from pmtiles.reader import Reader, MemorySource
from datetime import datetime, timedelta

from ingest.processing import dataset2fgb
from ingest.utils import prepare_arch_path

logging.basicConfig()
logger = logging.getLogger()
sthandler = logging.StreamHandler()
sthandler.setFormatter(
    logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                      "%Y-%m-%d %H:%M:%S"))
logger.handlers.clear()
logger.addHandler(sthandler)
logger.name = __name__
logger.setLevel(logging.INFO)

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# docker run --rm -it  -v .:/data -v ./ingest:/usr/src/app/ingest dockerfiles-app  python ./ingest/cli/upload_fgb.py -h


def generate_userid(user_email):
    if user_email:
        m = hashlib.md5(user_email.encode())
        return m.hexdigest()
    else:
        return


def get_blob_container(container_name):
    blob_service_client = BlobServiceClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING
    )
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    return container_client


async def get_layer_names(file_path):
    async with aiohttp.ClientSession() as session:
        async with session.get(file_path) as response:
            if response.status != 200:
                raise Exception(f"Failed to retrieve PMTiles from {file_path}, status code: {response.status}")

            data = await response.read()
            source = MemorySource(data)
            reader = Reader(source)
            metadata = reader.metadata()
            vector_layers = metadata.get("vector_layers", [])
        layer_names = [layer.get("id") for layer in vector_layers if "id" in layer]
        return layer_names


def generate_sas_url(container_client, blob_name):
    parts = dict(item.split("=", 1) for item in AZURE_STORAGE_CONNECTION_STRING.split(";") if "=" in item)
    account_name = parts.get("AccountName")
    account_key = parts.get("AccountKey")

    container_name = container_client.container_name

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    return sas_token


def download_blob(blob_client: BlobClient, download_path: str):
    """Download a blob to a local file with a progress bar."""

    logger.info(f"Downloading {blob_client.name} to {download_path}")
    # blob_properties = blob_client.get_blob_properties()
    # total_size = blob_properties.size

    with open(download_path, "wb") as f:
        stream = blob_client.download_blob()
        # progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading blob")
        for chunk in stream.chunks():
            f.write(chunk)
        #     progress_bar.update(len(chunk))
        # progress_bar.close()
    logger.info(f"Downloaded {blob_client.blob_name} to {download_path}")


async def ingest_user_folder(user_id, container_client, dist_dir, timeout_event: multiprocessing.Event = None,):
    print(user_id)
    dataset_path = f"{user_id}/datasets"

    # find pmtiles files in datasets folder
    for blob in container_client.list_blobs(name_starts_with=dataset_path):
        if blob.name.split(".")[-1] != 'pmtiles':
            continue
        pmtiles_path = blob.name

        sas_url = generate_sas_url(container_client, pmtiles_path)
        pmtiles_url = f"{container_client.url}/{pmtiles_path}?{sas_url}"

        layers = await get_layer_names(pmtiles_url)
        layer_count = len(layers)
        if layer_count == 0:
            continue
        else:
            # single layer
            parts = pmtiles_path.split('/')

            join_vector_tiles =  layer_count == 1
            print(f"join_vector_tiles: {join_vector_tiles}")
            raw_file = f"{container_client.url}/{user_id}/raw/{parts[2]}"
            raw_blob_name = parts[2]
            raw_file_path = os.path.join(dist_dir, raw_blob_name)

            blob_list = [blob for blob in container_client.list_blobs(name_starts_with=f"{user_id}/raw/{parts[2]}") if blob.name == f"{user_id}/raw/{parts[2]}"]

            print(f"blob_list: {len(blob_list)}")
            if not blob_list:
                continue
            blob_client = blob_list[0]

            download_blob(blob_client, raw_file_path)
            src_file_path = prepare_arch_path(src_path=raw_file_path)
            try:
                vdataset = gdal.OpenEx(src_file_path, gdal.OF_VECTOR)
            except RuntimeError as ioe:
                if 'supported' in str(ioe):
                    vdataset = None
                else:
                    raise
            if vdataset is not None:
                logger.info(f'Opened {raw_file} with {vdataset.GetDriver().ShortName} vector driver')
                nvector_layers = vdataset.GetLayerCount()
                if nvector_layers > 0:
                    if not join_vector_tiles:
                        # multi layers
                        for layer in layers:
                            fgb_layers = dataset2fgb(fgb_dir=dist_dir,
                                                     src_ds=vdataset,
                                                     layers=[layer],
                                                     timeout_event=timeout_event,
                                                     conn_string=AZURE_STORAGE_CONNECTION_STRING,
                                                     blob_url=raw_file)
                            print(fgb_layers)
                    else:
                        # single layers
                        fgb_layers = dataset2fgb(fgb_dir=dist_dir,
                                                 src_ds=vdataset,
                                                 layers=layers,
                                                 timeout_event=timeout_event,
                                                 conn_string=AZURE_STORAGE_CONNECTION_STRING,
                                                 blob_url=raw_file)
                        print(fgb_layers)



        print(layers)


async def main():

    parser = argparse.ArgumentParser(
        description='Convert previous vector data to flatgeobuf and upload them to blob storage',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-u', '--user',
                        help='User email address to process',
                        type=str, )
    parser.add_argument('-c', '--container',
                        help='Target container name of blob storage',
                        type=str, default="userdata")
    parser.add_argument('-dst', '--destination-directory',
                        help='A full absolute path to a folder where the files will be written.',
                        type=str, )
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Set log level to debug', default=False
                        )

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.debug:
        logger.setLevel(logging.DEBUG)
    timeout_event = multiprocessing.Event()

    dist_dir = args.destination_directory
    if not os.path.exists(dist_dir):
        os.mkdir(dist_dir)

    user_id = generate_userid(args.user)
    print(user_id)

    container_client = get_blob_container(args.container)
    if not user_id:
        user_ids = list(
            set([blob.name.split("/")[0] for blob in container_client.list_blobs() if
                 blob.name.split("/")[0] != "test"])
        )

        for id in user_ids:
            await ingest_user_folder(id, container_client, dist_dir, timeout_event=timeout_event)
    else:
        await ingest_user_folder(user_id, container_client, dist_dir, timeout_event=timeout_event)

if __name__ == '__main__':
    asyncio.run(main())


