import asyncio
import logging
import os
import subprocess
import tempfile
from osgeo import gdal
from osgeo.ogr import Layer

from ingest.config import GDAL_ARCHIVE_FORMATS

from azure.storage.blob.aio import BlobServiceClient

from ingest.config import (
    account_name,
    azure_storage_access_key,
    connection_string,
    container_name,
    datasets_folder,
    raw_folder,
)
from ingest.utils import upload_error_blob, upload_ingesting_blob

logger = logging.getLogger(__name__)


def ingest_vector_sync(vsiaz_blob_path: str, timeout=3600):

    dataset = gdal.OpenEx(vsiaz_blob_path, gdal.GA_ReadOnly)

    # if dataset is None:
    #     logger.error(f"{filename} does not contain GIS data")
    #     asyncio.run(upload_error_blob(filename, f"{filename} does not contain GIS data"))
    nvectors = dataset.GetLayerCount()
    for li in range(nvectors):
        l = dataset.GetLayer(li)


    dataset = None



    # # Replace raw folder with datasets folder and remove vsiaz and container name prefix
    # # for upload later in blob service client
    # logger.info(f'vsiaz {vsiaz_blob_path}')
    #
    # prefix, blob_path = vsiaz_blob_path.split(container_name)
    # user_path = blob_path.replace(f'/{raw_folder}/', f'/{datasets_folder}/')
    # logger.info(f'up {user_path}')
    # # Create the PMTiles file path with original raw file name as directory, and new PMTiles file name
    # if any([vsiaz_blob_path.endswith(e) for e in GDAL_ARCHIVE_FORMATS]):
    #     pm_tile_path, orig_ext = os.path.splitext(os.path.basename(user_path))
    # else:
    #     pm_tile_path = os.path.basename(user_path)
    #
    # out_pmtiles_path = f'/{container_name}{user_path}/{pm_tile_path}.pmtiles'
    # logger.info(f'opmtiles {out_pmtiles_path}')
    #
    #
    #
    # asyncio.run(upload_ingesting_blob(out_pmtiles_path))
    #
    # # Convert the input file to GeoJSON and export to PMTiles
    # output_geojson = asyncio.run(ogr2ogr_geojson(vsiaz_blob_path, timeout=timeout))
    # asyncio.run(
    #      tippecanoe_export(
    #         out_pmtiles_path, output_geojson, vsiaz_blob_path, timeout=timeout
    #     )
    # )
    # logger.info(f"PMTiles file created at: {out_pmtiles_path}.")





async def ingest_vector(vsiaz_blob_path: str, timeout=3600):
    # Replace raw folder with datasets folder and remove vsiaz and container name prefix
    # for upload later in blob service client

    user_path = vsiaz_blob_path.replace(
        f"/{raw_folder}/", f"/{datasets_folder}/"
    ).replace(f"/vsiaz/{container_name}/", "")

    # Create the PMTiles file path with original raw file name as directory, and new PMTiles file name
    pm_tile_path = os.path.splitext(os.path.basename(user_path))[0]
    out_pmtiles_path = f"{user_path}/{pm_tile_path}.pmtiles"


    await upload_ingesting_blob(out_pmtiles_path)

    # Convert the input file to GeoJSON and export to PMTiles
    output_geojson = await ogr2ogr_geojson(vsiaz_blob_path, timeout=timeout)
    await tippecanoe_export(
        out_pmtiles_path, output_geojson, vsiaz_blob_path, timeout=timeout
    )

    logger.info(f"PMTiles file created: {out_pmtiles_path}.")


async def ogr2ogr_geojson(blob_path: str, timeout=3600):
    """
    Convert
    """
    # output_geojson = os.path.join("/data/", str(uuid.uuid4()) + ".geojson")
    output_geojson = tempfile.NamedTemporaryFile(mode="w+", suffix=".geojson")
    # Launch ogr2ogr subprocess to convert the vector file to GeoJSON
    ogr2ogr_cmd = [
        "ogr2ogr",
        "-f",
        "GeoJSONSeq",
        output_geojson.name,
        blob_path,
        "-oo",
        "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE=YES",
        "-oo",
        f"AZURE_STORAGE_CONNECTION_STRING={connection_string}",
        "-oo",
        f"AZURE_STORAGE_ACCOUNT={account_name}",
        "-oo",
        f"AZURE_STORAGE_ACCESS_KEY={azure_storage_access_key}",
    ]
    ogr2ogr_proc = await asyncio.create_subprocess_exec(
        *ogr2ogr_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        logger.info("Waiting for ogr2ogr to complete")
        await asyncio.wait_for(
            ogr2ogr_proc.communicate(),
            timeout=timeout,
        )

        if ogr2ogr_proc.returncode == 0:
            logger.debug(f"Successfully wrote GeoJSON to tempfile")
            return output_geojson
        else:
            # Handle the case where ogr2ogr_proc failed
            logger.error(
                f"ogr2ogr process failed with return code {ogr2ogr_proc.returncode}, {ogr2ogr_proc.stderr}"
            )
            await upload_error_blob(
                blob_path,
                f"ogr2ogr process failed with return code {ogr2ogr_proc.returncode}, {ogr2ogr_proc.stderr}",
            )
    except asyncio.TimeoutError:
        ogr2ogr_proc.kill()
        logger.error(f"ogr2ogr process timed out after {timeout} seconds")
        await upload_error_blob(
            blob_path, f"ogr2ogr process timed out after {timeout} seconds"
        )


async def tippecanoe_export(
    out_pmtiles_path: str, output_geojson, blob_path: str, timeout=3600
):
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".pmtiles") as temp_pmfile:
        # Write GeoJSON to ogr2ogr stdin and wait for it to complete
        # Launch tippecanoe subprocess to convert the GeoJSON to PMTiles
        tippecanoe_cmd = [
            "tippecanoe",
            "-o",
            temp_pmfile.name,
            "--no-feature-limit",
            "-zg",
            "--simplify-only-low-zooms",
            "--detect-shared-borders",
            "--read-parallel",
            "--no-tile-size-limit",
            "--no-tile-compression",
            "--force",
            output_geojson.name,
        ]

        tippecanoe_proc = await asyncio.create_subprocess_exec(
            *tippecanoe_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    try:

        logger.info("Waiting for tippecanoe to complete")
        await asyncio.wait_for(
            tippecanoe_proc.communicate(),
            timeout=timeout,
        )
        # Check if tippecanoe_proc was successful
        if tippecanoe_proc.returncode == 0:
            # Close the GeoJSON file
            with output_geojson:
                logger.info(f"Successfully removed temp GeoJSON file")

            # Write the PMTiles file to the blob
            logger.info("Writing PMTiles to blob")
            async with BlobServiceClient.from_connection_string(
                connection_string
            ) as blob_service_client:
                async with blob_service_client.get_blob_client(
                    container=container_name, blob=out_pmtiles_path
                    ) as blob_client:

                    # Check if the blob already exists
                    if await blob_client.exists():
                        await blob_client.delete_blob()

                    with open(temp_pmfile.name, "rb") as upload_pmfile:
                        await blob_client.upload_blob(upload_pmfile, overwrite=True,)

                    logger.info(f"Successfully wrote PMTiles to {out_pmtiles_path}")

        else:
            # Handle the case where tippecanoe_proc failed
            logger.error(
                f"Tippecanoe process failed with return code {tippecanoe_proc.returncode}, {tippecanoe_proc.stderr}"
            )
            await upload_error_blob(
                blob_path,
                f"Tippecanoe process failed with return code {tippecanoe_proc.returncode}, {tippecanoe_proc.stderr}",
            )
    except asyncio.TimeoutError:
        tippecanoe_proc.kill()
        logger.error(f"Tippecanoe process timed out after {timeout} seconds")
        await upload_error_blob(
            blob_path, f"Tippecanoe process timed out after {timeout} seconds"
        )
