import asyncio
import logging
import os
import subprocess
import tempfile

from azure.storage.blob import BlobServiceClient

from ingest.config import (
    account_name,
    azure_storage_access_key,
    connection_string,
    container_name,
)

logger = logging.getLogger(__name__)


async def ingest_vector(vsiaz_blob_path: str, timeout=3600):
    # Split blob name on extension and use the resulting name to save the PMTiles file
    basename, _ = os.path.splitext(vsiaz_blob_path)
    vsiaz_pmtiles = basename + ".pmtiles"
    dst_blob_path = vsiaz_pmtiles.replace("/raw/", "/datasets/").replace(
        "/vsiaz/userdata/", ""
    )

    output_geojson = await ogr2ogr_geojson(vsiaz_blob_path, timeout=timeout)
    logger.info(f"Successfully wrote GeoJSON to {output_geojson}")

    await tippecanoe_export(dst_blob_path, output_geojson, timeout=timeout)

    output_geojson.close()
    logger.info(f"Successfully removed GeoJSON file {output_geojson}")


async def ogr2ogr_geojson(blob_path: str, timeout=3600):
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
            logger.info(f"Successfully wrote GeoJSON to {output_geojson}")
            return output_geojson
        else:
            # Handle the case where tippecanoe_proc failed
            logger.error(
                f"Ogr2ogr process failed with return code {ogr2ogr_proc.returncode}, {ogr2ogr_proc.stderr}"
            )
        raise Exception(
            f"Ogr2ogr process failed with return code {ogr2ogr_proc.returncode}, {ogr2ogr_proc.stderr}"
        )
    except asyncio.TimeoutError:
        ogr2ogr_proc.kill()

    raise asyncio.TimeoutError(ogr2ogr_cmd, timeout)


async def tippecanoe_export(dst_blob_path: str, output_geojson, timeout=3600):
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string,
        max_single_put_size=4 * 1024 * 1024,
        max_single_get_size=4 * 1024 * 1024,
    )

    # Get a BlobClient object for the destination blob
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=dst_blob_path
    )
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
            # Write the output of tippecanoe to the blob if it exists
            logger.info("Writing PMTiles to blob")
            # Write the PMTiles file to the blob
            with open(temp_pmfile.name, "rb") as upload_pmfile:
                blob_client.upload_blob(upload_pmfile, overwrite=True)
            logger.info(f"Successfully wrote PMTiles to {dst_blob_path}")

            return True

        else:
            # Handle the case where tippecanoe_proc failed
            logger.error(
                f"Tippecanoe process failed with return code {tippecanoe_proc.returncode}, {tippecanoe_proc.stderr}"
            )
            raise Exception(
                f"Tippecanoe process failed with return code {tippecanoe_proc.returncode}, {tippecanoe_proc.stderr}"
            )

    except asyncio.TimeoutError:
        tippecanoe_proc.kill()
    raise asyncio.TimeoutError(tippecanoe_cmd, timeout)
