import asyncio
import gc
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


async def ingest_vector(vsiaz_blob_path: str, timeout=3600) -> str:
    # Split blob name on extension and use the resulting name to save the PMTiles file
    basename, _ = os.path.splitext(vsiaz_blob_path)
    vsiaz_pmtiles = basename + ".pmtiles"
    dst_blob_path = vsiaz_pmtiles.replace("/raw/", "/datasets/").replace(
        "/vsiaz/userdata/", ""
    )

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a BlobClient object for the destination blob
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=dst_blob_path
    )

    # Create a temporary file to store the GeoJSON
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".geojson") as temp_geojson:

        # Launch ogr2ogr subprocess to convert the vector file to GeoJSON
        ogr2ogr_cmd = [
            "ogr2ogr",
            "-f",
            "GeoJSONSeq",
            temp_geojson.name,
            vsiaz_blob_path,
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
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".pmtiles") as vector_pmfile:
            # Write GeoJSON to ogr2ogr stdin and wait for it to complete
            # Launch tippecanoe subprocess to convert the GeoJSON to PMTiles
            tippecanoe_cmd = [
                "tippecanoe",
                "-o",
                vector_pmfile.name,
                "--no-feature-limit",
                "-zg",
                "--simplify-only-low-zooms",
                "--detect-shared-borders",
                "--read-parallel",
                "--no-tile-size-limit",
                "--no-tile-compression",
                "--force",
                temp_geojson.name,
            ]

            tippecanoe_proc = await asyncio.create_subprocess_exec(
                *tippecanoe_cmd,
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
                # Wait for tippecanoe to complete
                gc.collect()
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
                    blob_client.upload_blob(vector_pmfile.name, overwrite=True)
                    logger.info(f"Successfully wrote PMTiles to {dst_blob_path}")

                    return dst_blob_path

                else:
                    # Handle the case where tippecanoe_proc failed
                    logger.error(
                        f"Tippecanoe process failed with return code {tippecanoe_proc.returncode}, {tippecanoe_proc.stderr}"
                    )
                    raise Exception(
                        f"Tippecanoe process failed with return code {tippecanoe_proc.returncode}, {tippecanoe_proc.stderr}"
                    )
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
            tippecanoe_proc.kill()
        raise asyncio.TimeoutError(ogr2ogr_cmd + tippecanoe_cmd, timeout)


# def export_with_tippecanoe(
#     src_geojson_file=None,
#     layer_name=None,
#     minzoom=None,
#     maxzoom=None,
#     output_mvt_dir_path=None,
#     timeout=3600 * 10,
# ):

#     """
#     Export a GeoJSON into MVT using tippecanoe
#     :param src_geojson_file: str, the file to be exported
#     :param layer_name: str, the name to be asigne dto the layer
#     :param minzoom: int
#     :param maxzoom: int
#     :param output_mvt_dir_path: str, the folder where the tilers will be exported
#     :return:
#     """

#     out_dir = os.path.join(output_mvt_dir_path, layer_name)
#     logger.info(f"Exporting {src_geojson_file} to {out_dir}")
#     # tippecanoe_cmd =    f'tippecanoe  -l {layer_name} -e {out_dir} ' \
#     #                     f'-z {maxzoom} -Z {minzoom} --allow-existing --no-feature-limit --no-tile-size-limit ' \
#     #                     f'-f {src_geojson_file}'

#     tippecanoe_cmd = f"tippecanoe -z{maxzoom} --no-tile-compression --no-feature-limit --no-tile-size-limit --output-to-directory={out_dir} {src_geojson_file}"

#     # TODO ADD TIMER
#     with subprocess.Popen(
#         shlex.split(tippecanoe_cmd), stdout=subprocess.PIPE, start_new_session=True
#     ) as proc:
#         start = time.time()
#         while proc.poll() is None:
#             output = proc.stdout.readline()
#             if output:
#                 logger.info(output.strip())
#             if timeout:
#                 if time.time() - start > timeout:
#                     proc.terminate()
#                     raise subprocess.TimeoutExpired(tippecanoe_cmd, timeout=timeout)

#     return out_dir

# def vector_to_pmtile2s(filename, timeout=600):
#     # Split filename on extension and use the resulting name to save the PMTiles file
#     basename, extension = os.path.splitext(filename)
#     pmtiles_filename = basename + ".pmtiles"

#     # Launch ogr2ogr subprocess to convert the vector file to GeoJSON
#     ogr2ogr_cmd = [
#         "ogr2ogr",
#         "-f", "GeoJSONSeq",
#         "/vsistdout/",
#         filename
#     ]
#     ogr2ogr_proc = subprocess.Popen(
#         ogr2ogr_cmd,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE
#     )

#     # Launch tippecanoe subprocess to convert the GeoJSON to PMTiles
#     tippecanoe_cmd = [
#         "tippecanoe",
#         "-n", basename,
#         "-A", "attribution, license",
#         "-N", "description of dataset",
#         "-zg",
#         "--simplify-only-low-zooms",
#         "--detect-shared-borders",
#         "--read-parallel",
#         "--no-tile-size-limit",
#         "--no-tile-compression",
#         "--force",
#         "-o", pmtiles_filename,
#         "-L", f'"{{\\"file\\": \\"{basename}.geojson\\", \\"layer\\": \\"layer name\\", \\"description\\": \\"layer description\\"}}"',
#         "/vsistdin/"
#     ]
#     tippecanoe_proc = subprocess.Popen(
#         tippecanoe_cmd,
#         stdin=ogr2ogr_proc.stdout,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE
#     )

#     # Wait for subprocesses to complete or timeout to expire
#     elapsed_time = 0
#     poll_interval = 0.1
#     while True:
#         # Check if both subprocesses have completed
#         ogr2ogr_status = ogr2ogr_proc.poll()
#         tippecanoe_status = tippecanoe_proc.poll()
#         if ogr2ogr_status is not None and tippecanoe_status is not None:
#             break

#         # Check if timeout has expired
#         if elapsed_time >= timeout:
#             ogr2ogr_proc.kill()
#             tippecanoe_proc.kill()
#             raise subprocess.TimeoutExpired(ogr2ogr_cmd + tippecanoe_cmd, timeout)

#         # Sleep for a short time to avoid consuming too much CPU time
#         time.sleep(poll_interval)
#         elapsed_time += poll_interval

#     return pmtiles_filename

# def vector_ingest(filename):
#     subprocess.run(
#         [
#             "ogr2ogr",
#             "-f", "GeoJSONSeq",
#             filename,
#             "|",
#             "tippecanoe",
#             "-n", filename,
#             "-A attribution, license",
#             "-N description of dataset",
#             "-z "{maxzoom}",
#             "-zg",
#             "--simplify-only-low-zooms",
#             "--detect-shared-borders",
#             "--no-feature-limit",
#             "--read-parallel",
#             "--no-tile-size-limit",
#             "--no-tile-compression",
#             "--force",
#             "-o", "test.pmtiles",
#             # "-L", '{
#             #     "file": "test.geojson",
#             #     "layer": "layer name",
#             #     "description": "layer description",
#             #     }',
#         ],
#         stdout=subprocess.DEVNULL,
#     )


#
