import asyncio
import logging
import os
import subprocess
import tempfile

logger = logging.getLogger(__name__)


async def ingest_vector(vsiaz_blob_path: str, timeout=3600) -> str:
    # Split blob name on extension and use the resulting name to save the PMTiles file
    basename, _ = os.path.splitext(vsiaz_blob_path)
    vsiaz_pmtiles = basename + ".pmtiles"
    dst_blob_path = vsiaz_pmtiles.replace("/working/", "/datasets/")

    # Create a temporary file to store the GeoJSON
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".geojson") as temp_geojson:
        logger.info(f"Converting {vsiaz_blob_path} to {dst_blob_path}")
        logger.info("Beginning conversion with ogr2ogr and tippecanoe")
        # Launch ogr2ogr subprocess to convert the vector file to GeoJSON
        ogr2ogr_cmd = [
            "ogr2ogr",
            "-f",
            "GeoJSON",
            temp_geojson.name,
            vsiaz_blob_path,
        ]
        ogr2ogr_proc = await asyncio.create_subprocess_exec(
            *ogr2ogr_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Launch tippecanoe subprocess to convert the GeoJSON to PMTiles
        tippecanoe_cmd = [
            "tippecanoe",
            "--no-feature-limit",
            "-zg",
            "--simplify-only-low-zooms",
            "--detect-shared-borders",
            "--read-parallel",
            "--no-tile-size-limit",
            "--no-tile-compression",
            "--force",
            "-o",
            dst_blob_path,
            temp_geojson.name,
        ]
        tippecanoe_proc = await asyncio.create_subprocess_exec(
            *tippecanoe_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            logger.info("Waiting for ogr2ogr to complete")
            # Wait for subprocesses to complete or timeout to expire
            await asyncio.wait_for(
                ogr2ogr_proc.wait(),
                timeout=timeout,
            )
            logger.info("Waiting for tippecanoe to complete")
            await asyncio.wait_for(
                tippecanoe_proc.wait(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            ogr2ogr_proc.kill()
            tippecanoe_proc.kill()
            raise subprocess.TimeoutExpired(ogr2ogr_cmd + tippecanoe_cmd, timeout)

    return dst_blob_path


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
