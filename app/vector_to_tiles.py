import logging
import subprocess

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


def vector_process(filename):
    subprocess.run(
        [
            "ogr2ogr",
            "-f",
            "GeoJSONSeq",
            filename,
            "|",
            "tippecanoe",
            "-n",
            filename,
            "-A",
            "attribution, license",
            "-N",
            "description of dataset",
            "-zg",
            "--simplify-only-low-zooms",
            "--detect-shared-borders",
            "--read-parallel",
            "--no-tile-size-limit",
            "--no-tile-compression",
            "--force",
            "-o",
            "test.pmtiles",
            "-L",
            # '{
            #     "file": "test.geojson",
            #     "layer": "layer name",
            #     "description": "layer description",
            # }',
        ],
        stdout=subprocess.DEVNULL,
    )


#
