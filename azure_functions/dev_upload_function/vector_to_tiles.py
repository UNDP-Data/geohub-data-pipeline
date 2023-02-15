import subprocess


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
            {
                "file": "test.geojson",
                "layer": "layer name",
                "description": "layer description",
            },
        ],
        stdout=subprocess.DEVNULL,
    )
