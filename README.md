# geohub-data-pipeline
The "geohub-data-pipeline" is a service that receives the filepath of an Azure blob from a service bus queue from uploads to the [Geohub](https://github.com/UNDP-Data/geohub) platform and converts them to Cloud-Optimized GeoTIFF (COG) or PMTiles format and stores them in an Azure Blob storage container. This project is designed to be run in a Docker container on AKS as a deployment.

## Getting Started

### Prerequisites

To use this API, you'll need to have:

- An Azure account with access to an Azure Blob storage container for the UNDP Data Geohub
- Docker installed on your local machine
- An AKS cluster set up with kubectl configured to connect to it

### Installation

1. Clone this repository to your local machine.
2. Navigate to the project directory in your terminal or command prompt.
3. Any changes pushed to this repo will automatically trigger a build on the UNDP Data Azure Container Registry. 

## Configuration

To configure the API, you'll need to create an `.env` file in the `deployments/scripts` directory. 

Here are the configuration options:

- `AZURE_STORAGE_CONNECTION_STRING`
- `SERVICE_BUS_CONNECTION_STRING`
- `SERVICE_BUS_QUEUE_NAME`	

### Usage

To use the API, follow these steps:

1. Deploy the Docker container to your AKS cluster by running the command `deployments/scripts/install.sh`.
2. Upload a file on the Geohub dev app.
3. A message will be sent to the service bus queue with the filepath of the uploaded file, and the user token for authentication.
4. The API will determine whether the file is a raster or vector file, then convert it to a COG or PMTiles format and store it in the user's "datasets" directory in Azure.



## Testing the App Locally with Docker Compose

To test the app locally, you can use Docker Compose to build and run the app in a local development environment.

1. Clone the repository to your local machine.

        git clone https://github.com/UNDP-Data/geohub-data-pipeline.git


2. Navigate to the cloned repository.

        cd geohub-data-pipeline

3. Copy the `.env` file in to the root directory of the repository. This file will contain the environment variables that are used by the app. The following variables are required:

- `AZURE_STORAGE_CONNECTION_STRING`
- `SERVICE_BUS_CONNECTION_STRING`
- `SERVICE_BUS_QUEUE_NAME`

4. Build and run the app using Docker Compose.

        docker-compose up --build


    This command will build the app's Docker image and start the app in a Docker container. The `--build` flag ensures that the image is rebuilt whenever there are changes to the app's code.

5. Once the container is running, it will automatically begin scanning for messages in the service bus queue. If a message is found, it will be processed and the file will be converted to a COG or PMTiles format and stored in the user's "datasets" directory in Azure.

6. To stop the app, use the following command:

        docker-compose down

    This command will stop and remove the Docker container.

**Note:** This is only a local development environment. For production deployment, you should use a production-grade Docker image and deploy it to a production environment.


## Running the pipeline as a command line tool
While the pipeline was designed to be executed within and leverage cloud environment, sometimes it may be desirable
to run it as a command line tool. 
In general the best option ot run GDAL dependent python software is through docker.
This is to avoid potential issues that could arise from GDAL dependencies and version mismatch.

An viable alternative would be to create a setup.py/pyproject.toml but that
assumes the user has a  decent understanding of GDAL internals.

For the sake of simplicity we choose to rely on docker.

1. Clone the repository to your local machine.

        git clone https://github.com/UNDP-Data/geohub-data-pipeline.git


2. Navigate to the cloned repository.

        cd geohub-data-pipeline

3. Build the docker image and assign it a meaningful tag

   ```
   docker build . -t geohub-data-pipeline
   ```
4. Run the command line version
   ```commandline
   docker run --rm -it  -v /home/janf/Downloads/data:/data geohub-data-pipeline python -m ingest.cli.main -src /data/Sample.gpkg -dst /data/out/ 

   ```
The above command will run the previously built image in a new container,
will remove it afterwards, maps the "/home/janf/Downloads/data" folder as **/data** folder inside the container and finally
call the command line version of the pipeline to convert all vector layers to PMTiles and raster bands to COG format

if the command line script is called without any args the help is displayed
```commandline
docker run --rm -it  -v /home/janf/Downloads/data:/data geohub-data-pipeline python -m ingest.cli.main
/usr/local/lib/python3.10/dist-packages/rio_cogeo/profiles.py:182: UserWarning: Non-standard compression schema: zstd. The output COG might not be fully supported by software not build against latest libtiff.
  warnings.warn(
usage: main.py [-h] [-src SOURCE_FILE] [-dst DESTINATION_DIRECTORY] [-j] [-d]

Convert layers/bands from GDAL supported geospatial data files to COGs/PMtiles.

options:
  -h, --help            show this help message and exit
  -src SOURCE_FILE, --source-file SOURCE_FILE
                        A full absolute path to a geospatial data file. (default: None)
  -dst DESTINATION_DIRECTORY, --destination-directory DESTINATION_DIRECTORY
                        A full absolute path to a folder where the files will be written. (default: None)
  -j, --join-vector-tiles
                        Boolean flag to specify whether to create a multilayer PMtiles filein case the source file contains more then one vector layer (default: False)
  -d, --debug           Set log level to debug (default: False)

```

### FAQs
1. What geospatial formats are supported?</p>
ANSWER: anything GDAL supports. Beware the image uses latest GDAL image (3.7)
2. What does the tool do? </p>
ANSWER: - converts all vector layers to PMtiles format
        - optionally create a multilayer PMtiles in case join argument is set to True
        - convert all raster bands or RGB rasters into COG, featuring zstd compression and 
          Google Web mercator projection
3. Are any environmental variables needed? </p>
ANSWER: NO



        
