# geohub-data-pipeline
The "geohub-data-pipeline" is an API endpoint that receives the filepath from uploads on the [Geohub](https://github.com/UNDP-Data/geohub) platform and converts them to Cloud-Optimized GeoTIFF (COG) or PMTiles format and stores them in an Azure Blob storage container. This project is designed to be run in a Docker container on AKS.

## Getting Started

### Prerequisites

To use this API, you'll need to have:

- An Azure account with access to an Azure directory called "datasets"
- Docker installed on your local machine
- An AKS cluster set up with kubectl configured to connect to it

### Installation

1. Clone this repository to your local machine.
2. Navigate to the project directory in your terminal or command prompt.
3. Any changes pushed to this repo will automatically trigger a build on the UNDP Data Azure Container Registry. 

## Configuration

To configure the API, you'll need to create an `.env` file in the `deployments/scripts` directory. 

Here are the configuration options:

- `AZURE_ACCESS_KEY`
- `ACCOUNT_NAME`
- `CREDENTIAL_STRING`
- `CONTAINER_NAME`
- `ACCOUNT_URL`
- `CONNECTION_STRING`

### Usage

To use the API, follow these steps:

1. Deploy the Docker container to your AKS cluster by running the command `deployments/scripts/install.sh`.
2. Send a file upload to the endpoint by making a POST request to the external IP address of the service.
3. The API will determine whether the file is a raster or vector file, then convert it to a COG or PMTiles format and store it in the "datasets" directory in Azure.



## Testing the App Locally with Docker Compose

To test the app locally, you can use Docker Compose to build and run the app in a local development environment.

1. Clone the repository to your local machine.

        git clone https://github.com/UNDP-Data/geohub-data-pipeline.git


2. Navigate to the cloned repository.

        cd your-repository

3. Copy the `.env` file in to the root directory of the repository. This file will contain the environment variables that are used by the app. The following variables are required:

- `AZURE_ACCESS_KEY`
- `ACCOUNT_NAME`
- `CREDENTIAL_STRING`
- `CONTAINER_NAME`
- `ACCOUNT_URL`
- `CONNECTION_STRING`


4. Build and run the app using Docker Compose.

        docker-compose up --build


    This command will build the app's Docker image and start the app in a Docker container. The `--build` flag ensures that the image is rebuilt whenever there are changes to the app's code.

5. Once the container is running, you can access the api for the ingest endpoint via `http://localhost:8000/ingest`

6. To stop the app, use the following command:

        docker-compose down

    This command will stop and remove the Docker container.

**Note:** This is only a local development environment. For production deployment, you should use a production-grade Docker image and deploy it to a production environment.