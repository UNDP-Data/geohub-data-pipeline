# fgb upload script

This script scan all users or a specific user folder in Azure Blob Storage to search existing vector PMTiles datasets. Then, it processes original vector data into flatgeobuf formats for uploading.

## Usage

The easiest way to execute this CLI tool is to use docker compose.

```shell
docker compose -f dockerfiles/docker-compose_cli.yaml build

# process all users
docker compose -f dockerfiles/docker-compose_cli.yaml run geohub-data-pipeline python -m ingest.fgb_upload.main -dst /data

# process a specific user
docker compose -f dockerfiles/docker-compose_cli.yaml run geohub-data-pipeline python -m ingest.fgb_upload.main -u {user-email} -dst /data
```

If `-u {user-email}` is not specified, all users will be scanned. If fgb is already uploaded by the data pipeline, the dataset will be skipped.