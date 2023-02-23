import logging

from fastapi import APIRouter

from .read import read

app_router = APIRouter()


@app_router.get("/ingest")
async def main(myblob: str):
    logging.info(
        f"Python blob trigger function processed blob \n"
        f"User: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )
    try:

        if str(myblob.name).split("/")[2] == "raw":
            read(myblob.name)

    except Exception as e:
        logging.error(f"Exception! {e}")
        raise e

    logging.info("Completed ingesting, and uploaded dataset to Azure Storage.")
