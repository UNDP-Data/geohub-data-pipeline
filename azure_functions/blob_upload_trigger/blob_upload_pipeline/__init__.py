import logging

import azure.functions as func
import requests


def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob \n"
        f"User: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )
    try:

        if str(myblob.name).split("/")[2] == "raw":
            params = {"filename": myblob.name}
            requests.get("ingest.undpgeohub.org/ingest", params=params)

    except Exception as e:
        logging.error(f"File is not coming from the user raw upload directory! {e}")
        raise e

    logging.info("Completed processing, and uploaded dataset to Azure Storage.")
