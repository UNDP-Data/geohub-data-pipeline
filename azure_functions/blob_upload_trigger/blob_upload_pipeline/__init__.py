import logging

import azure.functions as func

from .processing import process


def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob \n"
        f"User: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )
    try:

        if str(myblob.name).split("/")[2] == "raw":
            process(myblob.name)

    except Exception as e:
        logging.error(f"Exception! {e}")
        raise e

    logging.info("Completed processing, and uploaded dataset to Azure Storage.")
