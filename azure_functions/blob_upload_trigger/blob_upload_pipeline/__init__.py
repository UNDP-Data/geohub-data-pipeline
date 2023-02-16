import logging

import azure.functions as func

from .processing import process


def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob \n"
        f"User: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )

    if str(myblob.name).split("/")[2] == "raw":
        process(myblob.name)
