import logging

import azure.functions as func

from .processing import process


def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob \n"
        f"Name: {myblob.name.raw}\n"
        f"Blob Size: {myblob.length} bytes"
    )

    process(myblob.name.raw)
