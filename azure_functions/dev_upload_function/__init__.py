import logging

import azure.functions as func

from .processing import process


def main(myblob: func.InputStream):
    logging.info("Python Blob trigger function processed %s", myblob.name)

    process(myblob.name.raw)
