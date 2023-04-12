import asyncio
import logging
from ingest.ingest import ingest_message

if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))



    logger.handlers.clear()
    logger.addHandler(sthandler)
    logger.name = __name__

    asyncio.run(ingest_message())
