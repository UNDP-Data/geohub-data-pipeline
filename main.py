import asyncio

from ingest.ingest import ingest_message

if __name__ == "__main__":
    asyncio.run(ingest_message())
