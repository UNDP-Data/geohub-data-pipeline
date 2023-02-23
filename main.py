from fastapi import FastAPI
from ingest import ingest

app = FastAPI()

# Mount routers
app.include_router(ingest.app_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
