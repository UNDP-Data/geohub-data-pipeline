from fastapi import APIRouter

from .ingest import app_router

router = APIRouter()

# Mount routers
router.include_router(app_router, prefix="/ingest")
