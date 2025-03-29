from fastapi import APIRouter

from .events import router as events_router
from stufio.api.admin import admin_router, internal_router

router = APIRouter()


# Include internal routes for translations
internal_router.include_router(events_router, prefix="/events", tags=["events"])
