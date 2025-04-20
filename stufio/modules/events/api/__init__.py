from fastapi import APIRouter
from .events import router as events_router
from .admin_topics import router as admin_topics_router 
from stufio.api.admin import admin_router

router = APIRouter()

# Include routes for events
router.include_router(events_router, prefix="/events", tags=["events"])

# Include admin routes
admin_router.include_router(admin_topics_router, prefix="/topics", tags=["kafka"])
