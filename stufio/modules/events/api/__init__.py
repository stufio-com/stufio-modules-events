from fastapi import APIRouter
from .admin_events import router as admin_events_router
from .admin_topics import router as admin_topics_router 
from stufio.api.admin import admin_router

router = APIRouter()

# Include admin routes for events
admin_router.include_router(admin_events_router, prefix="/events", tags=["events"])

# Include admin routes for Kafka topics
admin_router.include_router(admin_topics_router, prefix="/topics", tags=["kafka"])
