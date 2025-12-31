"""
Health check routes
"""

from fastapi import APIRouter
from ..config import APP_TITLE, APP_VERSION

router = APIRouter(tags=["Health"])


@router.get("/")
async def root():
    """API health check"""
    return {
        "status": "healthy",
        "service": f"{APP_TITLE}",
        "version": APP_VERSION,
        "docs": "/docs"
    }
