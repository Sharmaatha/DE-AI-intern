"""
ProductHunt Scraper API - FastAPI Application
"""

import logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import (
    APP_TITLE, APP_DESCRIPTION, APP_VERSION,
    CORS_ORIGINS, CORS_CREDENTIALS, CORS_METHODS, CORS_HEADERS,
    LOG_LEVEL, LOG_FORMAT, LOG_FILE
)
from .routes.health import router as health_router
from .routes.scraping import router as scraping_router
from .routes.jobs import router as jobs_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_application() -> FastAPI:
    """Create and configure FastAPI application"""

    app = FastAPI(
        title=APP_TITLE,
        description=APP_DESCRIPTION,
        version=APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc"
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=CORS_CREDENTIALS,
        allow_methods=CORS_METHODS,
        allow_headers=CORS_HEADERS,
    )

    # Include routers
    app.include_router(health_router)
    app.include_router(scraping_router)
    app.include_router(jobs_router)

    return app
