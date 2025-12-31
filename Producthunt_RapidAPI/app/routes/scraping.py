"""
Scraping API routes
"""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks

from ..models import ScrapeRequest, JobResponse, JobStatus
from ..background_tasks import create_job
from ..background_tasks import process_scrape_job

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Scraping"])


@router.post("/scrape", response_model=JobResponse)
async def create_scrape_job(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks
):
    """
    Create a new scraping job

   """
    try:
        job_id = create_job(request.dates, request.max_products_per_date)

        # Add background task
        background_tasks.add_task(process_scrape_job, job_id)

        return JobResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            created_at=datetime.now().isoformat(),
            message=f"Scraping job created for {len(request.dates)} dates"
        )
    except Exception as e:
        logger.error(f"Failed to create job: {e}")
        raise HTTPException(status_code=500, detail=str(e))
