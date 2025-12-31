"""
Job management routes
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from ..models import JobStatusResponse, JobStatus
from ..background_tasks import get_job, update_job, save_results, jobs, results_dir

router = APIRouter(tags=["Jobs"])


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status of a scraping job
    """
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    progress = int((job['processed_dates'] / job['total_dates']) * 100) if job['total_dates'] > 0 else 0

    return JobStatusResponse(
        job_id=job['job_id'],
        status=job['status'],
        progress=progress,
        total_dates=job['total_dates'],
        processed_dates=job['processed_dates'],
        total_products=job['total_products'],
        created_at=job['created_at'],
        updated_at=job['updated_at'],
        error=job.get('error')
    )


@router.get("/jobs/{job_id}/results")
async def get_job_results(job_id: str):
    """
    Get the results of a completed job
    """
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job['status'] != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed yet. Current status: {job['status']}"
        )

    # Return top 5 products per date for preview
    preview_results = {}
    for date_str, data in job['results'].items():
        products = data.get('products', [])[:5]
        preview_results[date_str] = {
            'date': date_str,
            'total_products': data.get('total_products', 0),
            'top_products': [
                {
                    'name': p.get('name'),
                    'daily_rank': p.get('daily_rank'),
                    'votes_count': p.get('votes_count'),
                    'domain': p.get('domain')
                } for p in products
            ]
        }

    return {
        'job_id': job_id,
        'summary': {
            'total_dates': job['total_dates'],
            'total_products': job['total_products'],
            'completed_at': job['updated_at']
        },
        'results': preview_results,
        'download_url': f"/api/v1/jobs/{job_id}/download"
    }


@router.get("/jobs/{job_id}/download")
async def download_results(job_id: str):
    """
    Download complete results as JSON file
    """
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job['status'] != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed yet. Current status: {job['status']}"
        )

    filepath = results_dir / f"{job_id}.json"
    if not filepath.exists():
        filepath = save_results(job_id)

    return FileResponse(
        filepath,
        media_type="application/json",
        filename=f"producthunt_results_{job_id}.json"
    )


@router.get("/jobs")
async def list_jobs(
    status: Optional[JobStatus] = Query(None, description="Filter by status"),
    limit: int = Query(10, ge=1, le=100, description="Number of jobs to return")
):
    """
    List all scraping jobs
    """
    jobs_list = list(jobs.values())

    # Filter by status if provided
    if status:
        jobs = [j for j in jobs if j['status'] == status]

    # Sort by created_at descending
    jobs.sort(key=lambda x: x['created_at'], reverse=True)

    # Limit results
    jobs = jobs[:limit]

    return {
        'total': len(jobs),
        'jobs': [
            {
                'job_id': j['job_id'],
                'status': j['status'],
                'total_dates': j['total_dates'],
                'processed_dates': j['processed_dates'],
                'total_products': j['total_products'],
                'created_at': j['created_at']
            } for j in jobs_list
        ]
    }
