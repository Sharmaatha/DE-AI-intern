"""
Background task processing and job management for ProductHunt scraper
"""

import logging
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from .api_client import ProductHuntAPIClient
from .config import API_KEY
from .models import JobStatus

logger = logging.getLogger(__name__)

# Initialize global instances
ph_client = ProductHuntAPIClient(API_KEY)

# Global job storage
jobs: Dict[str, dict] = {}
results_dir = Path("results")
results_dir.mkdir(exist_ok=True)


def create_job(dates: list, max_products: int) -> str:
    """Create a new scraping job"""
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        'job_id': job_id,
        'status': JobStatus.PENDING,
        'dates': dates,
        'max_products_per_date': max_products,
        'total_dates': len(dates),
        'processed_dates': 0,
        'total_products': 0,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat(),
        'results': {},
        'error': None
    }
    logger.info(f"Created job {job_id} for {len(dates)} dates")
    return job_id


def get_job(job_id: str) -> Optional[dict]:
    """Get job details"""
    return jobs.get(job_id)


def update_job(job_id: str, **kwargs):
    """Update job details"""
    if job_id in jobs:
        jobs[job_id].update(kwargs)
        jobs[job_id]['updated_at'] = datetime.now().isoformat()


def save_results(job_id: str):
    """Save job results to file"""
    job = jobs.get(job_id)
    if not job:
        return None

    filepath = results_dir / f"{job_id}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({
            'job_id': job_id,
            'summary': {
                'total_dates': job['total_dates'],
                'total_products': job['total_products'],
                'dates': job['dates'],
                'created_at': job['created_at'],
                'completed_at': job['updated_at']
            },
            'results': job['results']
        }, f, indent=2, ensure_ascii=False)

    return filepath


async def process_scrape_job(job_id: str):
    """Background task to process scraping job"""
    job = get_job(job_id)
    if not job:
        return

    try:
        update_job(job_id, status=JobStatus.PROCESSING)
        logger.info(f"Starting job {job_id}")

        dates = job['dates']
        max_products = job['max_products_per_date']

        for i, date_str in enumerate(dates):
            logger.info(f"Processing date {i+1}/{len(dates)}: {date_str}")

            try:
                products = await ph_client.get_all_launches_by_date(date_str, max_products)

                job['results'][date_str] = {
                    'date': date_str,
                    'total_products': len(products),
                    'products': products
                }

                update_job(
                    job_id,
                    processed_dates=i + 1,
                    total_products=job['total_products'] + len(products),
                    results=job['results']
                )

                logger.info(f"Date {date_str}: Fetched {len(products)} products")

                # Delay between dates
                if i < len(dates) - 1:
                    import asyncio
                    await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"Error processing {date_str}: {e}")
                job['results'][date_str] = {
                    'date': date_str,
                    'error': str(e),
                    'total_products': 0,
                    'products': []
                }

        # Save results and mark as completed
        filepath = save_results(job_id)
        update_job(job_id, status=JobStatus.COMPLETED)
        logger.info(f"Job {job_id} completed. Results saved to {filepath}")

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        update_job(
            job_id,
            status=JobStatus.FAILED,
            error=str(e)
        )
