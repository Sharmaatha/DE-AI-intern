"""
Celery application configuration
"""
from celery import Celery
from celery.schedules import crontab
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create Celery app
celery_app = Celery(
    "product_hunt_scraper",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["tasks"] 
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,  
    task_track_started=True,
    task_time_limit=30 * 60,  
)

# Celery Beat schedule - Multiple cron jobs (6 hour cycles)

celery_app.conf.beat_schedule = {

    # STEP 1: Scrape every 6 hours
    "auto-scrape-every-6-hours": {
        "task": "tasks.auto_scrape_yesterday",
        "schedule": crontab(hour="*/6", minute=0),   # 00:00, 06:00, 12:00, 18:00 UTC
    },

    # STEP 2: Enrich 10 minutes after scrape
    "auto-enrich-10-min-after": {
        "task": "tasks.auto_enrich_task",
        "schedule": crontab(hour="*/6", minute=10),  # 00:10, 06:10, 12:10, 18:10 UTC
    },

    # STEP 3: Analyze 20 minutes after scrape
    "auto-analyze-20-min-after": {
        "task": "tasks.auto_analyze_task",
        "schedule": crontab(hour="*/6", minute=20),  # 00:20, 06:20, 12:20, 18:20 UTC
    },
}


if __name__ == "__main__":
    celery_app.start()