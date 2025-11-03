# scheduler.py
import os
from sched import scheduler
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.batch_scraper import BatchProfileScraper
from src.db.db_functions import create_database_tables

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_daily_batch_job():
    logger.info("="*60)
    logger.info("Starting scheduled daily batch scraping job")
    logger.info(f"Current time: {datetime.now()}")
    logger.info("="*60)
    
    try:
        create_database_tables()
        
        scraper = BatchProfileScraper(
            min_delay=1,  
            max_delay=5,
            scrape_days=6, 
            created_by="daily_scheduler"
        )
        
        stats = scraper.run_daily_batch()
        
        logger.info("="*60)
        logger.info("Batch scraping completed successfully!")
        logger.info(f"Total active handles: {stats.get('total_active_handles', 0)}")
        logger.info(f"Daily quota: {stats.get('daily_quota', 0)}")
        logger.info(f"Successfully scraped: {stats.get('successful', 0)}")
        logger.info(f"Failed: {stats.get('failed', 0)}")
        
        if stats.get('errors'):
            logger.warning(f"Errors encountered: {len(stats['errors'])}")
            for error in stats['errors'][:5]: 
                logger.error(f"  - {error['handle']}: {error['error']}")
        
        logger.info("="*60)
        
        return stats
        
    except Exception as e:
        logger.error(f"Fatal error in batch scraping job: {str(e)}", exc_info=True)
        raise


def setup_scheduler():
    """
    Setup APScheduler to run the batch job
    Default: Runs Monday-Saturday at 2:00 AM
    """
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_daily_batch_job,
        trigger=CronTrigger(
            hour=2,        
            minute=0,         
            day_of_week='mon-sat'  
        ),
        id='daily_batch_scraper',
        name='Daily Profile Batch Scraper',
        replace_existing=True
    )
    
    logger.info("Scheduler configured:")
    logger.info("  - Job: Daily Profile Batch Scraper")
    logger.info("  - Schedule: Monday-Saturday at 2:00 AM")
    logger.info("  - Rate limit: 1-5 seconds between requests")
    return scheduler
def process_pending_and_scrape():
    """Auto-process new groups and scrape"""
    from src.db.db_functions import SessionLocal, process_all_pending_groups
    
    db = SessionLocal()
    try:
        result = process_all_pending_groups(db)
        logger.info(f"Processed {result.get('processed', 0)} pending groups")
        logger.info(f"Failed to process: {result.get('failed', 0)}")
        run_daily_batch_job()
    finally:
        db.close()
        
scheduler.add_job(
    process_pending_and_scrape,
    trigger=CronTrigger(hour=2, minute=0, day_of_week='mon-sat'),
    id='auto_process_and_scrape',
    name='Auto Process Groups & Scrape'
)

def main():
    """
    Main entry point for the scheduler
    """
    print("\n" + "="*60)
    print("Twitter Batch Scraper - Scheduler")
    print("="*60)
    print("\nInitializing scheduler...")
    print("This will run batch scraping Monday-Saturday at 2:00 AM")
    print("\nPress Ctrl+C to stop the scheduler")
    print("="*60 + "\n")
    
    try:
        scheduler = setup_scheduler()
        logger.info("Starting scheduler... (Press Ctrl+C to exit)")
        scheduler.start()
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

