"""
Celery tasks for Product Hunt scraping, CELERY WORKERS
"""
from celery_app import celery_app
from datetime import datetime, timedelta
from scrape_ph import scrape_producthunt_only
from enrich_social import enrich_social_links
from analyze_signals import analyze_signals
import traceback


@celery_app.task(name="tasks.scrape_task", bind=True)
def scrape_task(self, date_str: str, limit: int = 3):
    """
    STEP 1: Scrape Product Hunt
    """
    try:
        print(f"[CELERY TASK] Starting scrape for date: {date_str}, limit: {limit}")
        product_ids = scrape_producthunt_only(date_str, limit=limit)
        print(f"[CELERY TASK] Scrape completed. Product IDs: {product_ids}")
        return {
            "status": "success",
            "product_ids": product_ids,
            "count": len(product_ids),
            "date": date_str
        }
    except Exception as e:
        print(f"[CELERY TASK] Scrape failed: {e}")
        traceback.print_exc()
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise


@celery_app.task(name="tasks.enrich_task", bind=True)
def enrich_task(self, product_ids=None, limit=None):
    """
    STEP 2: Enrich social links
    """
    try:
        print(f"[CELERY TASK] Starting social enrichment. Product IDs: {product_ids}, Limit: {limit}")
        enrich_social_links(product_ids=product_ids, limit=limit)
        print(f"[CELERY TASK] Social enrichment completed")
        return {
            "status": "success",
            "message": "Social enrichment completed"
        }
    except Exception as e:
        print(f"[CELERY TASK] Enrichment failed: {e}")
        traceback.print_exc()
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise


@celery_app.task(name="tasks.analyze_task", bind=True)
def analyze_task(self, product_ids=None, limit=None):
    """
    STEP 3: Analyze signals with LLM
    """
    try:
        print(f"[CELERY TASK] Starting signal analysis. Product IDs: {product_ids}, Limit: {limit}")
        analyze_signals(product_ids=product_ids, limit=limit)
        print(f"[CELERY TASK] Signal analysis completed")
        return {
            "status": "success",
            "message": "Signal analysis completed"
        }
    except Exception as e:
        print(f"[CELERY TASK] Analysis failed: {e}")
        traceback.print_exc()
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise


@celery_app.task(name="tasks.full_pipeline_task", bind=True)
def full_pipeline_task(self, date_str: str, limit: int = 10):
    """
    Run all 3 steps in sequence
    """
    try:
        print(f"[CELERY TASK] Starting full pipeline for date: {date_str}")
        
        # Step 1: Scrape
        print("[CELERY TASK] Step 1/3: Scraping...")
        product_ids = scrape_producthunt_only(date_str, limit=limit)
        
        if not product_ids:
            print("[CELERY TASK] No products found, aborting pipeline")
            return {
                "status": "success",
                "steps_completed": [1],
                "message": "No products found",
                "product_ids": []
            }
        
        # Step 2: Enrich
        print(f"[CELERY TASK] Step 2/3: Enriching {len(product_ids)} products...")
        enrich_social_links(product_ids=product_ids)
        
        # Step 3: Analyze
        print(f"[CELERY TASK] Step 3/3: Analyzing {len(product_ids)} products...")
        analyze_signals(product_ids=product_ids)
        
        print("[CELERY TASK] Full pipeline completed!")
        return {
            "status": "success",
            "steps_completed": [1, 2, 3],
            "message": "Full pipeline completed",
            "product_ids": product_ids,
            "products_processed": len(product_ids),
            "date": date_str
        }
    except Exception as e:
        print(f"[CELERY TASK] Pipeline failed: {e}")
        traceback.print_exc()
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise


@celery_app.task(name="tasks.auto_scrape_yesterday")
def auto_scrape_yesterday():
    """
    Automatic scraping task - runs every 6 hours for yesterday's date
    """
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"[CELERY AUTO-TASK] Starting automatic scrape for {yesterday}")
        
        product_ids = scrape_producthunt_only(yesterday, limit=3)
        
        print(f"[CELERY AUTO-TASK] Auto scrape completed for {yesterday}. Products: {len(product_ids)}")
        return {
            "status": "success",
            "date": yesterday,
            "product_ids": product_ids,
            "count": len(product_ids)
        }
    except Exception as e:
        print(f"[CELERY AUTO-TASK] Auto scrape failed: {e}")
        traceback.print_exc()
        raise


@celery_app.task(name="tasks.auto_enrich_task")
def auto_enrich_task():
    """
    Automatic enrichment task - runs every 6 hours (10 minutes after scrape)
    """
    try:
        print(f"[CELERY AUTO-TASK] Starting automatic social enrichment")
        
        enrich_social_links(limit=10)
        
        print(f"[CELERY AUTO-TASK] Auto enrichment completed")
        return {
            "status": "success",
            "message": "Automatic social enrichment completed"
        }
    except Exception as e:
        print(f"[CELERY AUTO-TASK] Auto enrichment failed: {e}")
        traceback.print_exc()
        raise


@celery_app.task(name="tasks.auto_analyze_task")
def auto_analyze_task():
    """
    Automatic analysis task - runs every 6 hours (20 minutes after scrape)

    """
    try:
        print(f"[CELERY AUTO-TASK] Starting automatic signal analysis")
        
        analyze_signals(limit=10)
        
        print(f"[CELERY AUTO-TASK] Auto analysis completed")
        return {
            "status": "success",
            "message": "Automatic signal analysis completed"
        }
    except Exception as e:
        print(f"[CELERY AUTO-TASK] Auto analysis failed: {e}")
        traceback.print_exc()
        raise