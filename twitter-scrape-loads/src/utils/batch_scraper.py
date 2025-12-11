#batch_scraper.py
import time
import random
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from src.db.db_functions import (
    Activity,
    SessionLocal,
    get_active_handles_by_type,
    get_or_create_activity,
    load_profile_data,
    load_tweets_data,
    load_followers_data,
    load_following_data,
    update_activity_last_sync
)
from src.db.db_functions import (
    SessionLocal,
    get_or_create_activity,
    get_active_handles_by_type,
)
from src.utils.scraping_functions import get_profile, get_tweets, get_followers, get_following


class BatchScraper:
    """Universal batch scraper for all query types"""
    
    def __init__(
        self, 
        query_type: str, 
        min_delay: int = 0, 
        max_delay: int = 5,
        scrape_days: int = 6,
        created_by: str = "batch_scraper",
        limit_per_handle: Optional[int] = None  # For tweets/followers/following
    ):
        self.query_type = query_type
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.scrape_days = scrape_days
        self.created_by = created_by
        self.limit_per_handle = limit_per_handle
        
        self.scrape_function_map = {
            'get_profile': get_profile,
            'get_tweets': get_tweets,
            'get_followers': get_followers,
            'get_following': get_following
        }
        
        self.load_function_map = {
            'get_profile': self._load_profile,
            'get_tweets': self._load_tweets,
            'get_followers': self._load_followers,
            'get_following': self._load_following
        }
        
    def _apply_rate_limit(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        print(f"Rate limiting: waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def calculate_daily_quota(self, total_handles: int) -> int:
        daily_quota = total_handles / self.scrape_days
        return int(daily_quota) + (1 if daily_quota % 1 > 0 else 0)
    
    def _load_profile(self, db: Session, data: Dict, activity: Activity, handle: str):
        """Load profile data"""
        if data:
            load_profile_data(db, data, activity=activity, updated_by=self.created_by)
            return True
        return False
    
    def _load_tweets(self, db: Session, data: Dict, activity: Activity, handle: str):
        """Load tweets data with pagination"""
        all_tweets = []
        current_cursor = None
        limit = self.limit_per_handle or 200
        
        while len(all_tweets) < limit:
            tweets_json = get_tweets(handle, cursor=current_cursor)
            if not tweets_json:
                break
            
            new_tweets = tweets_json.get("timeline", [])
            if new_tweets:
                all_tweets.extend(new_tweets)
            
            current_cursor = tweets_json.get("next_cursor")
            if not current_cursor or current_cursor == "0":
                break
        
        if all_tweets:
            load_tweets_data(db, {"timeline": all_tweets}, activity, limit=limit)
            return True
        return False
    
    def _load_followers(self, db: Session, data: Dict, activity: Activity, handle: str):
        """Load followers data with pagination"""
        all_followers = []
        current_cursor = None
        limit = self.limit_per_handle or 50
        
        while len(all_followers) < limit:
            followers_json = get_followers(handle, cursor=current_cursor)
            if not followers_json:
                break
            
            new_followers = followers_json.get("followers", [])
            if new_followers:
                all_followers.extend(new_followers)
            
            current_cursor = followers_json.get("next_cursor")
            if not followers_json.get("more_users", False) or not current_cursor:
                break
        
        if all_followers:
            load_followers_data(db, {"followers": all_followers}, activity, user=self.created_by, limit=limit)
            return True
        return False
    
    def _load_following(self, db: Session, data: Dict, activity: Activity, handle: str):
        """Load following data with pagination"""
        all_following = []
        current_cursor = None
        limit = self.limit_per_handle or 50
        
        while len(all_following) < limit:
            following_json = get_following(handle, cursor=current_cursor)
            if not following_json:
                break
            
            new_following = following_json.get("following", [])
            if new_following:
                all_following.extend(new_following)
            
            current_cursor = following_json.get("next_cursor")
            if not following_json.get("more_users", False) or not current_cursor:
                break
        
        if all_following:
            load_following_data(db, {"following": all_following}, activity, user=self.created_by, limit=limit)
            return True
        return False
    
    def scrape_batch(
        self, 
        handles: List[str], 
        limit: int = None
    ) -> Dict[str, Any]:
        """Scrape a batch of handles for the specified query type"""
        db = SessionLocal()
        
        stats = {
            "query_type": self.query_type,
            "total_requested": len(handles),
            "total_scraped": 0,
            "successful": 0,
            "failed": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
            "completed_at": None
        }
        
        try:
            handles_to_scrape = handles[:limit] if limit else handles
            stats["total_to_scrape"] = len(handles_to_scrape)
            
            print(f"\n{'='*60}")
            print(f"Starting batch {self.query_type} scraping")
            print(f"Total handles to scrape: {len(handles_to_scrape)}")
            print(f"Rate limit: {self.min_delay}-{self.max_delay} seconds between requests")
            if self.limit_per_handle:
                print(f"Limit per handle: {self.limit_per_handle}")
            print(f"{'='*60}\n")
            
            scrape_func = self.scrape_function_map[self.query_type]
            load_func = self.load_function_map[self.query_type]
            
            for idx, handle in enumerate(handles_to_scrape, 1):
                try:
                    print(f"\n[{idx}/{len(handles_to_scrape)}] Processing {self.query_type}: @{handle}")
                    if idx > 1:
                        self._apply_rate_limit()
                    
                    activity = get_or_create_activity(
                        db, 
                        handle=handle, 
                        query_type=self.query_type,
                        created_by=self.created_by,
                        active=True
                    )
            
                    activity.status = 'in_progress'
                    activity.updated_by = self.created_by
                    db.commit()
                    
                    if self.query_type == 'get_profile':
                        data = scrape_func(handle)
                        success = load_func(db, data, activity, handle)
                    else:
                        success = load_func(db, {}, activity, handle)
                    
                    if success:
                        activity.status = 'completed'
                        if self.query_type == 'get_profile':
                            activity.task_data = data
                        update_activity_last_sync(db, handle, self.query_type)
                        db.commit()
                        stats["successful"] += 1
                        print(f"✓ Successfully scraped {self.query_type} for @{handle}")
                    else:
                        activity.status = 'failed'
                        activity.task_data = {"error": "No data returned from API"}
                        db.commit()
                        stats["failed"] += 1
                        stats["errors"].append({
                            "handle": handle,
                            "error": "No data returned from API"
                        })
                        print(f"✗ Failed to scrape {self.query_type} for @{handle}: No data returned")
                    
                    stats["total_scraped"] += 1
                    
                except Exception as e:
                    db.rollback()
                    stats["failed"] += 1
                    stats["errors"].append({
                        "handle": handle,
                        "error": str(e)
                    })
                    print(f"✗ Error scraping {self.query_type} for @{handle}: {str(e)}")
                    
                    try:
                        activity = db.query(Activity).filter_by(
                            handle=handle, 
                            query_type=self.query_type
                        ).first()
                        if activity:
                            activity.status = 'failed'
                            activity.task_data = {"error": str(e)}
                            db.commit()
                    except:
                        pass
            
            stats["completed_at"] = datetime.now(timezone.utc)
            duration = (stats["completed_at"] - stats["started_at"]).total_seconds()
            
            print(f"\n{'='*60}")
            print(f"Batch {self.query_type} scraping completed!")
            print(f"Statistics:")
            print(f"  Total scraped: {stats['total_scraped']}")
            print(f"  Successful: {stats['successful']}")
            print(f"  Failed: {stats['failed']}")
            print(f"  Duration: {duration:.2f} seconds")
            print(f"{'='*60}\n")
            
        finally:
            db.close()
        
        return stats
    
    def run_daily_batch(self) -> Dict[str, Any]:
        """Run daily batch job for this query type"""
        db = SessionLocal()
        
        try:
            all_handles = get_active_handles_by_type(db, self.query_type)
            
            if not all_handles:
                print(f"No active handles found for {self.query_type}.")
                return {
                    "query_type": self.query_type,
                    "total_handles": 0,
                    "daily_quota": 0,
                    "scraped": 0
                }
            
            daily_quota = self.calculate_daily_quota(len(all_handles))
            
            print(f"\n Daily Batch Scraping Job - {self.query_type}")
            print(f"Total active handles: {len(all_handles)}")
            print(f"Scrape days: {self.scrape_days}")
            print(f"Daily quota: {daily_quota}")
            
            return self.scrape_batch(all_handles, limit=daily_quota)
            
        finally:
            db.close()

class BatchProfileScraper(BatchScraper):
    def __init__(self, min_delay=0, max_delay=5, scrape_days=6, created_by="batch_scraper"):
        super().__init__(
            query_type='get_profile',
            min_delay=min_delay,
            max_delay=max_delay,
            scrape_days=scrape_days,
            created_by=created_by
        )
    
    def scrape_profile_batch(self, handles: List[str], limit: int = None):
        return self.scrape_batch(handles, limit)


def scrape_profiles_now(limit: int = None, min_delay: int = 0, max_delay: int = 5) -> Dict[str, Any]:
    scraper = BatchProfileScraper(min_delay=min_delay, max_delay=max_delay, created_by="manual_batch")
    if limit:
        db = SessionLocal()
        try:
            handles = get_active_handles_by_type(db, 'get_profile')
            return scraper.scrape_profile_batch(handles, limit=limit)
        finally:
            db.close()
    else:
        return scraper.run_daily_batch()


def scrape_tweets_now(limit: int = None, min_delay: int = 0, max_delay: int = 5, limit_per_handle: int = 200) -> Dict[str, Any]:
    scraper = BatchScraper(
        query_type='get_tweets',
        min_delay=min_delay,
        max_delay=max_delay,
        created_by="manual_batch",
        limit_per_handle=limit_per_handle
    )
    if limit:
        db = SessionLocal()
        try:
            handles = get_active_handles_by_type(db, 'get_tweets')
            return scraper.scrape_batch(handles, limit=limit)
        finally:
            db.close()
    else:
        return scraper.run_daily_batch()


def scrape_followers_now(limit: int = None, min_delay: int = 0, max_delay: int = 5, limit_per_handle: int = 50) -> Dict[str, Any]:
    scraper = BatchScraper(
        query_type='get_followers',
        min_delay=min_delay,
        max_delay=max_delay,
        created_by="manual_batch",
        limit_per_handle=limit_per_handle
    )
    if limit:
        db = SessionLocal()
        try:
            handles = get_active_handles_by_type(db, 'get_followers')
            return scraper.scrape_batch(handles, limit=limit)
        finally:
            db.close()
    else:
        return scraper.run_daily_batch()


def scrape_following_now(limit: int = None, min_delay: int = 0, max_delay: int = 5, limit_per_handle: int = 50) -> Dict[str, Any]:
    scraper = BatchScraper(
        query_type='get_following',
        min_delay=min_delay,
        max_delay=max_delay,
        created_by="manual_batch",
        limit_per_handle=limit_per_handle
    )
    if limit:
        db = SessionLocal()
        try:
            handles = get_active_handles_by_type(db, 'get_following')
            return scraper.scrape_batch(handles, limit=limit)
        finally:
            db.close()
    else:
        return scraper.run_daily_batch()


if __name__ == "__main__":
    print("Starting batch scraper...")
    print("\nAvailable batch operations:")
    print("1. Profile scraping")
    print("2. Tweets scraping")
    print("3. Followers scraping")
    print("4. Following scraping")
    
    
    # Example: Run profile batch
    results = scrape_profiles_now(limit=5)
    print(f"\nProfile scraping results: {results}")