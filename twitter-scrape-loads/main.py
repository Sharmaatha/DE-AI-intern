# main.py
from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from src.schema import schemas
from pydantic import BaseModel
import json
import os
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
from src.utils.scraping_functions import get_profile, get_tweets, get_followers, get_following
from src.db.db_functions import (
    get_db,
    create_database_tables,
    get_or_create_activity,
    load_profile_data,
    load_tweets_data,
    load_followers_data,
    load_following_data,
    has_followers_data,
    has_following_data,
    get_active_profile_handles, 
    Activity, Profile, Tweet, Follower, Following
)
from src.utils.batch_scraper import (
    BatchScraper, 
    scrape_tweets_now, 
    scrape_followers_now, 
    scrape_following_now
)
from src.db.db_functions import (
    SalesNavLeads, 
    get_active_groups, 
    mark_group_completed,  
    sync_activities_from_group, 
    process_group_profiles,
    process_all_pending_groups 
)
from src.utils.batch_scraper import BatchProfileScraper, scrape_profiles_now 

app = FastAPI(
    title="Twitter Scraper API",
    description="An API to scrape Twitter data and store it in a database.",
    version="2.0.0",
)

class BatchScrapeRequest(BaseModel):
    """Request model for batch scraping"""
    limit: Optional[int] = None 
    min_delay: int = 0
    max_delay: int = 5
    created_by: str = "batch_api"

class BatchScrapeResponse(BaseModel):
    """Response model for batch scraping"""
    total_requested: int
    total_to_scrape: int
    total_scraped: int
    successful: int
    failed: int
    errors: List[Dict[str, Any]]
    started_at: datetime
    completed_at: Optional[datetime]
    daily_quota: Optional[int] = None
    total_active_handles: Optional[int] = None

@app.on_event("startup")
def on_startup():
    print("--- Initializing Database (with new schema) ---")
    create_database_tables()

@app.post(
    "/batch/scrape-profiles", 
    tags=["Batch Scraping"], 
    response_model=BatchScrapeResponse
)
def batch_scrape_profiles(
    req: BatchScrapeRequest,
    db: Session = Depends(get_db)
):
    print(f"Batch profile scraping requested")
    scraper = BatchProfileScraper(
        min_delay=req.min_delay,
        max_delay=req.max_delay,
        scrape_days=6,
        created_by=req.created_by
    )
    
    if req.limit:
        handles = get_active_profile_handles(db)
        stats = scraper.scrape_profile_batch(handles, limit=req.limit)
    else:
        stats = scraper.run_daily_batch()
    return BatchScrapeResponse(**stats)


@app.post(
    "/batch/scrape-profiles-background",
    tags=["Batch Scraping"]
)
def batch_scrape_profiles_background(
    background_tasks: BackgroundTasks,
    req: BatchScrapeRequest,
    db: Session = Depends(get_db)
):

    def run_batch_scraping():
        scraper = BatchProfileScraper(
            min_delay=req.min_delay,
            max_delay=req.max_delay,
            scrape_days=6,
            created_by=req.created_by
        )
        
        if req.limit:
            from src.db.db_functions import SessionLocal
            db = SessionLocal()
            try:
                handles = get_active_profile_handles(db)
                scraper.scrape_profile_batch(handles, limit=req.limit)
            finally:
                db.close()
        else:
            scraper.run_daily_batch()
    
    background_tasks.add_task(run_batch_scraping)
    
    return {
        "message": "Batch scraping started in background",
        "status": "running",
        "limit": req.limit,
        "min_delay": req.min_delay,
        "max_delay": req.max_delay
    }

@app.post("/groups/process-all", tags=["Groups"])
def process_all_groups(db: Session = Depends(get_db)):
    print("API triggered: /groups/process-all")
    result = process_all_pending_groups(db)
    print("Completed processing groups")
    return result

@app.get(
    "/batch/active-handles",
    tags=["Batch Scraping"],
    response_model=Dict[str, Any]
)

def get_batch_active_handles(db: Session = Depends(get_db)):
    """
    Get all active profile handles that would be scraped in batch job
    Sorted by last_sync_on (NULL first, then oldest)
    """
    handles = get_active_profile_handles(db)
    
    # Calculate daily quota
    total_handles = len(handles)
    scrape_days = 6
    daily_quota = total_handles / scrape_days
    daily_quota_rounded = int(daily_quota) + (1 if daily_quota % 1 > 0 else 0)
    
    return {
        "total_active_handles": total_handles,
        "scrape_days": scrape_days,
        "daily_quota": daily_quota_rounded,
        "daily_quota_exact": daily_quota,
        "handles": handles[:20],
        "note": f"Showing first 20 of {total_handles} handles"
    }

@app.post("/scrape-all", tags=["Scraping"], response_model=List[schemas.ActivitySchema])
def scrape_all_for_handle(req: schemas.ScrapeTaskRequest, db: Session = Depends(get_db)):
    print(f"Starting ALL-IN-ONE scrape for: {req.handle}")
    
    completed_activities = []

    # 1. PROFILE
    try:
        activity = get_or_create_activity(db, handle=req.handle, query_type='get_profile', created_by=req.created_by, active=req.active)
        if activity.active:
            activity.status = 'in_progress'
            activity.updated_by = req.created_by
            
            profile_json = get_profile(req.handle)
            if profile_json:
                load_profile_data(db, profile_json, activity=activity, updated_by=req.created_by)
                activity.status = 'completed'
                activity.task_data = profile_json
            else:
                activity.status = 'failed'
            db.commit() 
            completed_activities.append(activity)
        else:
            db.commit() 
            completed_activities.append(activity)
            
    except Exception as e:
        print(f"Error scraping profile: {e}")
        db.rollback()

    # 2. TWEETS
    try:
        activity = get_or_create_activity(db, handle=req.handle, query_type='get_tweets', created_by=req.created_by, active=req.active)
        if activity.active:
            activity.status = 'in_progress'
            activity.updated_by = req.created_by
            
            tweets_json = get_tweets(req.handle)
            if tweets_json and tweets_json.get("timeline"):
                load_tweets_data(db, tweets_json, activity, limit=req.limit)
                activity.status = 'completed'
                activity.task_data = tweets_json
            else:
                activity.status = 'failed'
            db.commit()
            completed_activities.append(activity)
        else:
            db.commit()
            completed_activities.append(activity)
    except Exception as e:
        print(f"Error scraping tweets: {e}")
        db.rollback()

    # 3. FOLLOWERS
    try:
        activity = get_or_create_activity(db, handle=req.handle, query_type='get_followers', created_by=req.created_by, active=req.active)
        if activity.active:
            activity.status = 'in_progress'
            activity.updated_by = req.created_by
            
            followers_json = get_followers(req.handle)
            if followers_json and followers_json.get("followers"):
                load_followers_data(db, followers_json, activity, user=req.created_by, limit=req.limit)
                activity.status = 'completed'
                activity.task_data = followers_json
            else:
                activity.status = 'failed'
            db.commit()
            completed_activities.append(activity)
        else:
            db.commit()
            completed_activities.append(activity)
    except Exception as e:
        print(f"Error scraping followers: {e}")
        db.rollback()

    # 4. FOLLOWING
    try:
        activity = get_or_create_activity(db, handle=req.handle, query_type='get_following', created_by=req.created_by, active=req.active)
        if activity.active:
            activity.status = 'in_progress'
            activity.updated_by = req.created_by

            following_json = get_following(req.handle)
            if following_json and following_json.get("following"):
                load_following_data(db, following_json, activity, user=req.created_by, limit=req.limit)
                activity.status = 'completed'
                activity.task_data = following_json
            else:
                activity.status = 'failed'
            db.commit()
            completed_activities.append(activity)
        else:
            db.commit()
            completed_activities.append(activity)
    except Exception as e:
        print(f"Error scraping following: {e}")
        db.rollback()
        
    print(f"ALL-IN-ONE scrape finished for: {req.handle}")
    return completed_activities

@app.post("/scrape/profile", tags=["Scraping (Individual)"], response_model=schemas.ActivitySchema)
def scrape_profile(req: schemas.ScrapeTaskRequest, db: Session = Depends(get_db)):
    print(f"Received 'get_profile' task for: {req.handle}")
    
    try:
        activity = get_or_create_activity(
            db, 
            handle=req.handle, 
            query_type='get_profile', 
            created_by=req.created_by,
            active=req.active
        )

        if not activity.active:
            db.commit()
            return activity

        activity.status = 'in_progress'
        activity.updated_by = req.created_by
        
        profile_json = get_profile(req.handle)
        if profile_json:
            load_profile_data(db, profile_json, activity=activity, updated_by=req.created_by)
            activity.status = 'completed'
            activity.task_data = profile_json
        else:
            activity.status = 'failed'
            activity.task_data = {"error": "No data returned from API."}
        
        db.commit()
        return activity

    except Exception as e:
        db.rollback()
        print(f"Error scraping profile for {req.handle}: {e}")
        activity = db.query(Activity).filter_by(handle=req.handle, query_type='get_profile').first()
        if activity:
            activity.status = 'failed'
            activity.task_data = {"error": str(e)}
            db.commit()
            return activity
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.post("/scrape/tweets", tags=["Scraping (Individual)"], response_model=schemas.ActivitySchema)
def scrape_tweets(req: schemas.ScrapeTaskRequest, db: Session = Depends(get_db)):
    print(f"Received 'get_tweets' task for: {req.handle}")
    
    try:
        activity = get_or_create_activity(
            db, 
            handle=req.handle, 
            query_type='get_tweets', 
            created_by=req.created_by,
            active=req.active
        )
        
        if not activity.active:
            db.commit()
            return activity

        limit_to_use = req.limit if req.limit is not 0 else 200
        print(f"Setting tweet fetch limit to {limit_to_use}.")

        activity.status = 'in_progress'
        activity.updated_by = req.created_by

        all_tweets_list = []
        current_cursor = None
        last_good_response = None 

        while len(all_tweets_list) < limit_to_use:
            print(f"Looping: Fetched {len(all_tweets_list)}/{limit_to_use} tweets. Cursor: {current_cursor}")

            tweets_json = get_tweets(req.handle, cursor=current_cursor)
            
            if not tweets_json:
                print("API returned None or empty data mid-loop.")
                activity.status = 'failed'
                activity.task_data = {"error": "API returned no data during loop."}
                break 
            
            last_good_response = tweets_json

            new_tweets = tweets_json.get("timeline", [])
            if new_tweets:
                all_tweets_list.extend(new_tweets)
                print(f"Added {len(new_tweets)} new tweets. Total: {len(all_tweets_list)}")
            else:
                print("API returned 0 tweets in this page.")

            current_cursor = tweets_json.get("next_cursor")

            if not current_cursor or current_cursor == "0": 
                print("API reports no more tweets or no next cursor. Stopping loop.")
                break

        if all_tweets_list:
            print(f"Loop finished. Total tweets fetched: {len(all_tweets_list)}")
            data_to_load = {"timeline": all_tweets_list}

            load_tweets_data(
                db, 
                data_to_load,
                activity, 
                limit=limit_to_use
            )
            activity.status = 'completed'
            activity.task_data = last_good_response
        else:
            print("Loop finished, but all_tweets_list is empty.")
            if activity.status != 'failed':
                activity.status = 'failed'
                activity.task_data = {"error": "No tweets found after checking API."}

        db.commit()
        return activity

    except Exception as e:
        db.rollback()
        print(f"Error scraping tweets for {req.handle}: {e}")
        activity = db.query(Activity).filter_by(handle=req.handle, query_type='get_tweets').first()
        if activity:
            activity.status = 'failed'
            activity.task_data = {"error": str(e)}
            db.commit()
            return activity
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.post("/scrape/followers", tags=["Scraping (Individual)"], response_model=schemas.ActivitySchema)
def scrape_followers(req: schemas.ScrapeTaskRequest, db: Session = Depends(get_db)):
    print(f"Received 'get_followers' task for: {req.handle}")
    
    try:
        activity = get_or_create_activity(
            db, 
            handle=req.handle, 
            query_type='get_followers', 
            created_by=req.created_by,
            active=req.active
        )
        
        if not activity.active:
            db.commit()
            return activity

        limit_to_use = req.limit
        if limit_to_use == 0:
            if has_followers_data(db, req.handle):
                limit_to_use = 20
                print(f"Existing followers found. Setting limit to {limit_to_use}.")
            else:
                limit_to_use = 50
                print(f"No existing followers. Setting limit to {limit_to_use}.")

        activity.status = 'in_progress'
        activity.updated_by = req.created_by
        all_followers_list = []
        current_cursor = None
        last_good_response = None 
        while len(all_followers_list) < limit_to_use:
            print(f"Looping: Fetched {len(all_followers_list)}/{limit_to_use} followers. Cursor: {current_cursor}")
            
            followers_json = get_followers(req.handle, cursor=current_cursor)
            
            if not followers_json:
                print("API returned None or empty data mid-loop.")
                activity.status = 'failed'
                activity.task_data = {"error": "API returned no data during loop."}
                break
            
            last_good_response = followers_json

            new_followers = followers_json.get("followers", [])
            if new_followers:
                all_followers_list.extend(new_followers)
                print(f"Added {len(new_followers)} new followers. Total: {len(all_followers_list)}")
            else:
                print("API returned 0 followers in this page.")

            current_cursor = followers_json.get("next_cursor")

            if not followers_json.get("more_users", False) or not current_cursor:
                print("API reports no more users or no next cursor. Stopping loop.")
                break

        if all_followers_list:
            print(f"Loop finished. Total followers fetched: {len(all_followers_list)}")
            data_to_load = {"followers": all_followers_list} 
            
            load_followers_data(
                db, 
                data_to_load,
                activity, 
                user=req.created_by, 
                limit=limit_to_use
            )
            activity.status = 'completed'
            activity.task_data = last_good_response
        else:
            print("Loop finished, but all_followers_list is empty.")
            if activity.status != 'failed':
                activity.status = 'failed'
                activity.task_data = {"error": "No followers found after checking API."}

        db.commit()
        return activity

    except Exception as e:
        db.rollback()
        print(f"Error scraping followers for {req.handle}: {e}")
        activity = db.query(Activity).filter_by(handle=req.handle, query_type='get_followers').first()
        if activity:
            activity.status = 'failed'
            activity.task_data = {"error": str(e)}
            db.commit()
            return activity
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.post("/scrape/following", tags=["Scraping (Individual)"], response_model=schemas.ActivitySchema)
def scrape_following(req: schemas.ScrapeTaskRequest, db: Session = Depends(get_db)):
    print(f"Received 'get_following' task for: {req.handle}")
    
    try:
        activity = get_or_create_activity(
            db, 
            handle=req.handle, 
            query_type='get_following', 
            created_by=req.created_by,
            active=req.active
        )
        
        if not activity.active:
            db.commit()
            return activity

        use_limit = req.limit
        if use_limit == 0:
            if has_following_data(db, req.handle):
                use_limit = 20
            else:
                use_limit = 50

        activity.status = 'in_progress'
        activity.updated_by = req.created_by
        all_following_list = []
        current_cursor = None
        last_good_response = None

        while len(all_following_list) < use_limit:
            print(f"Fetching following for {req.handle}, cursor: {current_cursor}")
            following_json = get_following(req.handle, cursor=current_cursor)
            
            if not following_json:
                print(f"[ERROR] API returned no data for {req.handle}.")
                activity.status = 'failed'
                activity.task_data = {"error": "API returned no data during loop."}
                break

            last_good_response = following_json
            new_following = following_json.get("following") or following_json.get("users", [])

            if not new_following:
                print(f"[WARN] No 'following' or 'users' key found. Response keys: {list(following_json.keys())}")
                activity.status = 'failed'
                activity.task_data = {"error": "No following/users data in API response."}
                break

            all_following_list.extend(new_following)
            current_cursor = following_json.get("next_cursor")

            if not following_json.get("more_users", False) or not current_cursor:
                print(f"API indicates no more following for {req.handle}.")
                break

        if all_following_list:
            data_to_load = {"following": all_following_list}
            
            load_following_data(
                db, 
                data_to_load,
                activity, 
                user=req.created_by, 
                limit=use_limit
            )
            activity.status = 'completed'
            activity.task_data = last_good_response
        else:
            if activity.status != 'failed':
                activity.status = 'failed'
                activity.task_data = {"error": "No following found after checking API."}

        db.commit()
        return activity

    except Exception as e:
        db.rollback()
        print(f"Error scraping following for {req.handle}: {e}")
        activity = db.query(Activity).filter_by(handle=req.handle, query_type='get_following').first()
        if activity:
            activity.status = 'failed'
            activity.task_data = {"error": str(e)}
            db.commit()
            return activity
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/status", tags=["Monitoring"])
def get_database_status(db: Session = Depends(get_db)):
    try:
        group_count = db.query(SalesNavLeads).filter_by(
            project_type="twitter-profiles",
            source_from=2
        ).count()
        pending_groups = db.query(SalesNavLeads).filter_by(
            project_type="twitter-profiles",
            source_from=2,
            status="pending"
        ).count()
        
        return {
            "ok": True,
            "profiles": db.query(Profile).count(),
            "activities": db.query(Activity).count(),
            "tweets": db.query(Tweet).count(),
            "followers": db.query(Follower).count(),
            "following": db.query(Following).count(),
            "total_groups": group_count, 
            "pending_groups": pending_groups  
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/activities/{handle}", response_model=List[schemas.ActivitySchema], tags=["Data Retrieval"])
def get_activities_for_handle(handle: str, db: Session = Depends(get_db)):
    return db.query(Activity).filter(Activity.handle == handle).all()

@app.get("/profiles/{handle}", response_model=schemas.ProfileSchema, tags=["Data Retrieval"])
def get_profile_from_db(handle: str, db: Session = Depends(get_db)):
    profile = db.query(Profile).filter(Profile.handle == handle).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found for this handle.")
    return profile

@app.get("/tweets/{handle}", response_model=List[schemas.TweetSchema], tags=["Data Retrieval"])
def get_tweets_from_db(handle: str, db: Session = Depends(get_db)):
    tweets = db.query(Tweet).filter(Tweet.handle == handle).all()
    if not tweets:
        raise HTTPException(status_code=404, detail="No tweets found for this handle.")
    return tweets

@app.get("/followers/{handle}", response_model=List[schemas.FollowerSchema], tags=["Data Retrieval"])
def get_followers_from_db(handle: str, db: Session = Depends(get_db)):
    followers = db.query(Follower).filter(Follower.scraped_from_handle == handle).all()
    if not followers:
        raise HTTPException(status_code=404, detail="No followers found for this handle.")
    return followers

@app.get("/following/{handle}", response_model=List[schemas.FollowingSchema], tags=["Data Retrieval"])
def get_following_from_db(handle: str, db: Session = Depends(get_db)):
    following = db.query(Following).filter(Following.scraped_from_handle == handle).all()
    if not following:
        raise HTTPException(status_code=404, detail="No 'following' data found for this handle.")
    return following

# BATCH ENDPOINTS Tweets
@app.post("/batch/scrape-tweets", tags=["Batch Scraping"], response_model=BatchScrapeResponse)
def batch_scrape_tweets(req: BatchScrapeRequest, db: Session = Depends(get_db)):
    print(f"Batch tweets scraping requested")
    limit_per_handle = req.limit if req.limit is not None and req.limit > 0 else 200
    
    scraper = BatchScraper(
        query_type='get_tweets',
        min_delay=req.min_delay,
        max_delay=req.max_delay,
        scrape_days=6,
        created_by=req.created_by,
        limit_per_handle=limit_per_handle 
    )
    
    if req.limit:
        from src.db.db_functions import get_active_handles_by_type
        handles = get_active_handles_by_type(db, 'get_tweets')
        stats = scraper.scrape_batch(handles, limit=req.limit)
    else:
        stats = scraper.run_daily_batch()
    return BatchScrapeResponse(**stats)


@app.post("/batch/scrape-tweets-background", tags=["Batch Scraping"])
def batch_scrape_tweets_background(background_tasks: BackgroundTasks, req: BatchScrapeRequest, db: Session = Depends(get_db)):
    def run_batch_scraping():
        limit_per_handle = req.limit if req.limit is not None and req.limit > 0 else 200
        
        scraper = BatchScraper(
            query_type='get_tweets',
            min_delay=req.min_delay,
            max_delay=req.max_delay,
            scrape_days=6,
            created_by=req.created_by,
            limit_per_handle=limit_per_handle 
        )
        
        if req.limit:
            from src.db.db_functions import SessionLocal, get_active_handles_by_type
            db = SessionLocal()
            try:
                handles = get_active_handles_by_type(db, 'get_tweets')
                scraper.scrape_batch(handles, limit=req.limit)
            finally:
                db.close()
        else:
            scraper.run_daily_batch()
    
    background_tasks.add_task(run_batch_scraping)
    return {"message": "Batch tweets scraping started in background", "status": "running"}

# BATCH ENDPOINTS FOLLOWERS
@app.post("/batch/scrape-followers", tags=["Batch Scraping"], response_model=BatchScrapeResponse)
def batch_scrape_followers(req: BatchScrapeRequest, db: Session = Depends(get_db)):
    print(f"Batch followers scraping requested")
    limit_per_handle = req.limit if req.limit is not None and req.limit > 0 else 50
    
    scraper = BatchScraper(
        query_type='get_followers',
        min_delay=req.min_delay,
        max_delay=req.max_delay,
        scrape_days=6,
        created_by=req.created_by,
        limit_per_handle=limit_per_handle
    )
    
    if req.limit:
        from src.db.db_functions import get_active_handles_by_type
        handles = get_active_handles_by_type(db, 'get_followers')
        stats = scraper.scrape_batch(handles, limit=req.limit)
    else:
        stats = scraper.run_daily_batch()
    return BatchScrapeResponse(**stats)


@app.post("/batch/scrape-followers-background", tags=["Batch Scraping"])
def batch_scrape_followers_background(background_tasks: BackgroundTasks, req: BatchScrapeRequest, db: Session = Depends(get_db)):
    def run_batch_scraping():
        limit_per_handle = req.limit if req.limit is not None and req.limit > 0 else 50
        
        scraper = BatchScraper(
            query_type='get_followers',
            min_delay=req.min_delay,
            max_delay=req.max_delay,
            scrape_days=6,
            created_by=req.created_by,
            limit_per_handle=limit_per_handle
        )
        
        if req.limit:
            from src.db.db_functions import SessionLocal, get_active_handles_by_type
            db = SessionLocal()
            try:
                handles = get_active_handles_by_type(db, 'get_followers')
                scraper.scrape_batch(handles, limit=req.limit)
            finally:
                db.close()
        else:
            scraper.run_daily_batch()
    
    background_tasks.add_task(run_batch_scraping)
    return {"message": "Batch followers scraping started in background", "status": "running"}


# BATCH ENDPOINTS FOLLOWING
@app.post("/batch/scrape-following", tags=["Batch Scraping"], response_model=BatchScrapeResponse)
def batch_scrape_following(req: BatchScrapeRequest, db: Session = Depends(get_db)):
    print(f"Batch following scraping requested")
    
    limit_per_handle = req.limit if req.limit is not None and req.limit > 0 else 50
    
    scraper = BatchScraper(
        query_type='get_following',
        min_delay=req.min_delay,
        max_delay=req.max_delay,
        scrape_days=6,
        created_by=req.created_by,
        limit_per_handle=limit_per_handle
    )
    
    if req.limit:
        from src.db.db_functions import get_active_handles_by_type
        handles = get_active_handles_by_type(db, 'get_following')
        stats = scraper.scrape_batch(handles, limit=req.limit)
    else:
        stats = scraper.run_daily_batch()
    return BatchScrapeResponse(**stats)


@app.post("/batch/scrape-following-background", tags=["Batch Scraping"])
def batch_scrape_following_background(background_tasks: BackgroundTasks, req: BatchScrapeRequest, db: Session = Depends(get_db)):
    def run_batch_scraping():
        limit_per_handle = req.limit if req.limit is not None and req.limit > 0 else 50
        
        scraper = BatchScraper(
            query_type='get_following',
            min_delay=req.min_delay,
            max_delay=req.max_delay,
            scrape_days=6,
            created_by=req.created_by,
            limit_per_handle=limit_per_handle
        )
        
        if req.limit:
            from src.db.db_functions import SessionLocal, get_active_handles_by_type
            db = SessionLocal()
            try:
                handles = get_active_handles_by_type(db, 'get_following')
                scraper.scrape_batch(handles, limit=req.limit)
            finally:
                db.close()
        else:
            scraper.run_daily_batch()
    
    background_tasks.add_task(run_batch_scraping)
    return {"message": "Batch following scraping started in background", "status": "running"}

@app.get("/batch/active-handles/{query_type}", tags=["Batch Scraping"], response_model=Dict[str, Any])
def get_batch_active_handles_by_type(query_type: str, db: Session = Depends(get_db)):
    """
    Get all active handles for specified query type
    Valid query_types: get_profile, get_tweets, get_followers, get_following
    """
    from src.db.db_functions import get_active_handles_by_type
    
    valid_types = ['get_profile', 'get_tweets', 'get_followers', 'get_following']
    if query_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid query_type. Must be one of: {valid_types}")
    
    handles = get_active_handles_by_type(db, query_type)
    
    total_handles = len(handles)
    scrape_days = 6
    daily_quota = total_handles / scrape_days
    daily_quota_rounded = int(daily_quota) + (1 if daily_quota % 1 > 0 else 0)
    
    return {
        "query_type": query_type,
        "total_active_handles": total_handles,
        "scrape_days": scrape_days,
        "daily_quota": daily_quota_rounded,
        "daily_quota_exact": daily_quota,
        "handles": handles[:20],
        "note": f"Showing first 20 of {total_handles} handles"
    }


@app.post("/linkedin/{handle}", tags=["LinkedIn Data"])
def store_linkedin_profile(
    handle: str,
    linkedin_data: Dict[str, Any],
    created_by: str = "system",
    db: Session = Depends(get_db)
):
    """Store LinkedIn data for a Twitter profile"""
    try:
        from src.db.db_functions import store_linkedin_data
        
        profile = store_linkedin_data(db, handle, linkedin_data, updated_by=created_by)
        db.commit()
        
        return {
            "message": "LinkedIn data stored successfully",
            "handle": handle,
            "has_linkedin_data": True
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error storing LinkedIn data: {str(e)}")
    
@app.post("/groups/create", tags=["Groups"])
def create_group(req: schemas.CreateGroupRequest, db: Session = Depends(get_db)):
    """Create a new group with Twitter handlers"""
    try:
        valid_types = ["twitter-profiles", "twitter-followers", "twitter-following"]
        if req.project_type not in valid_types:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid project_type. Must be one of: {valid_types}"
            )
        
        new_group = SalesNavLeads(
            name=req.name,
            description=req.description,
            project_type=req.project_type, 
            source_from=2,
            status="pending",
            active=True,
            meta_data={"twitter_handlers": req.twitter_handlers},
            created_by=int(req.created_by) if req.created_by.isdigit() else None
        )
        
        db.add(new_group)
        db.commit()
        db.refresh(new_group)
        
        return {
            "ok": True,
            "group_id": new_group.id,
            "name": new_group.name,
            "project_type": new_group.project_type,
            "handlers": req.twitter_handlers,
            "message": f"Group '{new_group.name}' created successfully"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/groups/list", tags=["Groups"])
def list_groups(status: str = None, db: Session = Depends(get_db)):
    """List all groups, optionally filtered by status."""
    try:
        query = db.query(SalesNavLeads).filter_by(
            project_type="twitter-profiles",
            source_from=2
        )
        
        if status:
            query = query.filter_by(status=status)
        
        groups = query.all()
        
        groups_data = []
        for group in groups:
            groups_data.append({
                "id": group.id,
                "name": group.name,
                "description": group.description,
                "status": group.status,
                "active": group.active,
                "handlers": group.meta_data.get('twitter_handlers', []) if group.meta_data else [], 
                "created_at": group.created_at.isoformat() if group.created_at else None,
                "last_sync_on": group.last_sync_on.isoformat() if group.last_sync_on else None
            })
        
        return {
            "ok": True,
            "total": len(groups_data),
            "groups": groups_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing groups: {str(e)}")

@app.post("/groups/{group_id}/mark-completed", tags=["Groups"])
def mark_group_as_completed(group_id: int, db: Session = Depends(get_db)):
    """Mark a group as completed."""
    try:
        mark_group_completed(db, group_id)
        db.commit()
        return {"ok": True, "message": f"Group {group_id} marked as completed"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error marking group: {str(e)}")