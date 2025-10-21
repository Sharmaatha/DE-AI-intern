from fastapi import FastAPI, Depends, HTTPException
from typing import List
from sqlalchemy.orm import Session
import schemas
import json
import os
from scraping_functions import get_profile, get_tweets, get_followers, get_following

from db_functions import (
    get_db,
    create_database_tables,
    load_profile_data,
    load_tweets_data,
    load_followers_data,
    load_following_data,
    Activity, Profile, Tweet, Follower, Following
)

app = FastAPI(
    title="Twitter Scraper API",
    description="An API to scrape Twitter data and store it in a database.",
    version="1.0.0",
)

@app.on_event("startup")
def on_startup():
    print("--- Initializing Database ---")
    create_database_tables()

@app.post("/scrape/{handle}", tags=["Scraping"])
def scrape_and_load_handle(handle: str, db: Session = Depends(get_db)):
    print(f"Starting full process for: {handle}")
    
    profile_json = get_profile(handle)
    if not profile_json:
        raise HTTPException(status_code=404, detail=f"Profile for '{handle}' not found or API error.")
    load_profile_data(db, profile_json)

    tweets_json = get_tweets(handle)
    if tweets_json:
        load_tweets_data(db, tweets_json, scraped_from=handle)

    followers_json = get_followers(handle)
    if followers_json:
        load_followers_data(db, followers_json, scraped_from=handle, limit=5)

    following_json = get_following(handle)
    if following_json:
        load_following_data(db, following_json, scraped_from=handle, limit=5)

    return {"status": "success", "message": f"Successfully scraped and loaded data for {handle}."}


@app.get("/profiles/{handle}", response_model=schemas.ProfileSchema, tags=["Data Retrieval"])
def get_profile_from_db(handle: str, db: Session = Depends(get_db)):
    activity = db.query(Activity).filter(Activity.handle == handle).first()
    if not activity:
        raise HTTPException(status_code=404, detail="Handle not found in database.")
    
    profile = db.query(Profile).filter(Profile.id == activity.id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile data not found for this handle.")
    return profile

@app.get("/tweets/{handle}", response_model=List[schemas.TweetSchema], tags=["Data Retrieval"])
def get_tweets_from_db(handle: str, db: Session = Depends(get_db)):
    tweets = db.query(Tweet).filter(Tweet.handler == handle).all()
    if not tweets:
        raise HTTPException(status_code=404, detail="No tweets found for this handle.")
    return tweets

@app.get("/followers/{handle}", response_model=List[schemas.FollowerSchema], tags=["Data Retrieval"])
def get_followers_from_db(handle: str, db: Session = Depends(get_db)):
    followers = db.query(Follower).filter(Follower.scraped_from == handle).all()
    if not followers:
        raise HTTPException(status_code=404, detail="No followers found for this handle.")
    return followers

@app.get("/following/{handle}", response_model=List[schemas.FollowingSchema], tags=["Data Retrieval"])
def get_following_from_db(handle: str, db: Session = Depends(get_db)):
    following = db.query(Following).filter(Following.scraped_from == handle).all()
    if not following:
        raise HTTPException(status_code=404, detail="No 'following' data found for this handle.")
    return following

@app.post("/load/profile-from-file", tags=["File Loading"])
def load_profile_from_file(file_path: str, db: Session = Depends(get_db)):
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found at: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            profile_json = json.load(f)
        
        load_profile_data(db, profile_json)
        handle = profile_json.get('profile', 'unknown handle')
        return {"status": "success", "message": f"Successfully loaded profile for {handle} from {file_path}."}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format in the provided file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.post("/load/tweets-from-file", tags=["File Loading"])
def load_tweets_from_file(file_path: str, scraped_from: str, db: Session = Depends(get_db)):
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found at: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tweets_json = json.load(f)
        
        load_tweets_data(db, tweets_json, scraped_from=scraped_from)
        return {"status": "success", "message": f"Successfully loaded tweets for {scraped_from} from {file_path}."}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format in the provided file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@app.post("/load/followers-from-file", tags=["File Loading"])
def load_followers_from_file(file_path: str, scraped_from: str, db: Session = Depends(get_db)):
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found at: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            followers_json = json.load(f)
        
        load_followers_data(db, followers_json, scraped_from=scraped_from)
        return {"status": "success", "message": f"Successfully loaded followers for {scraped_from} from {file_path}."}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format in the provided file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@app.post("/load/following-from-file", tags=["File Loading"])
def load_following_from_file(file_path: str, scraped_from: str, db: Session = Depends(get_db)):
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found at: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            following_json = json.load(f)
        
        load_following_data(db, following_json, scraped_from=scraped_from)
        return {"status": "success", "message": f"Successfully loaded 'following' for {scraped_from} from {file_path}."}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format in the provided file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/status", tags=["Monitoring"])
def get_database_status(db: Session = Depends(get_db)):
    try:
        tweet_count = db.query(Tweet).count()
        profile_count = db.query(Profile).count()
        activity_count = db.query(Activity).count()
        followers_count = db.query(Follower).count()
        following_count = db.query(Following).count()
        
        return {
            "ok": True,
            "activities": activity_count,
            "profiles": profile_count,
            "tweets": tweet_count,
            "followers_total": followers_count,
            "following_total": following_count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")