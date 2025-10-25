import os
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.engine import URL
import psycopg
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Convert DATABASE_URL to use postgresql+psycopg instead of postgresql
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")
if DATABASE_URL.startswith('postgresql://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DATABASE_URL)
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Activity(Base):
    __tablename__ = "activities"

    id = Column(Integer, primary_key=True, index=True)
    handle = Column(String, unique=True, nullable=False)
    status = Column(String, nullable=False, default="pending") 
    is_active = Column(Boolean, default=True) 
    rawdata = Column(Text, nullable=True) 
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True) 
    created_by = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)
    deleted_by = Column(String, nullable=True)


class Profile(Base):
    __tablename__ = "profiles"
    id = Column(Integer, ForeignKey('activities.id'), primary_key=True)
    profile = Column(String)
    name = Column(String)
    description_current = Column(Text)
    description_previous = Column(Text)
    followers_count_current = Column(BigInteger)
    followers_count_previous = Column(BigInteger)
    following_count_current = Column(BigInteger)
    following_count_previous = Column(BigInteger)
    profile_created_at = Column(DateTime)
    updated_columns = Column(String)
    scraped_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    raw_data_current = Column(Text)
    raw_data_previous = Column(Text) 
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True) 
    created_by = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)
    deleted_by = Column(String, nullable=True)

class Tweet(Base):
    __tablename__ = "tweets"
    id = Column(BigInteger, primary_key=True)
    url = Column(String, unique=True)
    text = Column(Text)
    retweet_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    quote_count = Column(Integer, default=0)
    created_at = Column(DateTime)
    bookmark_count = Column(Integer, default=0)
    handler = Column(String)

class Follower(Base):
    __tablename__ = "followers"
    id = Column(BigInteger, primary_key=True)
    profile_id = Column(Integer, ForeignKey('activities.id'))
    username = Column(String, index=True)
    name = Column(String)
    scraped_from = Column(String)
    scraped_at = Column(DateTime, default=datetime.utcnow)

class Following(Base):
    __tablename__ = "following"
    id = Column(BigInteger, primary_key=True)
    profile_id = Column(Integer, ForeignKey('activities.id'))
    username = Column(String, index=True)
    name = Column(String)
    scraped_from = Column(String)
    scraped_at = Column(DateTime, default=datetime.utcnow)

def create_database_tables():
    try:
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully (if they didn't exist).")
    except Exception as e:
        print(f" An error occurred during table creation: {e}")

def get_or_create_activity(session: Session, handle: str) -> Activity:
    activity = session.query(Activity).filter_by(handle=handle).first()
    if not activity:
        activity = Activity(handle=handle)
        session.add(activity)
        session.commit()
    return activity

def parse_twitter_date(date_string: Optional[str]) -> Optional[datetime]:
    if not date_string: return None
    try:
        return datetime.strptime(date_string, '%a %b %d %H:%M:%S +0000 %Y')
    except (ValueError, TypeError):
        return None


def load_profile_data(session: Session, data: Dict[str, Any]):
    handle = data.get('profile')
    if not handle:
        print("Skipping profile: missing 'profile' key.")
        return
    
    activity = get_or_create_activity(session, handle)
    profile = session.query(Profile).filter(Profile.id == activity.id).first()
    updated_columns = []

    raw_json = {
        "status": data.get("status"),
        "profile": data.get("profile"),
        "rest_id": data.get("rest_id"),
        "blue_verified": data.get("blue_verified"),
        "verification_type": data.get("verification_type"),
        "affiliates": data.get("affiliates"),
        "business_account": data.get("business_account"),
        "avatar": data.get("avatar"),
        "header_image": data.get("header_image"),
        "desc": data.get("desc"),
        "name": data.get("name"),
        "website": data.get("website"),
        "protected": data.get("protected"),
        "location": data.get("location"),
        "friends": data.get("friends"),
        "sub_count": data.get("sub_count"),
        "statuses_count": data.get("statuses_count"),
        "media_count": data.get("media_count"),
        "pinned_tweet_ids_str": data.get("pinned_tweet_ids_str"),
        "created_at": data.get("created_at"),
        "profile_id": data.get("id"),
    }

    raw_json_str = json.dumps(raw_json, ensure_ascii=False, indent=4)

    if profile:
        if profile.raw_data_current != raw_json_str:
            if profile.raw_data_current:
                profile.raw_data_previous = json.dumps(profile.raw_data_current, ensure_ascii=False, indent=4)
            else:
                profile.raw_data_previous = None

            profile.raw_data_current = raw_json_str
            updated_columns.append("raw_data")

        new_name = data.get('name')
        if new_name and profile.name != new_name:
            profile.name = new_name
            updated_columns.append('name')

        new_desc = data.get('desc')
        if profile.description_current != new_desc:
            profile.description_previous = profile.description_current
            profile.description_current = new_desc
            updated_columns.append('description')

        new_followers = data.get('sub_count')
        if profile.followers_count_current != new_followers:
            profile.followers_count_previous = profile.followers_count_current
            profile.followers_count_current = new_followers
            updated_columns.append('followers_count')

        new_following = data.get('friends')
        if profile.following_count_current != new_following:
            profile.following_count_previous = profile.following_count_current
            profile.following_count_current = new_following
            updated_columns.append('following_count')

        new_created_at = parse_twitter_date(data.get('created_at'))
        if profile.profile_created_at != new_created_at:
            profile.profile_created_at = new_created_at
            updated_columns.append('profile_created_at')

        profile.updated_columns = ",".join(updated_columns) if updated_columns else None

    else:
        profile = Profile(
            id=activity.id,
            profile=handle,
            name=data.get('name'),
            description_current=data.get('desc'),
            followers_count_current=data.get('sub_count'),
            following_count_current=data.get('friends'),
            profile_created_at=parse_twitter_date(data.get('created_at')),
            updated_columns="name,description,followers_count,following_count,profile_created_at,raw_data",
            raw_data_current=raw_json_str,
            raw_data_previous=None,
        )
        session.add(profile)

    session.commit()
    print(f"Profile for '{handle}' loaded/updated.")
    print(f"Raw description from data: {data.get('desc')}")

def load_tweets_data(session: Session, data: Dict[str, Any], scraped_from: str):
    timeline = data.get('timeline', [])
    if not timeline: return
    original_tweets_loaded = 0
    for tweet_data in timeline:
        author_handle = tweet_data.get('author', {}).get('screen_name')
        if author_handle != scraped_from:
            continue
        tweet_id = tweet_data.get('tweet_id')
        if not tweet_id: continue
        tweet = Tweet(
            id=int(tweet_id),
            url=f"https://twitter.com/{author_handle}/status/{tweet_id}",
            text=tweet_data.get('text'),
            retweet_count=tweet_data.get('retweets'),
            reply_count=tweet_data.get('replies'),
            like_count=tweet_data.get('favorites'),
            quote_count=tweet_data.get('quotes'),
            created_at=parse_twitter_date(tweet_data.get('created_at')),
            bookmark_count=tweet_data.get('bookmarks'),
            handler=author_handle
        )
        session.merge(tweet)
        original_tweets_loaded += 1
    
    session.commit()
    print(f"Loaded/updated {original_tweets_loaded} original tweets for '{scraped_from}'.")

def load_followers_data(session: Session, data: Dict[str, Any], scraped_from: str, limit: Optional[int] = None):
    if data.get("protected") == 1:
        print(f"Cannot load followers for '{scraped_from}': Account is private.")
        return

    activity = get_or_create_activity(session, scraped_from)
    followers_list = data.get('followers', [])

    if limit is not None:
        followers_list = followers_list[:limit]

    for follower_data in followers_list:
        follower = Follower(
            id=int(follower_data['user_id']),
            profile_id=activity.id,
            username=follower_data.get('screen_name'),
            name=follower_data.get('name'),
            scraped_from=scraped_from
        )
        session.merge(follower)
    session.commit()
    print(f"Loaded/updated {len(followers_list)} followers for '{scraped_from}'.")

def load_following_data(session: Session, data: Dict[str, Any], scraped_from: str, limit: Optional[int] = None):
    if data.get("protected") == 1:
        print(f"Cannot load following for '{scraped_from}': Account is private.")
        return

    activity = get_or_create_activity(session, scraped_from)
    following_list = data.get('following', [])

    if limit is not None:
        following_list = following_list[:limit]

    for following_data in following_list:
        following = Following(
            id=int(following_data['user_id']),
            profile_id=activity.id,
            username=following_data.get('screen_name'),
            name=following_data.get('name'),
            scraped_from=scraped_from
        )
        session.merge(following)
    session.commit()
    print(f"Loaded/updated {len(following_list)} accounts followed by '{scraped_from}'.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:

        db.close()

