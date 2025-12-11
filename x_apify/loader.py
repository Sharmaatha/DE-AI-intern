from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, DateTime, Boolean, Text, inspect, text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from utils import get_tweet_by_user_handler, get_tweet_by_user_handler_from_file, get_followers_from_file,get_following_from_file
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, echo=False, future=True)
Base = declarative_base()

class Tweet(Base):
    __tablename__ = "tweets"
    tweet_id = Column(BigInteger, primary_key=True, autoincrement=True)
    url = Column(String)
    text = Column(String)
    retweet_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    quote_count = Column(Integer, default=0)
    created_at = Column(DateTime)
    bookmark_count = Column(Integer, default=0)
    handler = Column(String, default="unknown")
    batch_time = Column(DateTime, default=datetime.utcnow)

class Activity(Base):
    __tablename__ = "activities"
    id = Column(Integer, primary_key=True, autoincrement=True)
    handler = Column(String, unique=True)

class Profile(Base):
    __tablename__ = "profiles"
    profile_id = Column(Integer, primary_key=True, autoincrement=False)
    profile_name = Column(String, unique=True)
    name = Column(String)
    description = Column(String)
    followers = Column(Integer)
    following = Column(Integer)
    profile_created_at = Column(DateTime)

class Follower(Base):
    __tablename__ = "followers"
    follower_id = Column(BigInteger, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey("profiles.profile_id"))
    username = Column(String)
    name = Column(String)
    description = Column(Text)
    followers_count = Column(Integer, default=0)
    following_count = Column(Integer, default=0)
    tweets_count = Column(Integer, default=0)
    created_at = Column(DateTime)
    verified = Column(Boolean, default=False)
    location = Column(String)
    url = Column(String)
    profile_image_url = Column(String)
    profile_image_url_hd = Column(String)
    scrape_type = Column(String)
    scraped_from = Column(String)
    scraped_at = Column(DateTime, default=datetime.utcnow)

class Following(Base):
    __tablename__ = "following"
    following_id = Column(BigInteger, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey("profiles.profile_id"))
    username = Column(String)
    name = Column(String)
    description = Column(Text)
    followers_count = Column(Integer, default=0)
    following_count = Column(Integer, default=0)
    tweets_count = Column(Integer, default=0)
    created_at = Column(DateTime)
    verified = Column(Boolean, default=False)
    location = Column(String)
    url = Column(String)
    profile_image_url = Column(String)
    profile_image_url_hd = Column(String)
    scrape_type = Column(String)
    scraped_from = Column(String)
    scraped_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def get_session() -> Session:
    return SessionLocal()

def add_column_if_not_exists(table_name: str, column_name: str, column_type: str = "VARCHAR"):
    session = get_session()
    try:
        inspector = inspect(engine)
        existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        if column_name not in existing_columns:
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
            session.execute(text(alter_query))
            session.commit()
            print(f"Column '{column_name}' added to table '{table_name}'")
            return True
        else:
            print(f"Column '{column_name}' already exists in '{table_name}'")
            return False
    except Exception as e:
        session.rollback()
        print(f"Error adding column: {e}")
        return False
    finally:
        session.close()

def load_profiles_to_db(profiles: list[dict]):
    if not profiles:
        print("No profiles to insert.")
        return

    session = get_session()
    try:
        for profile_data in profiles:
            profile_name = profile_data.get("profile")
            
            matching_activity = session.query(Activity).filter_by(handler=profile_name).first()
            
            if not matching_activity:
                print(f"No matching activity found for profile: {profile_name}. Skipping.")
                continue      
            activity_id = matching_activity.id
            existing_profile = session.query(Profile).filter_by(profile_name=profile_name).first()
            
            if existing_profile:
                for field in ["name", "description", "followers", "following", "profile_created_at"]:
                    new_value = profile_data.get(field)
                    current_value = getattr(existing_profile, field)
                    if (current_value is None or current_value == "") and new_value is not None:
                        setattr(existing_profile, field, new_value)
                print(f"Updated profile: {profile_name}")
            else:
                new_profile = Profile(
                    profile_id=activity_id,
                    profile_name=profile_name,
                    name=profile_data.get("name"),
                    description=profile_data.get("description"),
                    followers=profile_data.get("followers"),
                    following=profile_data.get("following"),
                    profile_created_at=profile_data.get("profile_created_at")
                )
                session.add(new_profile)
                print(f"Created new profile: {profile_name} (profile_id: {activity_id})")
        session.commit()
        print(f"Profiles synced successfully.")

    except Exception as e:
        session.rollback()
        print(f"Profile database error: {e}")
    finally:
        session.close()

def load_tweets_to_db(tweets: list[dict]):
    if not tweets:
        print("No tweets to insert.")
        return
    session = get_session()
    batch_time = datetime.utcnow()
    inserted = 0
    updated = 0
    try:
        for item in tweets:
            existing_tweet = session.query(Tweet).filter_by(url=item.get("url")).first()
            
            if existing_tweet:
                existing_tweet.text = item.get("text", "")
                existing_tweet.retweet_count = item.get("retweet_count", 0)
                existing_tweet.reply_count = item.get("reply_count", 0)
                existing_tweet.like_count = item.get("like_count", 0)
                existing_tweet.quote_count = item.get("quote_count", 0)
                existing_tweet.bookmark_count = item.get("bookmark_count", 0)
                existing_tweet.batch_time = batch_time
                updated += 1
            else:
                new_tweet = Tweet(
                    url=item.get("url", ""),
                    text=item.get("text", ""),
                    retweet_count=item.get("retweet_count", 0),
                    reply_count=item.get("reply_count", 0),
                    like_count=item.get("like_count", 0),
                    quote_count=item.get("quote_count", 0),
                    created_at=item.get("created_at"),
                    bookmark_count=item.get("bookmark_count", 0),
                    handler=item.get("handler", "unknown"),
                    batch_time=batch_time
                )
                session.add(new_tweet)
                inserted += 1
        
        session.commit()
        print(f"Tweets: {inserted} inserted, {updated} updated. Batch: {batch_time}")
    except Exception as e:
        session.rollback()
        print(f"Database error: {e}")
    finally:
        session.close()

def load_followers_to_db(followers: list[dict]):
    session = get_session()
    try:
        for f in followers:
            obj = Follower(
                username=f.get("username"),
                name=f.get("name"),
                description=f.get("description"),
                followers_count=f.get("followers_count"),
                following_count=f.get("following_count"),
                tweets_count=f.get("tweets_count"),
                created_at=f.get("created_at"),
                verified=f.get("verified"),
                location=f.get("location"),
                url=f.get("url"),
                profile_image_url=f.get("profile_image_url"),
                profile_image_url_hd=f.get("profile_image_url_hd"),
                scraped_from=f.get("scraped_from"),
                scrape_type=f.get("scrape_type")
            )
            session.merge(obj) 
        session.commit()
        print(f"Loaded {len(followers)} followers into DB")
    except Exception as e:
        session.rollback()
        print(f"DB Error: {e}")
    finally:
        session.close()

def load_following_to_db(following: list[dict]):
    session = get_session()
    try:
        for f in following:
            obj = Following(
                username=f.get("username"),
                name=f.get("name"),
                description=f.get("description"),
                followers_count=f.get("followers_count"),
                following_count=f.get("following_count"),
                tweets_count=f.get("tweets_count"),
                created_at=f.get("created_at"),
                verified=f.get("verified"),
                location=f.get("location"),
                url=f.get("url"),
                profile_image_url=f.get("profile_image_url"),
                profile_image_url_hd=f.get("profile_image_url_hd"),
                scraped_from=f.get("scraped_from"),
                scrape_type=f.get("scrape_type")
            )
            session.merge(obj)
        session.commit()
        print(f"Loaded {len(following)} following into DB")
    except Exception as e:
        session.rollback()
        print(f"DB Error: {e}")
    finally:
        session.close()

def load_all_handlers(
    maxItems: int = 5, 
    handlers: list[str] | None = None,
    use_static_file: bool = False
) -> dict:
    result = {
        "tweets_fetched": 0,
        "profiles_loaded": 0,
        "handlers": []
    }  
    if use_static_file:
        tweets, profiles = get_tweet_by_user_handler_from_file("data.json")
        print(f"Loaded {len(tweets)} tweets and {len(profiles)} profiles from file.")
        if profiles:
            load_profiles_to_db(profiles)
            result["profiles_loaded"] = len(profiles)
            result["tweets_fetched"] = len(tweets)
    else:
        if not handlers:
            print("No handlers provided for Apify fetch.")
            return result
        
        tweets = get_tweet_by_user_handler(handlers, maxItems=maxItems)
        print(f"Fetched {len(tweets)} tweets from Apify.")
        result["tweets_fetched"] = len(tweets)
        result["handlers"] = handlers
    load_tweets_to_db(tweets)
    return result

def load_all_followers(
    handlers: list[str] | None = None,
    use_static_file: bool = False,
    max_followers: int = 5
) -> dict:
    result = {
        "followers_loaded": 0,
        "handlers": []
    }
    if use_static_file:
        followers = get_followers_from_file("followersdata.json")
        print(f"Loaded {len(followers)} followers from file.")
        result["followers_loaded"] = len(followers)
    else:
        if not handlers:
            print("No handlers provided for Apify fetch.")
            return result
        followers = get_followers_by_user_handler(handlers, maxItems=max_followers)
        print(f"Fetched {len(followers)} followers from Apify.")
        result["followers_loaded"] = len(followers)
        result["handlers"] = handlers
    load_followers_to_db(followers)
    return result

def load_all_following(
    handlers: list[str] | None = None,
    use_static_file: bool = False,
    max_following: int = 5
) -> dict:
    result = {
        "following_loaded": 0,
        "handlers": []
    }
    if use_static_file:
        following = get_following_from_file("followingdata.json")
        print(f"Loaded {len(following)} following from file.")
        result["following_loaded"] = len(following)
    else:
        if not handlers:
            print("No handlers provided for Apify fetch.")
            return result
        following = get_following_by_user_handler(handlers, maxItems=max_following)
        print(f"Fetched {len(following)} following from Apify.")
        result["following_loaded"] = len(following)
        result["handlers"] = handlers
    load_following_to_db(following)
    return result
