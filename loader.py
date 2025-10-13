# loader.py
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils import get_tweet_by_user_handler, get_tweet_by_user_handler_from_file

# ---------- Database Setup ----------
DATABASE_URL = "postgresql+pg8000://postgres:Sharnu@23@172.18.0.1:5432/twitter_db"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL, echo=False, future=True)

# Define Base class
Base = declarative_base()

# ---------- Tweet Model ----------
class Tweet(Base):
    __tablename__ = "tweets"

    id = Column(Integer, primary_key=True, autoincrement=True)
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

# Create tables if not exist
Base.metadata.create_all(engine)

# ---------- Session Setup ----------
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# ---------- Utility ----------
def safe_get(item, *keys, default=None):
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return default

# ---------- Main Logic ----------
def load_tweets_to_db(tweets):
    """Insert a batch of tweets into the database using SQLAlchemy ORM."""
    if not tweets:
        print("No tweets to insert.")
        return

    session = SessionLocal()
    batch_time = datetime.utcnow()

    try:
        tweet_objects = [
            Tweet(
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
            for item in tweets
        ]

        session.bulk_save_objects(tweet_objects)
        session.commit()

        print(f"✅ Inserted {len(tweet_objects)} tweets successfully. Batch time: {batch_time}")

    except Exception as e:
        session.rollback()
        print(f"❌ Database error: {e}")

    finally:
        session.close()


def load_all_handlers(maxItems=1, handlers=None, use_static_file=False):
    """Fetch tweets (from Apify or file) and load into the database."""
    if use_static_file:
        tweets = get_tweet_by_user_handler_from_file("data.json")
    else:
        if not handlers:
            print("⚠️ No handlers provided for Apify fetch.")
            return []
        tweets = get_tweet_by_user_handler(handlers, maxItems=maxItems)

    load_tweets_to_db(tweets)
    return tweets
