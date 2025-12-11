from fastapi import FastAPI
from loader import load_all_handlers, load_all_followers, load_all_following, get_session, Activity, Tweet, Profile, Follower, Following
from utils import get_tweet_by_user_handler
from loader import load_followers_to_db, load_following_to_db

app = FastAPI()


@app.get("/")
def root():
    message = {"message": "Twitter Data Scraper API"}
    print("Root endpoint hit:", message)
    return message


@app.get("/fetch_all_handlers")
def fetch_all_handlers(max_items: int = 5):
    session = get_session()
    try:
        handlers = [row[0] for row in session.query(Activity.handler).distinct().all()]
        if not handlers:
            print("No handlers found in activities table.")
            return {"ok": False, "message": "No handlers found in activities table."}

        print(f"Processing {len(handlers)} handlers: {handlers}")
        result = load_all_handlers(maxItems=max_items, handlers=handlers, use_static_file=False)
        print("Tweets fetched result:", result)
        return {
            "ok": True,
            "handlers": handlers,
            "tweets_fetched": result["tweets_fetched"],
            "max_items_per_handler": max_items,
            "message": "Tweets fetched from Apify successfully"
        }
    except Exception as e:
        print("Error in fetch_all_handlers:", e)
        return {"ok": False, "error": str(e)}
    finally:
        session.close()


@app.get("/fetch_from_file")
def fetch_from_file():
    result = load_all_handlers(maxItems=5, use_static_file=True)
    print("Data loaded from file:", result)
    return {
        "ok": True,
        "tweets_fetched": result["tweets_fetched"],
        "profiles_loaded": result["profiles_loaded"],
        "message": "Data loaded from file successfully"
    }


@app.get("/fetch_followers_from_file")
def fetch_followers_from_file():
    result = load_all_followers(use_static_file=True)
    print("Followers loaded from file:", result)
    return {
        "ok": True,
        "followers_loaded": result["followers_loaded"],
        "message": "Followers loaded from file successfully"
    }


@app.get("/fetch_following_from_file")
def fetch_following_from_file():
    result = load_all_following(use_static_file=True)
    print("Following loaded from file:", result)
    return {
        "ok": True,
        "following_loaded": result["following_loaded"],
        "message": "Following loaded from file successfully"
    }


@app.get("/status")
def status():
    session = get_session()
    try:
        tweet_count = session.query(Tweet).count()
        profile_count = session.query(Profile).count()
        activity_count = session.query(Activity).count()
        follower_count = session.query(Follower).count()
        following_count = session.query(Following).count()

        status_result = {
            "ok": True,
            "tweets": tweet_count,
            "profiles": profile_count,
            "activities": activity_count,
            "followers": follower_count,
            "following": following_count,
            "message": "All counts retrieved successfully"
        }
        print("Status endpoint result:", status_result)
        return status_result
    except Exception as e:
        print("Error in status endpoint:", e)
        return {"ok": False, "error": str(e)}
    finally:
        session.close()