from apify_client import ApifyClient
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import os
load_dotenv() 

APIFY_TOKEN =  os.getenv("APIFY_TOKEN")
client = ApifyClient(APIFY_TOKEN)

def get_tweet_by_user_handler(handlers, maxItems=5):
    print(f" Fetching up to {maxItems} tweets per handler")
    result = []
    for handle in handlers:
        clean_handle = handle.lstrip('@')
        print(f"\n Scraping tweets for @{clean_handle}...")
        run_input = {
            "twitterHandles": [clean_handle], 
            "maxItems": maxItems,
            "proxyConfig": {"useApifyProxy": True}
        }
        try:
            run = client.actor("apidojo/tweet-scraper").call(run_input=run_input)
            dataset = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            if not dataset:
                print(f"No tweets returned for @{clean_handle}")
                continue
            for item in dataset[:maxItems]:
                tweet_data = {
                    "url": item.get("url") or item.get("twitterUrl") or f"https://twitter.com/{clean_handle}/status/{item.get('id')}",
                    "text": item.get("text") or item.get("fullText") or item.get("full_text") or "",
                    "retweet_count": item.get("retweetCount") or item.get("retweet_count") or item.get("retweets") or 0,
                    "reply_count": item.get("replyCount") or item.get("reply_count") or item.get("replies") or 0,
                    "like_count": item.get("likeCount") or item.get("like_count") or item.get("likes") or item.get("favoriteCount") or 0,
                    "quote_count": item.get("quoteCount") or item.get("quote_count") or item.get("quotes") or 0,
                    "created_at": parse_date(item.get("createdAt") or item.get("created_at") or item.get("timestamp")),
                    "bookmark_count": item.get("bookmarkCount") or item.get("bookmark_count") or item.get("bookmarks") or 0,
                    "handler": clean_handle
                }
                result.append(tweet_data)
            print(f"@{clean_handle}: Scraped {len(dataset[:maxItems])} tweets")
        except Exception as e:
            print(f"Error scraping tweets for @{clean_handle}: {e}")
            continue
    print(f"\nTotal tweets scraped: {len(result)}")
    return result


def parse_date(date_str):
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str
    formats = [
        "%a %b %d %H:%M:%S %z %Y",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def parse_author_data(author: dict) -> dict:
    if not author:
        return {}
    return {
        "name": author.get("name"),
        "description": author.get("description"),
        "followers": author.get("followers"),
        "following": author.get("following"),
        "profile_created_at": parse_date(author.get("createdAt"))
    }


def get_tweet_by_user_handler_from_file(file_path="data.json"):
    tweets = []
    profiles = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            dataset = json.load(f)

        if isinstance(dataset, dict) and "users" in dataset:
            dataset = dataset["users"]

        for user_data in dataset:
            profile_name = user_data.get("profile")
            user_tweets = user_data.get("tweets", [])

            author_data = {}
            if user_tweets and len(user_tweets) > 0:
                author_data = user_tweets[0].get("author", {})

            if author_data:
                profile_info = parse_author_data(author_data)
                profile_info["profile"] = profile_name
                profiles.append(profile_info)

            for tweet_item in user_tweets:
                tweet_data = {
                    "url": tweet_item.get("url") or tweet_item.get("twitterUrl"),
                    "text": tweet_item.get("text") or tweet_item.get("fullText"),
                    "retweet_count": tweet_item.get("retweet_count") or tweet_item.get("retweets") or tweet_item.get("retweetCount", 0),
                    "reply_count": tweet_item.get("reply_count") or tweet_item.get("replies") or tweet_item.get("replyCount", 0),
                    "like_count": tweet_item.get("like_count") or tweet_item.get("likes") or tweet_item.get("likeCount", 0),
                    "quote_count": tweet_item.get("quote_count") or tweet_item.get("quotes") or tweet_item.get("quoteCount", 0),
                    "created_at": parse_date(tweet_item.get("created_at") or tweet_item.get("createdAt")),
                    "bookmark_count": tweet_item.get("bookmark_count") or tweet_item.get("bookmarks") or tweet_item.get("bookmarkCount", 0),
                    "handler": profile_name or "unknown"
                }
                tweets.append(tweet_data)

        print(f"Parsed {len(tweets)} tweets and {len(profiles)} profiles from {file_path}")
        return tweets, profiles
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return [], []


def get_followers_from_file(file_path="followersdata.json"):
    followers = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            dataset = json.load(f)
        if not isinstance(dataset, list):
            print(f"Expected a list in {file_path}, got {type(dataset)}")
            return []
        for follower_item in dataset:
            follower_data = {
                "username": follower_item.get("username"),
                "name": follower_item.get("name"),
                "description": follower_item.get("description"),
                "followers_count": follower_item.get("followers_count", 0),
                "following_count": follower_item.get("following_count", 0),
                "tweets_count": follower_item.get("tweets_count", 0),
                "created_at": parse_date(follower_item.get("created_at")),
                "verified": follower_item.get("verified", False),
                "location": follower_item.get("location"),
                "url": follower_item.get("url"),
                "profile_image_url": follower_item.get("profile_image_url"),
                "profile_image_url_hd": follower_item.get("profile_image_url_hd"),
                "scraped_from": follower_item.get("scraped_from"),
                "scrape_type": follower_item.get("scrape_type", "followers")
            }
            followers.append(follower_data)
        print(f"Parsed {len(followers)} followers from {file_path}")
        return followers
    except Exception as e:
        print(f"Error loading followers file: {e}")
        return []


def get_following_from_file(file_path="followingdata.json"):
    following = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            dataset = json.load(f)

        if not isinstance(dataset, list):
            print(f"Expected a list in {file_path}, got {type(dataset)}")
            return []

        for following_item in dataset:
            following_data = {
                "username": following_item.get("username"),
                "name": following_item.get("name"),
                "description": following_item.get("description"),
                "followers_count": following_item.get("followers_count", 0),
                "following_count": following_item.get("following_count", 0),
                "tweets_count": following_item.get("tweets_count", 0),
                "created_at": parse_date(following_item.get("created_at")),
                "verified": following_item.get("verified", False),
                "location": following_item.get("location"),
                "url": following_item.get("url"),
                "profile_image_url": following_item.get("profile_image_url"),
                "profile_image_url_hd": following_item.get("profile_image_url_hd"),
                "scraped_from": following_item.get("scraped_from"),
                "scrape_type": "following"
            }
            following.append(following_data)

        print(f"Parsed {len(following)} following from {file_path}")
        return following
    except Exception as e:
        print(f"Error loading following file: {e}")
        return []