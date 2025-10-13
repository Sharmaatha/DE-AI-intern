# utils.py
import json
from apify_client import ApifyClient

APIFY_TOKEN = "APIFY_API_TOKEN"
client = ApifyClient(APIFY_TOKEN)

def get_tweet_by_user_handler(handlers, maxItems=1):
    #Fetch tweets for given handlers from Apify Actor.
    result = []
    for handle in handlers:
        run_input = {
            "twitterHandles": [handle],
            "maxItems": maxItems,
            "sort": "Latest",
            "tweetLanguage": "en",
            "author": "apify",
            "start": "2025-10-13",
            "end": "2025-10-14",
        }
        run = client.actor("apidojo~tweet-scraper").call(run_input=run_input)
        dataset = list(client.dataset(run["defaultDatasetId"]).iterate_items())

        for item in dataset:
            tweet_data = {
                "url": item.get("url") or item.get("twitterUrl"),
                "text": item.get("text") or item.get("fullText"),
                "retweet_count": item.get("retweet_count") or item.get("retweets") or item.get("retweetCount", 0),
                "reply_count": item.get("reply_count") or item.get("replies") or item.get("replyCount", 0),
                "like_count": item.get("like_count") or item.get("likes") or item.get("likeCount", 0),
                "quote_count": item.get("quote_count") or item.get("quotes") or item.get("quoteCount", 0),
                "created_at": item.get("created_at") or item.get("createdAt") or item.get("timestamp"),
                "bookmark_count": item.get("bookmark_count") or item.get("bookmarks") or item.get("bookmarkCount", 0),
                "handler": handle
            }
            result.append(tweet_data)
    return result


def get_tweet_by_user_handler_from_file(file_path="data.json"):
   # Load tweets from a static JSON file instead of Apify.
    result = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            dataset = json.load(f)
        for item in dataset:
            tweet_data = {
                "url": item.get("url") or item.get("twitterUrl"),
                "text": item.get("text") or item.get("fullText"),
                "retweet_count": item.get("retweet_count") or item.get("retweets") or item.get("retweetCount", 0),
                "reply_count": item.get("reply_count") or item.get("replies") or item.get("replyCount", 0),
                "like_count": item.get("like_count") or item.get("likes") or item.get("likeCount", 0),
                "quote_count": item.get("quote_count") or item.get("quotes") or item.get("quoteCount", 0),
                "created_at": item.get("created_at") or item.get("createdAt") or item.get("timestamp"),
                "bookmark_count": item.get("bookmark_count") or item.get("bookmarks") or item.get("bookmarkCount", 0),
                "handler": item.get("handler", "unknown")
            }
            result.append(tweet_data)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
    return result
