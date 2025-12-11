
import logging
from typing import Optional, Dict, Any
import requests
from config import RAPIDAPI_KEY, API_TIMEOUT

logger = logging.getLogger(__name__)


class TwitterScraper:
    """
    Pure Twitter profile enrichment using RapidAPI.
    """

    def __init__(self):
        self.base_url = "https://twitter-api45.p.rapidapi.com/screenname.php"
        self.headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "twitter-api45.p.rapidapi.com",
        }
        self.timeout = API_TIMEOUT

    def extract_handle_from_url(self, url: str) -> Optional[str]:
        if not url:
            return None
        try:
            handle = url.rstrip("/").split("/")[-1]
            return handle.replace("@", "").split("?")[0]
        except:
            return None

    def get_profile(self, twitter_handle: str) -> Optional[Dict[str, Any]]:
        if not twitter_handle:
            return None

        try:
            resp = requests.get(
                self.base_url,
                headers=self.headers,
                params={"screenname": twitter_handle},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data or "error" in data:
                return None

            return {
                "handle": twitter_handle,
                "name": data.get("name", ""),
                "bio": data.get("desc", ""),
                "followers_count": data.get("sub_count", 0),
                "following_count": data.get("friends", 0),
                "tweet_count": data.get("statuses_count", 0),
                "location": data.get("location", ""),
                "website": data.get("website", ""),
                "profile_image": data.get("avatar", ""),
                "verified": data.get("blue_verified", False),
                "created_at": data.get("created_at", ""),
                "source": "rapidapi_twitter",
            }

        except Exception as e:
            logger.error(f"Twitter enrichment error: {e}")
            return None
