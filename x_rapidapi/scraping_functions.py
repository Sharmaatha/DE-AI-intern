from dotenv import load_dotenv
import os
import requests
from typing import Optional, Dict, Any
load_dotenv()
api_header = {
	"x-rapidapi-key": os.getenv('X-API-KEY'),
	"x-rapidapi-host": "twitter-api45.p.rapidapi.com"
}

def get_profile(twitter_handle: str, rest_id: str | None = None):
    url = "https://twitter-api45.p.rapidapi.com/screenname.php"
    querystring = {
        "screenname": twitter_handle,
    }
    if rest_id != None:
        querystring["rest_id"] = rest_id
    headers = api_header
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response body: {response.text}")
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred: {err}")
    return None

def get_tweets(twitter_handle: str, rest_id: str | None = None, cursor : str | None = None):
    url = "https://twitter-api45.p.rapidapi.com/timeline.php"
    querystring = {"screenname": twitter_handle}
    if rest_id != None:
        querystring["rest_id"] = rest_id
    if cursor != None:
        querystring["cursor"] = cursor
    headers = api_header
    try:
        response = requests.get(url, headers = headers, params = querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response body: {response.text}")
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred: {err}")
    return None

def get_following(twitter_handle: str, rest_id: str | None = None, cursor : str | None = None):
    url = "https://twitter-api45.p.rapidapi.com/following.php"
    querystring = {"screenname": twitter_handle}
    if rest_id != None:
        querystring["rest_id"] = rest_id
    if cursor != None:
        querystring["cursor"] = cursor
    headers = api_header
    try:
        response = requests.get(url, headers = headers, params = querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response body: {response.text}")
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred: {err}")
    return None

def get_followers(twitter_handle: str, blue_verified: Optional[int] = None, cursor: Optional[str] = None):
    url = "https://twitter-api45.p.rapidapi.com/followers.php"
    attempts = [blue_verified] if blue_verified is not None else [1, 0]
    
    if len(attempts) > 1:
        print(f"Searching for followers for '{twitter_handle}' by checking verified and unverified both.")

    last_response_json = None
    for bv_status in attempts:
        querystring = {"screenname": twitter_handle}
        if bv_status is not None:
            querystring["blue_verified"] = bv_status
        if cursor:
            querystring["cursor"] = cursor
        
        if len(attempts) > 1:
            print(f"-> Trying with blue_verified = {bv_status}")

        try:
            response = requests.get(url, headers=api_header, params=querystring)
            response.raise_for_status()
            response_json = response.json()
            last_response_json = response_json

            if response_json and response_json.get("followers"):
                if len(attempts) > 1:
                    print(f"Success! Found followers with blue_verified = {bv_status}.")
                return response_json

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err} | Response: {response.text}")
            return None
        except requests.exceptions.RequestException as err:
            print(f"An unexpected error occurred: {err}")
            return None

    print(f"Could not find a non-empty follower list for '{twitter_handle}'. Returning the last response.")
    return last_response_json