# test_api.py
import os
import json
from dotenv import load_dotenv
from utils.scraping_functions import get_followers  # <-- Adjust this import path if needed

# --- Setup ---
load_dotenv()  # Load your .env file to get the API key
handle_to_test = "Saraya"

print(f"--- Starting direct API test for: {handle_to_test} ---")

# --- Call the function ---
try:
    response_json = get_followers(handle_to_test)

    # --- Print the result ---
    if response_json:
        print("\n--- API Response Received (SUCCESS) ---")
        print(json.dumps(response_json, indent=2))
        
        followers_list = response_json.get("followers", [])
        print(f"\nFound {len(followers_list)} followers in this response.")
        
        if not followers_list and response_json.get("more_users"):
             print("WARNING: API returned 0 followers but says 'more_users' is true. This might be an API bug.")
    else:
        print("\n--- API Response (FAILURE) ---")
        print("API returned None or an empty response.")

except Exception as e:
    print(f"\n--- An Error Occurred ---")
    print(f"Error: {e}")