from scraping_functions import get_followers, get_following, get_profile, get_tweets
from db_functions import (
    SessionLocal,
    create_database_tables,
    load_profile_data,
    load_tweets_data,
    load_followers_data,
    load_following_data
)

def run_scraper_workflow():
    print("--- Initializing Database ---")
    create_database_tables()
    db_session = SessionLocal()
    target_handles = ["emiliaclarke", "elonmusk"]

    try:
        for handle in target_handles:
            profile_json = get_profile(handle)
            if profile_json:
                print("Profile data fetched successfully.")
                load_profile_data(db_session, profile_json)
            else:
                print(f"Failed to fetch profile for '{handle}'. Skipping to next user.")
                continue 
            cursor = None
            for page_num in range(1, 3):
                print(f"   - Fetching page {page_num}...")
                tweets_json = get_tweets(handle, cursor=cursor)
                if tweets_json and tweets_json.get('timeline'):
                    print(f"Page {page_num} of tweets fetched successfully.")
                    load_tweets_data(db_session, tweets_json, scraped_from=handle)
                    cursor = tweets_json.get('next_cursor')
                    if not cursor or cursor == "0":
                        print("Reached the end of the timeline.")
                        break
                else:
                    print("No more tweets found or an API error occurred.")
                    break
            followers_json = get_followers(handle)
            if followers_json:
                print("Followers data fetched successfully.")
                load_followers_data(db_session, followers_json, scraped_from=handle, limit=10)
            else:
                print(f"Failed to fetch followers for '{handle}'.")

            following_json = get_following(handle)
            if following_json:
                print("'Following' data fetched successfully.")
                load_following_data(db_session, following_json, scraped_from=handle, limit=10)
            else:
                print(f"Failed to fetch 'following' for '{handle}'.")

    except Exception as e:
        print(f"\nAn unexpected error occurred during the workflow: {e}")
    finally:
        db_session.close()
        print("\n Workflow complete. Database session closed.")


if __name__ == "__main__":
    run_scraper_workflow()