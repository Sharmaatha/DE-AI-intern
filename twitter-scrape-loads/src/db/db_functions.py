# db_functions.py 
import os
import json
from datetime import datetime, timezone, date
from dotenv import load_dotenv
from sqlalchemy import JSON, create_engine, Column, String, Integer, BigInteger, DateTime, Boolean, Text, ForeignKey, UniqueConstraint, Date
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, DateTime, Boolean, Text, ForeignKey, UniqueConstraint, Date, TIMESTAMP
from sqlalchemy.sql import func
from typing import Dict, Any, Optional, List
from src.utils.scraping_functions import get_profile, get_followers, get_following 
import time 
    
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Activity(Base):
    __tablename__ = "activities"
    id = Column(Integer, primary_key=True, autoincrement=True)
    handle = Column(String, nullable=False, index=True)
    query_type = Column(String, nullable=False, index=True)
    status = Column(String, default='pending', index=True)
    active = Column(Boolean, default=True)
    task_data = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    created_by = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    deleted_by = Column(String, nullable=True)
    last_sync_on = Column(DateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint('handle', 'query_type', name='uq_handle_query_type'),)

class Profile(Base):
    __tablename__ = "profiles"
    handle = Column(String, primary_key=True)
    name = Column(String, nullable=True)
    activity_id = Column(Integer, ForeignKey('activities.id', ondelete="SET NULL"), nullable=True)
    description = Column(Text, nullable=True)
    followers_count = Column(BigInteger, nullable=True)
    following_count = Column(BigInteger, nullable=True)
    profile_created_at = Column(DateTime, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    curr_update = Column(JSONB, nullable=True)
    prev_update = Column(JSONB, nullable=True)
    changed = Column(ARRAY(String), nullable=True)
    changed_on = Column(DateTime(timezone=True), nullable=True)
    curr_raw_date = Column(DateTime(timezone=True), nullable=True)
    prev_raw_date = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    updated_by = Column(String, nullable=True)
    avatar = Column(String, nullable=True)
    location = Column(String, nullable=True)
    website = Column(String, nullable=True)
    is_review = Column(Integer, nullable=True, default=0)
    is_signal = Column(Integer, nullable=True, default=0)
    linkedin_data = Column(JSONB, nullable=True)

class Tweet(Base):
    __tablename__ = "tweets"
    id = Column(BigInteger, primary_key=True)
    activity_id = Column(Integer, ForeignKey('activities.id'), index=True)
    url = Column(String, unique=True, nullable=True)
    text = Column(Text, nullable=True)
    retweet_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    quote_count = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), nullable=True)
    bookmark_count = Column(Integer, default=0)
    handle = Column(String, index=True, nullable=True)
    author_rest_id = Column(String, nullable=True)
    author_name = Column(String, nullable=True)
    author_screen_name = Column(String, nullable=True)
    author_image = Column(String, nullable=True)

class Follower(Base):
    __tablename__ = "followers"
    id = Column(BigInteger, primary_key=True)
    scraped_from_handle = Column(String, ForeignKey('profiles.handle', ondelete="CASCADE"), primary_key=True)
    activity_id = Column(Integer, ForeignKey('activities.id', ondelete="SET NULL"), index=True, nullable=True)
    username = Column(String, index=True, nullable=True)
    name = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    updated_by = Column(String, nullable=True)
    last_sync_on = Column(DateTime(timezone=True), nullable=True)

class Following(Base):
    __tablename__ = "following"
    id = Column(BigInteger, primary_key=True)
    scraped_from_handle = Column(String, ForeignKey('profiles.handle', ondelete="CASCADE"), primary_key=True)
    activity_id = Column(Integer, ForeignKey('activities.id', ondelete="SET NULL"), index=True, nullable=True)
    username = Column(String, index=True, nullable=True)
    name = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    updated_by = Column(String, nullable=True)
    last_sync_on = Column(DateTime(timezone=True), nullable=True)

class MasterTweet(Base):
    __tablename__ = "master_tweet"
    id = Column(BigInteger, primary_key=True)
    handle = Column(String, nullable=False, unique=True, index=True)
    name = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    profile_image_url = Column(String, nullable=True)
    followers_count = Column(BigInteger, nullable=True)
    following_count = Column(BigInteger, nullable=True)
    media_count = Column(BigInteger, nullable=True)
    profile_created_at = Column(DateTime(timezone=True), nullable=True)
    curr_updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    website = Column(String, nullable=True)
    location = Column(String, nullable=True)

class SalesNavLeads(Base):
    __tablename__ = "salesnav_leads"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    list_id = Column(String(255), unique=True, nullable=True)
    source = Column(Text, nullable=True)
    agent_type = Column(Text, nullable=True)
    entity_type = Column(Integer, nullable=True, default=0)
    subscriber_ids = Column(JSONB, nullable=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    meta_data = Column(JSON, nullable=True)
    created_by = Column(BigInteger, nullable=True)
    updated_by = Column(BigInteger, nullable=True)
    status = Column(String(50), nullable=True)
    project_type = Column(String(50), nullable=False, server_default='project')
    lead_type = Column(Integer, nullable=True)
    active = Column(Boolean, nullable=False, server_default='false')
    deleted_at = Column(TIMESTAMP, nullable=True)
    deleted_by = Column(BigInteger, nullable=True)
    last_sync_on = Column(TIMESTAMP, nullable=True)
    last_imported_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.timezone('UTC', func.now()))
    updated_at = Column(TIMESTAMP, server_default=func.timezone('UTC', func.now()), onupdate=func.timezone('UTC', func.now()))
    source_from = Column(Integer, nullable=True, default=2)

def create_database_tables():
    try:
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully (if they didn't exist).")
    except Exception as e:
        print(f" An error occurred during table creation: {e}")

def get_or_create_profile(session: Session, handle: str, created_by: str = "system") -> Profile:
    profile = session.query(Profile).filter_by(handle=handle).first()
    if not profile:
        profile = Profile(
            handle=handle, 
            created_by=created_by, 
            updated_by=created_by,
            is_review=0,  
            is_signal=0   
        )
        session.add(profile)
        session.flush()
        session.refresh(profile)
    return profile

def get_or_create_activity(
    session: Session,
    handle: str,
    query_type: str,
    created_by: str,
    active: bool = True
) -> Optional[Activity]:
    activity = session.query(Activity).filter_by(handle=handle, query_type=query_type).first()
    if activity:
        activity.active = active
        activity.updated_by = created_by
        activity.updated_at = func.now()
        activity.status = 'pending' if active else 'deactivated'
    else:
        get_or_create_profile(session, handle, created_by)
        activity = Activity(
            handle=handle, query_type=query_type, active=active,
            created_by=created_by, updated_by=created_by,
            status='pending' if active else 'deactivated'
        )
        session.add(activity)
    session.flush()
    session.refresh(activity)
    return activity

def parse_twitter_date(date_string: Optional[str]) -> Optional[datetime]:
    if not date_string: return None
    formats_to_try = [
        '%a %b %d %H:%M:%S +0000 %Y',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S %z'
    ]
    for fmt in formats_to_try:
        try:
            dt = datetime.strptime(date_string, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (ValueError, TypeError):
            continue
    try:
        dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        print(f"Warning: Could not parse date string: {date_string} with any known format.")
        return None

def _compare_profiles(old_json: Optional[Dict], new_json: Dict) -> List[str]:
    if not old_json:
        return list(new_json.keys())
    changed_keys = []
    all_keys = set(old_json.keys()) | set(new_json.keys())
    for key in all_keys:
        old_value = old_json.get(key)
        new_value = new_json.get(key)
        if old_value != new_value:
            changed_keys.append(key)
    return changed_keys

def upsert_master_profile(session: Session, data: Dict[str, Any]):
    print(f"--- Running upsert_master_profile for data: {data.get('screen_name') or data.get('profile')}")

    profile_id_str = data.get('user_id') or data.get('rest_id')
    handle = data.get('screen_name') or data.get('profile')

    if not handle:
        print("WARN (upsert): Skipping master_tweet upsert, no handle found.")
        return

    if not profile_id_str:
        print(f"WARN (upsert): user_id AND rest_id missing for handle '{handle}'. Trying to find existing by handle.")
        existing_profile = session.query(MasterTweet).filter(MasterTweet.handle == handle).first()
        if not existing_profile or not existing_profile.id:
            print(f"ERROR (upsert): Cannot insert or reliably update {handle} without a user_id/rest_id. Skipping.")
            return
        profile_id_int = existing_profile.id
        print(f"INFO (upsert): Found existing ID {profile_id_int} for handle {handle}.")
    else:
        try:
            profile_id_int = int(profile_id_str)
        except (ValueError, TypeError):
            print(f"ERROR (upsert): Invalid user_id/rest_id '{profile_id_str}' for handle '{handle}'. Skipping.")
            return

    existing_profile_by_id = session.query(MasterTweet).filter(MasterTweet.id == profile_id_int).first()
    if existing_profile_by_id and existing_profile_by_id.handle != handle:
        print(f"HANDLE CHANGE DETECTED: ID {profile_id_int} now has handle '{handle}', previously '{existing_profile_by_id.handle}'. Updating handle.")

    profile_obj = MasterTweet(
        id=profile_id_int,
        handle=handle,
        name=data.get('name'),
        description=data.get('description') or data.get('desc'),
        profile_image_url=data.get('profile_image') or data.get('avatar'),
        followers_count=data.get('followers_count') or data.get('sub_count'),
        following_count=data.get('friends_count') or data.get('friends'),
        media_count=data.get('media_count'),
        profile_created_at=parse_twitter_date(data.get('created_at')),
        website=data.get('website'),
        location=data.get('location')
    )

    try:
        print(f"DEBUG (upsert): Merging MasterTweet for ID: {profile_id_int}, Handle: {handle}")
        session.merge(profile_obj)
        session.flush()
        print(f"DEBUG (upsert): Merge successful for {handle}.")
    except Exception as e:
        print(f"ERROR (upsert): Error merging MasterTweet for ID {profile_id_int}, Handle {handle}: {e}")
        session.rollback()

def load_profile_data(session: Session, data: Dict[str, Any], activity: Activity, updated_by: str):
    handle = data.get('profile')
    if not handle:
        print("Skipping profile load: missing 'profile' key.")
        return

    profile = get_or_create_profile(session, handle, created_by=updated_by)

    old_json = profile.curr_update
    profile.prev_update = old_json
    previous_raw_date = profile.curr_raw_date
    profile.prev_raw_date = previous_raw_date
    profile.curr_update = data
    profile.curr_raw_date = datetime.now(timezone.utc)
    changed_fields = _compare_profiles(old_json if isinstance(old_json, dict) else {}, data)
    profile.changed = changed_fields
    if changed_fields:
        profile.changed_on = datetime.now(timezone.utc)

    profile.name = data.get('name')
    profile.description = data.get('desc')
    profile.followers_count = data.get('sub_count')
    profile.following_count = data.get('friends')
    profile.profile_created_at = parse_twitter_date(data.get('created_at'))
    profile.updated_by = updated_by
    profile.avatar = data.get("avatar")
    profile.location = data.get("location")
    profile.website = data.get("website")
    profile.activity_id = activity.id

    print(f"DEBUG: Calling upsert_master_profile from load_profile_data for {handle}")
    upsert_master_profile(session, data)
    print(f"Profile for '{handle}' processed in session.")

def load_tweets_data(session: Session, data: Dict[str, Any], activity: Activity, limit: Optional[int] = None):
    timeline = data.get('timeline', [])
    if not timeline:
        print(f"No timeline data found for {activity.handle}.")
        return
    if limit is not None:
        timeline = timeline[:limit]

    original_tweets_loaded = 0
    for tweet_data in timeline:
        author_info = tweet_data.get('author')
        if not author_info: continue
        author_handle = author_info.get('screen_name')
        if author_handle != activity.handle: continue

        tweet_id_str = tweet_data.get('tweet_id')
        if not tweet_id_str: continue

        try: tweet_id = int(tweet_id_str)
        except (ValueError, TypeError): continue

        tweet_created_at = parse_twitter_date(tweet_data.get('created_at'))
        if not tweet_created_at: continue
        author_rest_id = author_info.get('rest_id')
        author_name = author_info.get('name')
        author_screen_name = author_info.get('screen_name')
        author_image = author_info.get('profile_image') or author_info.get('avatar')

        tweet = Tweet(
            id=tweet_id, activity_id=activity.id,
            url=f"https://twitter.com/{author_handle}/status/{tweet_id}",
            text=tweet_data.get('text'), retweet_count=tweet_data.get('retweets', 0),
            reply_count=tweet_data.get('replies', 0), like_count=tweet_data.get('favorites', 0),
            quote_count=tweet_data.get('quotes', 0), created_at=tweet_created_at,
            bookmark_count=tweet_data.get('bookmarks', 0), handle=author_handle,
            author_rest_id=author_rest_id,
            author_name=author_name,
            author_screen_name=author_screen_name,
            author_image=author_image
        )
        try:
            session.merge(tweet)
            original_tweets_loaded += 1
        except Exception as e:
            print(f"Error merging tweet {tweet_id} for handle {activity.handle}: {e}")
            session.rollback()
    print(f" Processed {original_tweets_loaded} tweets for '{activity.handle}' in session.")

def load_followers_data(session: Session, data: Dict[str, Any], activity: Activity, user: str, limit: Optional[int] = None):
    get_or_create_profile(session, activity.handle, created_by=user)

    followers_list = data.get('followers', [])
    if not followers_list:
        print(f"No followers found in data for {activity.handle}.")
        return
    if limit is not None:
        followers_list = followers_list[:limit]

    loaded_count = 0
    current_time = datetime.now(timezone.utc)

    for follower_data in followers_list:
        if not follower_data.get('user_id') or not follower_data.get('screen_name'):
            print(f"Skipping follower due to missing user_id/screen_name: {follower_data}")
            continue

        print(f"DEBUG: Calling upsert_master_profile from load_followers_data for follower: {follower_data.get('screen_name')}")
        upsert_master_profile(session, follower_data)

        try:
            follower_id = int(follower_data['user_id'])
        except (ValueError, TypeError):
            print(f"Skipping follower due to invalid user_id: {follower_data.get('user_id')}")
            continue

        follower = session.query(Follower).filter_by(id=follower_id, scraped_from_handle=activity.handle).first()

        if follower:
            follower.name = follower_data.get('name')
            follower.username = follower_data.get('screen_name')
            follower.updated_by = user
            follower.activity_id = activity.id
            follower.last_sync_on = current_time
        else:
            follower = Follower(
                id=follower_id,
                activity_id=activity.id,
                scraped_from_handle=activity.handle,
                username=follower_data.get('screen_name'),
                name=follower_data.get('name'),
                created_by=user,
                updated_by=user,
                last_sync_on=current_time
            )
            session.add(follower)

        loaded_count += 1

    print(f"Processed {loaded_count} followers for '{activity.handle}' in session.")


def load_following_data(session: Session, data: Dict[str, Any], activity: Activity, user: str, limit: Optional[int] = None):
    following_list = data.get('following', [])
    if not following_list:
        print(f"No following found in data for {activity.handle}.")
        return
    if limit is not None: following_list = following_list[:limit]

    loaded_count = 0
    current_time = datetime.now(timezone.utc)

    for following_data in following_list:
        if not following_data.get('user_id') or not following_data.get('screen_name'):
             print(f"Skipping following due to missing user_id/screen_name: {following_data}")
             continue

        print(f"DEBUG: Calling upsert_master_profile from load_following_data for following: {following_data.get('screen_name')}")
        upsert_master_profile(session, following_data)

        try: following_id = int(following_data['user_id'])
        except (ValueError, TypeError):
             print(f"Skipping following due to invalid user_id: {following_data.get('user_id')}")
             continue

        following_entry = session.query(Following).filter_by(id=following_id, scraped_from_handle=activity.handle).first()

        if following_entry:
            following_entry.name = following_data.get('name')
            following_entry.username = following_data.get('screen_name')
            following_entry.updated_by = user
            following_entry.activity_id = activity.id
            following_entry.last_sync_on = current_time
        else:
            following_entry = Following(
                id=following_id, activity_id=activity.id, scraped_from_handle=activity.handle,
                username=following_data.get('screen_name'), name=following_data.get('name'),
                created_by=user, updated_by=user, last_sync_on=current_time
            )
            session.add(following_entry)
        loaded_count += 1

    print(f"Processed {loaded_count} accounts followed by '{activity.handle}' in session.")

def has_followers_data(session: Session, handle: str) -> bool:
    return session.query(session.query(Follower).filter(Follower.scraped_from_handle == handle).exists()).scalar()

def has_following_data(session: Session, handle: str) -> bool:
    return session.query(session.query(Following).filter(Following.scraped_from_handle == handle).exists()).scalar()


def get_active_handles_by_type(session: Session, query_type: str) -> List[str]:
    """Get active handles for specific query type, sorted by last_sync_on"""
    activities = (
        session.query(Activity.handle, Activity.last_sync_on)
        .filter(Activity.active == True, Activity.query_type == query_type)
        .order_by(Activity.last_sync_on.asc().nullsfirst(), Activity.created_at.asc())
        .all()
    )
    seen = set()
    handles = []
    for handle, _ in activities:
        if handle not in seen:
            seen.add(handle)
            handles.append(handle)
    print(f"Found {len(handles)} unique active handles for {query_type} batch scraping.")
    return handles

def get_active_profile_handles(session: Session) -> List[str]:
    """Backward compatibility wrapper"""
    return get_active_handles_by_type(session, 'get_profile')

def update_activity_last_sync(session: Session, handle: str, query_type: str = 'get_profile'):
    activity = session.query(Activity).filter_by(handle=handle, query_type=query_type).first()
    if activity:
        activity.last_sync_on = datetime.now(timezone.utc)
        session.add(activity)
        print(f"Updated last_sync_on for {query_type} activity of {handle}")
    else:
        print(f"Warning: Could not find {query_type} activity for {handle} to update last_sync_on.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
def store_linkedin_data(session: Session, handle: str, linkedin_json: Dict[str, Any], updated_by: str = "system"):
    """Store LinkedIn data for a profile"""
    profile = get_or_create_profile(session, handle, created_by=updated_by)
    
    profile.linkedin_data = linkedin_json
    profile.updated_by = updated_by
    session.flush()
    print(f"LinkedIn data stored for {handle}")
    return profile

def get_active_groups(session: Session, project_type: str = "twitter-profiles", source_from: int = 2, status: str = "pending"):
    """Get active groups matching criteria."""
    groups = session.query(SalesNavLeads).filter(
        SalesNavLeads.project_type == project_type,
        SalesNavLeads.source_from == source_from,
        SalesNavLeads.status == status,
        SalesNavLeads.active == True
    ).all()
    return groups

def mark_group_completed(session: Session, group_id: int):
    """Mark a group as completed."""
    group = session.query(SalesNavLeads).filter_by(id=group_id).first()
    if group:
        group.status = "completed"
        group.last_sync_on = datetime.now(timezone.utc)
        session.flush()
        print(f"Group {group_id} ({group.name}) marked as completed")

def sync_activities_from_group(session: Session, group_id: int, handlers: list[str], query_type: str, created_by: str):
    """Create or update activities for handlers in a group."""
    for handler in handlers:
        activity = get_or_create_activity(
            session=session,
            handle=handler,
            query_type=query_type,
            created_by=created_by,
            active=True
        )
        print(f"Activity '{handler}' ({query_type}) linked to group {group_id}")

def process_group_profiles(session: Session, group_id: int, created_by: str = "system"):
    """Process a single group - create activities"""
    group = session.query(SalesNavLeads).filter_by(id=group_id).first()
    if not group:
        print(f"Group {group_id} not found")
        return {"ok": False, "message": "Group not found"}

    meta_data = group.meta_data or {} 
    twitter_handlers = meta_data.get('twitter_handlers', [])
    if not twitter_handlers:
        print(f"No twitter_handlers found in group {group_id}")
        return {"ok": False, "message": "No handlers found"}

    print(f"Setting up activities for group '{group.name}' (ID: {group_id})")
    print(f"Total handlers: {len(twitter_handlers)}")

    query_types = ['get_profile', 'get_tweets', 'get_followers', 'get_following']
    for query_type in query_types:
        sync_activities_from_group(session, group_id, twitter_handlers, query_type, created_by)
    
    session.flush()
    print(f"Created activities for group '{group.name}'")
    
    return {
        "ok": True, 
        "group_id": group_id,
        "handlers": twitter_handlers,
        "message": f"Activities created for {len(twitter_handlers)} handlers."
    }

def process_all_pending_groups(session: Session, created_by: str = "system"):
    pending_groups = session.query(SalesNavLeads).filter(
        SalesNavLeads.status == "pending",
        SalesNavLeads.active == True,
        SalesNavLeads.source_from == 2
    ).all()

    if not pending_groups:
        return {"ok": True, "message": "No pending groups found.", "processed": 0}

    print(f"Found {len(pending_groups)} pending groups")

    all_results = []
    
    for group in pending_groups:
        meta_data = group.meta_data or {}
        twitter_handlers = meta_data.get('twitter_handlers', [])
        
        if not twitter_handlers:
            print(f"Skipping group {group.id} - no handlers")
            continue
        
        print(f"\nProcessing group: {group.name} (Type: {group.project_type})")
        
        if group.project_type == "twitter-profiles":
            # Only scrape profiles
            sync_activities_from_group(session, group.id, twitter_handlers, 'get_profile', created_by)
            
            profiles_scraped = 0
            for handler in twitter_handlers:
                try:
                    activity = session.query(Activity).filter_by(
                        handle=handler, query_type='get_profile'
                    ).first()
                    
                    if activity:
                        print(f"  [Profile] Scraping {handler}...")
                        profile_json = get_profile(handler)
                        
                        if profile_json:
                            load_profile_data(session, profile_json, activity=activity, updated_by=created_by)
                            activity.status = 'completed'
                            profiles_scraped += 1
                            print(f"  [Profile] {handler}")
                        else:
                            activity.status = 'failed'
                        
                        session.flush()
                        time.sleep(1)
                except Exception as e:
                    print(f"  [Profile] {handler}: {e}")
                    session.rollback()
            
            all_results.append({
                "group_id": group.id,
                "group_name": group.name,
                "project_type": "twitter-profiles",
                "handlers_count": len(twitter_handlers),
                "profiles_scraped": profiles_scraped
            })
        
        elif group.project_type == "twitter-followers":
            # Only scrape followers
            sync_activities_from_group(session, group.id, twitter_handlers, 'get_followers', created_by)
            
            followers_scraped = 0
            for handler in twitter_handlers:
                try:
                    activity = session.query(Activity).filter_by(
                        handle=handler, query_type='get_followers'
                    ).first()
                    
                    if activity:
                        print(f"  [Followers] Scraping {handler}...")
                        limit = 20 if has_followers_data(session, handler) else 50
                        
                        all_followers_list = []
                        current_cursor = None
                        
                        while len(all_followers_list) < limit:
                            followers_json = get_followers(handler, cursor=current_cursor)
                            if not followers_json:
                                break
                            
                            new_followers = followers_json.get("followers", [])
                            if new_followers:
                                all_followers_list.extend(new_followers)
                            
                            current_cursor = followers_json.get("next_cursor")
                            if not followers_json.get("more_users", False) or not current_cursor:
                                break
                            
                            time.sleep(1)
                        
                        if all_followers_list:
                            data_to_load = {"followers": all_followers_list}
                            load_followers_data(session, data_to_load, activity, user=created_by, limit=limit)
                            activity.status = 'completed'
                            followers_scraped += 1
                            print(f"  [Followers] {handler} ({len(all_followers_list)})")
                        else:
                            activity.status = 'failed'
                        
                        session.flush()
                        time.sleep(1)
                except Exception as e:
                    print(f"  [Followers] {handler}: {e}")
                    session.rollback()
            
            all_results.append({
                "group_id": group.id,
                "group_name": group.name,
                "project_type": "twitter-followers",
                "handlers_count": len(twitter_handlers),
                "followers_scraped": followers_scraped
            })
        
        elif group.project_type == "twitter-following":
            # Only scrape following
            sync_activities_from_group(session, group.id, twitter_handlers, 'get_following', created_by)
            
            following_scraped = 0
            for handler in twitter_handlers:
                try:
                    activity = session.query(Activity).filter_by(
                        handle=handler, query_type='get_following'
                    ).first()
                    
                    if activity:
                        print(f"  [Following] Scraping {handler}...")
                        limit = 20 if has_following_data(session, handler) else 50
                        
                        all_following_list = []
                        current_cursor = None
                        
                        while len(all_following_list) < limit:
                            following_json = get_following(handler, cursor=current_cursor)
                            if not following_json:
                                break
                            
                            new_following = following_json.get("following") or following_json.get("users", [])
                            if new_following:
                                all_following_list.extend(new_following)
                            
                            current_cursor = following_json.get("next_cursor")
                            if not following_json.get("more_users", False) or not current_cursor:
                                break
                            
                            time.sleep(1)
                        
                        if all_following_list:
                            data_to_load = {"following": all_following_list}
                            load_following_data(session, data_to_load, activity, user=created_by, limit=limit)
                            activity.status = 'completed'
                            following_scraped += 1
                            print(f"  [Following] {handler} ({len(all_following_list)})")
                        else:
                            activity.status = 'failed'
                        
                        session.flush()
                        time.sleep(1)
                except Exception as e:
                    print(f"  [Following] {handler}: {e}")
                    session.rollback()
            
            all_results.append({
                "group_id": group.id,
                "group_name": group.name,
                "project_type": "twitter-following",
                "handlers_count": len(twitter_handlers),
                "following_scraped": following_scraped
            })
        
        # Mark group as completed
        mark_group_completed(session, group.id)
    
    session.commit()
    
    return {
        "ok": True,
        "processed": len(all_results),
        "groups": all_results,
        "message": f"Processed {len(all_results)} groups"
    }