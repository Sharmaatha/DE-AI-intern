# schemas.py
from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import Optional, List, Any

class BaseConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

class CreateGroupRequest(BaseModel):
    name: str
    twitter_handlers: List[str]
    description: Optional[str] = None
    created_by: str = "system"
    
class ScrapeTaskRequest(BaseModel):
    handle: str
    created_by: str = "system"
    active: bool = True
    limit: Optional[int] = None

class ProfileSchema(BaseConfig):
    handle: str
    name: Optional[str] = None
    activity_id: Optional[int] = None
    description: Optional[str] = None
    followers_count: Optional[int] = None
    following_count: Optional[int] = None
    profile_created_at: Optional[datetime] = None
    scraped_at: datetime
    curr_update: Optional[Any] = None
    prev_update: Optional[Any] = None
    changed: Optional[List[str]] = None
    changed_on: Optional[datetime] = None
    curr_raw_date: Optional[datetime] = None
    prev_raw_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    avatar: Optional[str] = None
    location: Optional[str] = None
    website: Optional[str] = None
    is_review: Optional[int] = 0
    is_signal: Optional[int] = 0
    linkedin_data: Optional[Any] = None

class ActivitySchema(BaseConfig):
    id: int
    handle: str
    query_type: str
    status: str
    active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    task_data: Optional[Any] = None
    last_sync_on: Optional[datetime] = None

class TweetSchema(BaseConfig):
    id: int
    activity_id: Optional[int] = None
    url: Optional[str] = None
    text: Optional[str] = None
    retweet_count: Optional[int] = 0
    reply_count: Optional[int] = 0
    like_count: Optional[int] = 0
    quote_count: Optional[int] = 0
    created_at: Optional[datetime] = None
    bookmark_count: Optional[int] = 0
    handle: Optional[str] = None
    author_rest_id: Optional[str] = None
    author_name: Optional[str] = None
    author_screen_name: Optional[str] = None
    author_image: Optional[str] = None

class FollowerSchema(BaseConfig):
    id: int
    scraped_from_handle: str
    activity_id: Optional[int] = None
    username: Optional[str] = None
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    last_sync_on: Optional[datetime] = None

class FollowingSchema(BaseConfig):
    id: int
    scraped_from_handle: str
    activity_id: Optional[int] = None
    username: Optional[str] = None
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    last_sync_on: Optional[datetime] = None


class MasterTweetSchema(BaseConfig):
    id: int
    handle: str
    name: Optional[str] = None
    description: Optional[str] = None
    profile_image_url: Optional[str] = None
    followers_count: Optional[int] = None
    following_count: Optional[int] = None
    media_count: Optional[int] = None
    profile_created_at: Optional[datetime] = None
    curr_updated_at: datetime
    website: Optional[str] = None
    location: Optional[str] = None

class SalesNavLeadsSchema(BaseConfig):
    id: int
    list_id: Optional[int] = None
    source: Optional[str] = None
    agent_type: Optional[str] = None
    entity_type: Optional[int] = 0
    subscriber_ids: Optional[Any] = None
    name: str
    project_type: str
    description: Optional[str] = None
    metadata: Optional[Any] = None
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
    status: Optional[str] = None
    project_type: Optional[str] = "project"
    lead_type: Optional[int] = None
    active: bool = False
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    last_sync_on: Optional[datetime] = None
    last_imported_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    source_from: Optional[int] = 2


class APIProfileResponseSchema(BaseModel):
    profile: str
    rest_id: Optional[str] = None 
    name: str
    desc: Optional[str] = None
    sub_count: Optional[int] = None 
    friends: Optional[int] = None
    created_at: Optional[str] = None

class APIAuthorSchema(BaseModel):
    screen_name: str
    name: str

class APITweetSchema(BaseModel):
    tweet_id: str
    text: str
    favorites: Optional[int] = 0 
    author: APIAuthorSchema

class APITweetsResponseSchema(BaseModel):
    timeline: List[APITweetSchema]
    next_cursor: Optional[str] = None

class APIFollowUserSchema(BaseModel):
    user_id: str
    screen_name: str
    name: str
    description: Optional[str] = None
    profile_image: Optional[str] = None
    statuses_count: Optional[int] = None
    followers_count: Optional[int] = None
    friends_count: Optional[int] = None
    media_count: Optional[int] = None
    created_at: Optional[str] = None

class APIFollowersResponseSchema(BaseModel):
    followers: List[APIFollowUserSchema]
    next_cursor: Optional[str] = None
    more_users: Optional[bool] = None

class APIFollowingResponseSchema(BaseModel):
    following: List[APIFollowUserSchema]
    next_cursor: Optional[str] = None
    more_users: Optional[bool] = None