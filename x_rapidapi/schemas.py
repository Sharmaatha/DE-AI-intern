from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

class Config:
    orm_mode = True

class ProfileSchema(BaseModel):
    id: int
    profile: Optional[str]
    name: Optional[str]
    description_current: Optional[str]
    description_previous: Optional[str]
    followers_count_current: Optional[int]
    followers_count_previous: Optional[int]
    following_count_current: Optional[int]
    following_count_previous: Optional[int]
    profile_created_at: Optional[datetime]
    updated_columns: Optional[str]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]
    created_by: Optional[str]
    updated_by: Optional[str]
    deleted_by: Optional[str]

    class Config(Config):
        pass

class TweetSchema(BaseModel):
    id: int
    url: Optional[str] = None
    text: Optional[str] = None
    like_count: int
    handler: Optional[str] = None

    class Config(Config):
        pass

class FollowerSchema(BaseModel):
    id: int
    username: Optional[str] = None
    name: Optional[str] = None
    scraped_from: Optional[str] = None
    
    class Config(Config):
        pass

class FollowingSchema(BaseModel):
    id: int
    username: Optional[str] = None
    name: Optional[str] = None
    scraped_from: Optional[str] = None

    class Config(Config):
        pass

class APIProfileResponseSchema(BaseModel):
    profile: str
    name: str
    desc: Optional[str] = None
    sub_count: int
    friends: int
    created_at: str

class APIAuthorSchema(BaseModel):
    screen_name: str
    name: str

class APITweetSchema(BaseModel):
    tweet_id: str
    text: str
    favorites: int
    author: APIAuthorSchema

class APITweetsResponseSchema(BaseModel):
    timeline: List[APITweetSchema]

class APIFollowUserSchema(BaseModel):
    user_id: str
    screen_name: str
    name: str

class APIFollowersResponseSchema(BaseModel):
    followers: List[APIFollowUserSchema]

class APIFollowingResponseSchema(BaseModel):
    following: List[APIFollowUserSchema]