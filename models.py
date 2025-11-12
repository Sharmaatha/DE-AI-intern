"""
Pydantic models for database schemas and data validation
"""
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime

class User(BaseModel):
    id: Optional[int] = None
    email: EmailStr
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class Attendee(BaseModel):
    email: Optional[str] = None
    responseStatus: Optional[str] = None
    displayName: Optional[str] = ""

class Event(BaseModel):
    id: Optional[int] = None
    user_id: int
    event_id: str
    summary: Optional[str] = "No Title"
    description: Optional[str] = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "confirmed"
    attendees_count: int = 0
    attendees_list: Optional[str] = None 
    change_history: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class SyncToken(BaseModel):
    id: Optional[int] = None
    user_id: int
    sync_token: Optional[str] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class GoogleUserInfo(BaseModel):
    email: EmailStr
    name: Optional[str] = "Unknown User"
    picture: Optional[str] = None

class CalendarEvent(BaseModel):
    id: str
    summary: Optional[str] = "No Title"
    description: Optional[str] = ""
    start: dict
    end: dict
    status: Optional[str] = "confirmed"
    attendees: Optional[List[Attendee]] = []