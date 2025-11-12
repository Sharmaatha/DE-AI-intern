"""
Google OAuth and API authentication
"""
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from config import settings
from models import GoogleUserInfo

#might need changes here
def get_credentials():
    creds = None
    
    if os.path.exists(settings.TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(
            settings.TOKEN_FILE, 
            settings.GOOGLE_SCOPES
        )
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                settings.CLIENT_SECRET_FILE, 
                settings.GOOGLE_SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        with open(settings.TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    
    return creds


def get_calendar_service(creds):
    return build('calendar', 'v3', credentials=creds)

def get_user_service(creds):
    return build('oauth2', 'v2', credentials=creds)

def get_user_info(user_service) -> GoogleUserInfo:
    user_info = user_service.userinfo().get().execute()
    return GoogleUserInfo(
        email=user_info.get('email'),
        name=user_info.get('name', 'Unknown User'),
        picture=user_info.get('picture')
    )