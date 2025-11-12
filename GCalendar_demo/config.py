"""
Configuration settings loaded from environment variables
"""
import os
from typing import List


class Settings:
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5433"))
    DB_NAME: str = os.getenv("DB_NAME", "calendar_demo")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    
    CLIENT_SECRET_FILE: str = os.getenv("CLIENT_SECRET_FILE", "client_secret.json")
    TOKEN_FILE: str = os.getenv("TOKEN_FILE", "token.json")
    
    GOOGLE_SCOPES: List[str] = [
        'https://www.googleapis.com/auth/calendar.readonly',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile',
        'openid'
    ]
    
    CHECK_INTERVAL: int = int(os.getenv("CHECK_INTERVAL", "10"))
settings = Settings()