"""
Configuration settings for ProductHunt Scraper API
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_KEY = os.getenv("PRODUCTHUNT_API_KEY")
if not API_KEY:
    raise ValueError("PRODUCTHUNT_API_KEY environment variable is required. Please set it in your .env file.")
BASE_URL = "https://producthunt-api.p.rapidapi.com"

# App Configuration
APP_TITLE = "ProductHunt Scraper API"
APP_DESCRIPTION = """
 API for scraping ProductHunt product launches.
"""
APP_VERSION = "1.0.0"

# Server Configuration
HOST = "127.0.0.1"
PORT = 8000
RELOAD = False

# CORS Configuration
CORS_ORIGINS = ["*"]
CORS_CREDENTIALS = True
CORS_METHODS = ["*"]
CORS_HEADERS = ["*"]

# Directory Configuration
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'api_server.log'
