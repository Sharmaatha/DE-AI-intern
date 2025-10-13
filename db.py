# db.py
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Create a database URL
import urllib.parse
safe_password = urllib.parse.quote_plus(DB_PASSWORD)
DATABASE_URL = f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the connection engine here
engine = create_engine(DATABASE_URL)
