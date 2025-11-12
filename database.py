"""
Database connection and setup utilities
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from config import settings

def get_db_connection():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD
    )

def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            event_id VARCHAR(255) UNIQUE NOT NULL,
            summary TEXT,
            description TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status VARCHAR(50) DEFAULT 'confirmed',
            attendees_count INTEGER DEFAULT 0,
            attendees_list TEXT,
            change_history TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_tokens (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) UNIQUE,
            sync_token TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Database tables initialized")