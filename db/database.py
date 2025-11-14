import sqlite3
from contextlib import contextmanager
from typing import Generator
from config import settings
import logging

logger = logging.getLogger(__name__)

class Database:
    """ database manager"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(settings.DATABASE_PATH)
        self._init_database()
    
    def _init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    founded_year INTEGER,
                    industry TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS persons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    company_id INTEGER,
                    role TEXT,
                    joined_date TEXT,
                    FOREIGN KEY (company_id) REFERENCES companies(id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL UNIQUE,
                    file_path TEXT NOT NULL,
                    document_type TEXT NOT NULL,
                    upload_date TIMESTAMP NOT NULL,
                    total_pages INTEGER NOT NULL,
                    file_size_mb REAL NOT NULL,
                    related_company_id INTEGER,
                    related_person_id INTEGER,
                    chunk_count INTEGER DEFAULT 0,
                    faiss_start_idx INTEGER,
                    faiss_end_idx INTEGER,
                    FOREIGN KEY (related_company_id) REFERENCES companies(id),
                    FOREIGN KEY (related_person_id) REFERENCES persons(id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS document_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    page_numbers TEXT NOT NULL,
                    text_content TEXT NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    faiss_vector_id INTEGER,
                    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
                    UNIQUE(document_id, chunk_index)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_filename 
                ON documents(filename)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_document 
                ON document_chunks(document_id)
            """)
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = ()):
        """Execute a query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_write(self, query: str, params: tuple = ()):
        """Execute a write query and commit"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.lastrowid

db = Database()