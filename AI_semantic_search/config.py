from pydantic_settings import BaseSettings
from pathlib import Path
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""
    
    CHUNK_SIZE: int = 2000
    CHUNK_OVERLAP: int = 300
    
    EMBEDDING_MODEL: str = "sentence-transformers/all-mpnet-base-v2" 
    EMBEDDING_DIMENSION: int = 768
    
    KEYWORD_BOOST_WEIGHT: float = 0.5  
    SEMANTIC_WEIGHT: float = 0.5  
    MINIMUM_SIMILARITY_THRESHOLD: float = 0.15  
    
    FAISS_INDEX_FILE: str = "documents.index"
    FAISS_METADATA_FILE: str = "metadata.json"
    
    DEFAULT_TOP_K: int = 5
    MAX_TOP_K: int = 20
    
    API_TITLE: str = "Semantic Document Search"
    API_VERSION: str = "1.0.0"
    
    class Config:
        env_file = ".env"
        case_sensitive = True
    
    @property
    def BASE_DIR(self) -> Path:
        return Path(__file__).parent.resolve()
    
    @property
    def DATASET_DIR(self) -> Path:
        path = self.BASE_DIR / "datasets"
        path.mkdir(exist_ok=True)
        return path
    
    @property
    def VECTORDB_DIR(self) -> Path:
        path = self.BASE_DIR / "vectordb"
        path.mkdir(exist_ok=True)
        return path
    
    @property
    def DATABASE_PATH(self) -> Path:
        return self.BASE_DIR / "app.db"

settings = Settings()