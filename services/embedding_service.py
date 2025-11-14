from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List, Union
import logging
from config import settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating embeddings from text"""
    
    _instance = None
    _model = None
    
    def __new__(cls):
        """Singleton pattern to avoid loading model multiple times"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._model is None:
            logger.info(f"Loading embedding model: {settings.EMBEDDING_MODEL}")
            self._model = SentenceTransformer(settings.EMBEDDING_MODEL)
            logger.info("Embedding model loaded successfully")
    
    @property
    def model(self):
        return self._model
    
    @property
    def dimension(self) -> int:
        """Get the dimension of embeddings"""
        return settings.EMBEDDING_DIMENSION
    
    def encode_text(self, text: str) -> np.ndarray:
        """
        Generate embedding for a single text
        """
        try:
            embedding = self._model.encode(text, convert_to_numpy=True)
            return embedding
        except Exception as e:
            logger.error(f"Error encoding text: {e}")
            raise
    
    def encode_batch(self, texts: List[str], batch_size: int = 32) -> np.ndarray:
        """
        Generate embeddings for multiple texts efficiently
        """
        try:
            logger.info(f"Encoding batch of {len(texts)} texts")
            embeddings = self._model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=len(texts) > 100,
                convert_to_numpy=True
            )
            logger.info(f"Successfully encoded {len(texts)} texts")
            return embeddings
        except Exception as e:
            logger.error(f"Error encoding batch: {e}")
            raise
    
    def encode_query(self, query: str) -> np.ndarray:
        """
        Generate embedding for search query
        """
        return self.encode_text(query)
    
    def similarity(self, embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """
        Calculate cosine similarity between two embeddings
        """
        return np.dot(embedding1, embedding2) / (
            np.linalg.norm(embedding1) * np.linalg.norm(embedding2)
        )
    
embedding_service = EmbeddingService()