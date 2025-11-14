import faiss
import numpy as np
import json
from pathlib import Path
from typing import List, Tuple, Dict, Optional
import logging
from config import settings

logger = logging.getLogger(__name__)


class VectorStore:
    """Manages FAISS vector index for document embeddings"""
    
    def __init__(self):
        self.index_path = settings.VECTORDB_DIR / settings.FAISS_INDEX_FILE
        self.metadata_path = settings.VECTORDB_DIR / settings.FAISS_METADATA_FILE
        self.dimension = settings.EMBEDDING_DIMENSION
        self.index: Optional[faiss.IndexFlatL2] = None
        self.metadata: List[Dict] = []
        
        self._load_or_create_index()
    
    def _load_or_create_index(self):
        """Load existing index or create new one"""
        if self.index_path.exists():
            self._load_index()
        else:
            self._create_index()
    
    def _create_index(self):
        """Create a new FAISS index"""
        logger.info(f"Creating new FAISS index with dimension {self.dimension}")
        self.index = faiss.IndexFlatL2(self.dimension)  # L2 distance
        self.metadata = []
        self._save_index()
    
    def _load_index(self):
        """Load existing FAISS index"""
        try:
            logger.info(f"Loading FAISS index from {self.index_path}")
            self.index = faiss.read_index(str(self.index_path))
        
            if self.metadata_path.exists():
                with open(self.metadata_path, 'r') as f:
                    self.metadata = json.load(f)
            
            logger.info(f"Loaded index with {self.index.ntotal} vectors")
        except Exception as e:
            logger.error(f"Error loading index: {e}")
            self._create_index()
    
    def _save_index(self):
        """Save FAISS index and metadata to disk"""
        try:
            faiss.write_index(self.index, str(self.index_path))
            
            with open(self.metadata_path, 'w') as f:
                json.dump(self.metadata, f, indent=2)
            
            logger.info(f"Saved index with {self.index.ntotal} vectors")
        except Exception as e:
            logger.error(f"Error saving index: {e}")
            raise
    
    def add_embeddings(
        self, 
        embeddings: np.ndarray, 
        document_id: int,
        chunk_indices: List[int]
    ) -> Tuple[int, int]:
        """
        Add embeddings to the index
        """
        if embeddings.shape[0] != len(chunk_indices):
            raise ValueError("Number of embeddings must match number of chunk indices")
        
        start_idx = self.index.ntotal
        self.index.add(embeddings.astype('float32'))
        
        for i, chunk_idx in enumerate(chunk_indices):
            self.metadata.append({
                'vector_id': start_idx + i,
                'document_id': document_id,
                'chunk_index': chunk_idx
            })
        
        end_idx = self.index.ntotal - 1
        self._save_index()
        
        logger.info(f"Added {len(chunk_indices)} embeddings for document {document_id}")
        return start_idx, end_idx
    
    def search(
        self, 
        query_embedding: np.ndarray, 
        top_k: int = 5
    ) -> List[Dict]:
        """
        Search for similar vectors
        """
        if self.index.ntotal == 0:
            logger.warning("Index is empty, no results to return")
            return []
        
        query_vector = query_embedding.reshape(1, -1).astype('float32')
        
        distances, indices = self.index.search(query_vector, min(top_k, self.index.ntotal))
        
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < len(self.metadata):  
                meta = self.metadata[idx].copy()
                meta['distance'] = float(dist)
                meta['similarity_score'] = self._distance_to_similarity(float(dist))
                results.append(meta)
        
        return results
    
    def _distance_to_similarity(self, distance: float) -> float:
        """
        Convert L2 distance to similarity score (0-1)
        """
        return 1.0 / (1.0 + distance)
    
    def delete_document_embeddings(self, document_id: int) -> bool:
        """
        Remove embeddings for a document 
        """
        keep_indices = [
            i for i, meta in enumerate(self.metadata) 
            if meta['document_id'] != document_id
        ]
        
        if len(keep_indices) == len(self.metadata):
            logger.warning(f"No embeddings found for document {document_id}")
            return False
        
        logger.info(f"Rebuilding index after removing document {document_id}")
        
        all_vectors = []
        for i in keep_indices:
            vector = self.index.reconstruct(i)
            all_vectors.append(vector)
        
        self._create_index()
        
        if all_vectors:
            embeddings = np.array(all_vectors)
            self.index.add(embeddings.astype('float32'))
            self.metadata = [self.metadata[i] for i in keep_indices]
        
            for i, meta in enumerate(self.metadata):
                meta['vector_id'] = i
        
        self._save_index()
        logger.info(f"Successfully removed document {document_id} embeddings")
        return True
    
    def get_stats(self) -> Dict:
        """Get statistics about the index"""
        return {
            'total_vectors': self.index.ntotal,
            'dimension': self.dimension,
            'index_size_mb': self.index_path.stat().st_size / (1024 * 1024) if self.index_path.exists() else 0
        }
    
    def reset_index(self):
        """Reset the entire index (use with caution)"""
        logger.warning("Resetting entire FAISS index")
        self._create_index()


vector_store = VectorStore()