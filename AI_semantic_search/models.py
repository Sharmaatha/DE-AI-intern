from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime
from enum import Enum


class DocumentType(str, Enum):
    FINANCIAL_REPORT = "financial_report"
    INVOICE = "invoice"
    STATEMENT = "statement"
    CONTRACT = "contract"
    OTHER = "other"


class DocumentMetadata(BaseModel):
    """Metadata for a document stored in the system"""
    model_config = ConfigDict(from_attributes=True)
    
    id: Optional[int] = None
    filename: str
    file_path: str
    document_type: DocumentType
    upload_date: datetime = Field(default_factory=datetime.now)
    total_pages: int
    file_size_mb: float
    
    # Vector DB references
    chunk_count: int = 0
    faiss_start_idx: Optional[int] = None 
    faiss_end_idx: Optional[int] = None   


class DocumentChunk(BaseModel):
    """Represents a chunk of a document"""
    document_id: int
    chunk_index: int
    page_numbers: List[int]
    text_content: str
    chunk_size: int = Field(description="Number of characters")
    faiss_vector_id: Optional[int] = None


class EmbeddingVector(BaseModel):
    """Embedding vector with metadata"""
    vector_id: int
    document_id: int
    chunk_index: int
    embedding: List[float]


class SearchQuery(BaseModel):
    """User search query"""
    query: str = Field(..., min_length=1, max_length=500)
    top_k: int = Field(default=5, ge=1, le=20)
    document_type_filter: Optional[DocumentType] = None


class SearchResult(BaseModel):
    """Search result containing document info"""
    document_id: int
    filename: str
    file_path: str
    document_type: DocumentType
    relevance_score: float
    matched_chunks: List[int] 
    matched_pages: List[int]  
    snippet: str = Field(description="Preview of matched content")


class SearchResponse(BaseModel):
    """Complete search response"""
    query: str
    results: List[SearchResult]
    total_results: int
    search_time_ms: float


class Company(BaseModel):
    """Company entity"""
    model_config = ConfigDict(from_attributes=True)
    
    id: Optional[int] = None
    name: str
    founded_year: Optional[int] = None
    industry: Optional[str] = None


class Person(BaseModel):
    """Person entity"""
    model_config = ConfigDict(from_attributes=True)
    
    id: Optional[int] = None
    name: str
    company_id: Optional[int] = None
    role: Optional[str] = None
    joined_date: Optional[str] = None


class IndexStats(BaseModel):
    """Statistics about the vector index"""
    total_documents: int
    total_chunks: int
    index_size_mb: float
    last_updated: datetime