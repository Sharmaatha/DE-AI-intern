from pathlib import Path
from typing import List, Optional
import shutil
from datetime import datetime
import logging

from models import DocumentMetadata, DocumentType, DocumentChunk
from db.database import db
from services.pdf_processor import PDFProcessor
from services.embedding_service import embedding_service
from services.vector_store import vector_store
from config import settings

logger = logging.getLogger(__name__)


class DocumentManager:
    """Manages document lifecycle: add, remove, update"""
    
    def __init__(self):
        self.pdf_processor = PDFProcessor()
    
    def add_document(
        self, 
        pdf_path: str,
        document_type: DocumentType = DocumentType.FINANCIAL_REPORT,
        related_company_id: Optional[int] = None,
        related_person_id: Optional[int] = None
    ) -> DocumentMetadata:
        """
        Add a new document to the system
        """
        pdf_path = Path(pdf_path)
        
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")
        
        logger.info(f"Adding document: {pdf_path.name}")
        
        existing = self._get_document_by_filename(pdf_path.name)
        if existing:
            raise ValueError(f"Document {pdf_path.name} already exists")
    
        pdf_path = Path(pdf_path).resolve()
        dest_path = pdf_path  # Use original location

        file_size_mb = pdf_path.stat().st_size / (1024 * 1024)

        try:
            extracted = self.pdf_processor.extract_text_from_pdf(str(pdf_path))
            total_pages = extracted.get('total_pages', 0) if isinstance(extracted, dict) else 0
        
            doc_metadata = DocumentMetadata(
                filename=pdf_path.name,
                file_path=str(dest_path),
                document_type=document_type,
                upload_date=datetime.now(),
                total_pages=total_pages,
                file_size_mb=file_size_mb,
                related_company_id=related_company_id,
                related_person_id=related_person_id
            )
            
            doc_id = self._save_document_metadata(doc_metadata)
            doc_metadata.id = doc_id
            
            chunks = self.pdf_processor.process_pdf(str(dest_path), doc_id)
            
            self._save_chunks(chunks)
            
            chunk_texts = [chunk.text_content for chunk in chunks]
            embeddings = embedding_service.encode_batch(chunk_texts)
            
            chunk_indices = [chunk.chunk_index for chunk in chunks]
            start_idx, end_idx = vector_store.add_embeddings(
                embeddings, 
                doc_id, 
                chunk_indices
            )
            
            self._update_document_vector_indices(doc_id, start_idx, end_idx, len(chunks))
            doc_metadata.faiss_start_idx = start_idx
            doc_metadata.faiss_end_idx = end_idx
            doc_metadata.chunk_count = len(chunks)
            
            logger.info(f"Successfully added document {pdf_path.name} with {len(chunks)} chunks")
            return doc_metadata
        
        except Exception as e:
            logger.error(f"Error adding document: {e}")
            raise
    
    def remove_document(self, document_id: int) -> bool:
        """
        Remove a document from the system
        """
        logger.info(f"Removing document ID: {document_id}")
        
        doc = self._get_document_by_id(document_id)
        if not doc:
            raise ValueError(f"Document {document_id} not found")
        
        vector_store.delete_document_embeddings(document_id)
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM documents WHERE id = ?", (document_id,))
            conn.commit()
        
        file_path = Path(doc['file_path'])
        if file_path.exists():
            file_path.unlink()
        
        logger.info(f"Successfully removed document {document_id}")
        return True
    
    def list_documents(self) -> List[DocumentMetadata]:
        """Get all documents in the system"""
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM documents ORDER BY upload_date DESC")
            rows = cursor.fetchall()
        
        return [DocumentMetadata(**dict(row)) for row in rows]
    
    def get_document(self, document_id: int) -> Optional[DocumentMetadata]:
        """Get a specific document"""
        doc = self._get_document_by_id(document_id)
        if doc:
            return DocumentMetadata(**doc)
        return None
    
    def _get_document_by_id(self, document_id: int) -> Optional[dict]:
        """Internal method to get document as dict"""
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM documents WHERE id = ?", (document_id,))
            row = cursor.fetchone()
        
        return dict(row) if row else None
    
    def _get_document_by_filename(self, filename: str) -> Optional[dict]:
        """Internal method to get document by filename"""
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM documents WHERE filename = ?", (filename,))
            row = cursor.fetchone()
        
        return dict(row) if row else None
    
    def _save_document_metadata(self, doc: DocumentMetadata) -> int:
        """Save document metadata to database"""
        query = """
            INSERT INTO documents 
            (filename, file_path, document_type, upload_date, total_pages, 
             file_size_mb, related_company_id, related_person_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        return db.execute_write(query, (
            doc.filename,
            doc.file_path,
            doc.document_type.value,
            doc.upload_date.isoformat(),
            doc.total_pages,
            doc.file_size_mb,
            doc.related_company_id,
            doc.related_person_id
        ))
    
    def _save_chunks(self, chunks: List[DocumentChunk]):
        """Save document chunks to database"""
        with db.get_connection() as conn:
            cursor = conn.cursor()
            for chunk in chunks:
                cursor.execute("""
                    INSERT INTO document_chunks 
                    (document_id, chunk_index, page_numbers, text_content, chunk_size)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    chunk.document_id,
                    chunk.chunk_index,
                    ','.join(map(str, chunk.page_numbers)),
                    chunk.text_content,
                    chunk.chunk_size
                ))
            conn.commit()
    
    def _update_document_vector_indices(
        self, 
        document_id: int, 
        start_idx: int, 
        end_idx: int,
        chunk_count: int
    ):
        """Update document with FAISS vector indices"""
        query = """
            UPDATE documents 
            SET faiss_start_idx = ?, faiss_end_idx = ?, chunk_count = ?
            WHERE id = ?
        """
        db.execute_write(query, (start_idx, end_idx, chunk_count, document_id))

document_manager = DocumentManager()