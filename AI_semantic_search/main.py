from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse
from typing import Optional
import logging
import tempfile
from pathlib import Path

from models import (
    SearchQuery, SearchResponse, DocumentMetadata, 
    DocumentType, IndexStats
)
from services.document_manager import document_manager
from services.search_service import search_service
from services.vector_store import vector_store
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description="Semantic search engine for financial documents"
)

@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": settings.API_TITLE,
        "version": settings.API_VERSION
    }


@app.post("/documents/upload", response_model=DocumentMetadata)
async def upload_document(
    file: UploadFile = File(...),
    document_type: DocumentType = Form(DocumentType.FINANCIAL_REPORT),
    related_company_id: Optional[int] = Form(None),
    related_person_id: Optional[int] = Form(None)
):
    """
    Upload a new PDF document
    """
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        doc_metadata = document_manager.add_document(
            pdf_path=tmp_path,
            document_type=document_type,
            related_company_id=related_company_id,
            related_person_id=related_person_id
        )
        Path(tmp_path).unlink()
        
        return doc_metadata
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error uploading document: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload document")


@app.get("/documents", response_model=list[DocumentMetadata])
def list_documents():
    """Get all documents in the system"""
    try:
        return document_manager.list_documents()
    except Exception as e:
        logger.error(f"Error listing documents: {e}")
        raise HTTPException(status_code=500, detail="Failed to list documents")


@app.get("/documents/{document_id}", response_model=DocumentMetadata)
def get_document(document_id: int):
    """Get a specific document by ID"""
    try:
        doc = document_manager.get_document(document_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        return doc
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting document: {e}")
        raise HTTPException(status_code=500, detail="Failed to get document")


@app.delete("/documents/{document_id}")
def delete_document(document_id: int):
    """Delete a document from the system"""
    try:
        success = document_manager.remove_document(document_id)
        if not success:
            raise HTTPException(status_code=404, detail="Document not found")
        return {"message": "Document deleted successfully", "document_id": document_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete document")


@app.get("/documents/{document_id}/download")
def download_document(document_id: int):
    """Download a document PDF file"""
    try:
        doc = document_manager.get_document(document_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        
        file_path = Path(doc.file_path)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found on disk")
        
        return FileResponse(
            path=str(file_path),
            filename=doc.filename,
            media_type="application/pdf"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading document: {e}")
        raise HTTPException(status_code=500, detail="Failed to download document")

@app.post("/search", response_model=SearchResponse)
def search_documents(query: SearchQuery):
    """
    Search for documents semantically
    """
    try:
        return search_service.search(query)
    except Exception as e:
        import traceback
        logger.error(f"Error searching documents: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/stats", response_model=IndexStats)
def get_stats():
    """Get statistics about the system"""
    try:
        vector_stats = vector_store.get_stats()
        documents = document_manager.list_documents()
        
        return IndexStats(
            total_documents=len(documents),
            total_chunks=vector_stats['total_vectors'],
            index_size_mb=vector_stats['index_size_mb'],
            last_updated=max([doc.upload_date for doc in documents]) if documents else None
        )
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get stats")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)