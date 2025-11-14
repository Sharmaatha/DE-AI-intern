from PyPDF2 import PdfReader
from pathlib import Path
from typing import List, Dict
import logging
from models import DocumentChunk
from config import settings

logger = logging.getLogger(__name__)


class PDFProcessor:
    """Processes PDF files and extracts text with chunking"""
    
    def __init__(self, chunk_size: int = None, chunk_overlap: int = None):
        self.chunk_size = chunk_size or settings.CHUNK_SIZE
        self.chunk_overlap = chunk_overlap or settings.CHUNK_OVERLAP
    
    def extract_text_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extract text from PDF file page by page
        """
        try:
            reader = PdfReader(pdf_path)
            pages = []
            
            for page_num, page in enumerate(reader.pages, start=1):
                text = page.extract_text()
                if text.strip():  
                    pages.append({
                        'page_number': page_num,
                        'text': text.strip()
                    })
            
            logger.info(f"Extracted {len(pages)} pages from {Path(pdf_path).name}")
            
            return {
                'pages': pages,
                'total_pages': len(reader.pages)
            }
        
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
            raise
    
    def chunk_text(self, pages: List[Dict], document_id: int) -> List[DocumentChunk]:
        """
        Split extracted pages into chunks with overlap
        """
        chunks = []
        chunk_index = 0
        current_chunk = ""
        current_pages = []
        
        for page in pages:
            page_num = page['page_number']
            page_text = page['text']
            
            if current_chunk:
                current_chunk += "\n\n"
            current_chunk += page_text
            current_pages.append(page_num)
            
            while len(current_chunk) >= self.chunk_size:
                break_point = self._find_break_point(
                    current_chunk, 
                    self.chunk_size
                )
                
                chunk_text = current_chunk[:break_point].strip()
                
                if chunk_text:
                    chunks.append(DocumentChunk(
                        document_id=document_id,
                        chunk_index=chunk_index,
                        page_numbers=current_pages.copy(),
                        text_content=chunk_text,
                        chunk_size=len(chunk_text)
                    ))
                    chunk_index += 1
                

                overlap_start = max(0, break_point - self.chunk_overlap)
                current_chunk = current_chunk[overlap_start:]
                
                if current_pages:
                    current_pages = [current_pages[-1]]
    
        if current_chunk.strip():
            chunks.append(DocumentChunk(
                document_id=document_id,
                chunk_index=chunk_index,
                page_numbers=current_pages,
                text_content=current_chunk.strip(),
                chunk_size=len(current_chunk.strip())
            ))
        
        logger.info(f"Created {len(chunks)} chunks for document {document_id}")
        return chunks
    
    def _find_break_point(self, text: str, target_size: int) -> int:
        """
        Find a natural breaking point in text near target_size
        """
        if len(text) <= target_size:
            return len(text)
    
        para_break = text.rfind('\n\n', 0, target_size)
        if para_break > target_size * 0.7: 
            return para_break
        
        sentence_breaks = ['. ', '! ', '? ']
        best_break = -1
        for delimiter in sentence_breaks:
            pos = text.rfind(delimiter, 0, target_size)
            if pos > best_break:
                best_break = pos + len(delimiter)
        
        if best_break > target_size * 0.7:
            return best_break
        
        word_break = text.rfind(' ', 0, target_size)
        if word_break > 0:
            return word_break
        return target_size
    
    def process_pdf(self, pdf_path: str, document_id: int) -> List[DocumentChunk]:
        """
        Complete processing pipeline for a PDF
        """
        # Extract text
        extracted = self.extract_text_from_pdf(pdf_path)
        
        # Create chunks
        chunks = self.chunk_text(extracted['pages'], document_id)
        
        return chunks