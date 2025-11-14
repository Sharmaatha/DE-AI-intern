
import sys
from pathlib import Path
import logging

sys.path.append(str(Path(__file__).parent))

from services.document_manager import document_manager
from models import DocumentType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def setup_documents():
    """Add all PDF files from dataset subfolders"""
    
    pdf_directory = Path("datasets")  
    
    if not pdf_directory.exists():
        logger.error(f"Directory {pdf_directory} does not exist!")
        return
    
    pdf_files = list(pdf_directory.rglob("*.pdf")) 
    
    if not pdf_files:
        logger.warning("No PDF files found in datasets directory")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF files across all dataset folders")
    
    
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing [{i}/{len(pdf_files)}]: {pdf_path.name}")
            logger.info(f"{'='*60}")
            
            doc_metadata = document_manager.add_document(
                pdf_path=str(pdf_path),
                document_type=DocumentType.FINANCIAL_REPORT, 
                related_company_id=None,
                related_person_id=None
            )
            
            logger.info(f"✓ Successfully processed: {doc_metadata.filename}")
            logger.info(f"  - Total pages: {doc_metadata.total_pages}")
            logger.info(f"  - Chunks created: {doc_metadata.chunk_count}")
            logger.info(f"  - File size: {doc_metadata.file_size_mb:.2f} MB")
            
        except ValueError as e:
            logger.warning(f"✗ Skipping {pdf_path.name}: {e}")
        except Exception as e:
            logger.error(f"✗ Error processing {pdf_path.name}: {e}")
    
    logger.info(f"\n{'='*60}")
    logger.info("Setup complete!")
    logger.info(f"{'='*60}")
    
    documents = document_manager.list_documents()
    logger.info(f"Total documents in system: {len(documents)}")
    
    if documents:
        logger.info("\nDocuments:")
        for doc in documents:
            logger.info(f"  - {doc.filename} ({doc.chunk_count} chunks)")


if __name__ == "__main__":
    logger.info("Starting initial document setup...")
    setup_documents()
    logger.info("\nSetup script finished!")
    logger.info("\nYou can now start the  server with: python main.py")