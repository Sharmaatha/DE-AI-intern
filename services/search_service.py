from typing import List, Dict
from collections import defaultdict
import time
import logging
import re

from models import SearchQuery, SearchResult, SearchResponse
from services.embedding_service import embedding_service
from services.vector_store import vector_store
from db.database import db
from config import settings

logger = logging.getLogger(__name__)


class SearchService:
    """Handles semantic search operations with hybrid strategy"""
    
    def search(self, query: SearchQuery) -> SearchResponse:
        """
        Hybrid search with 4-step strategy:
        1. Exact Keyword Match (boost scores)
        2. Semantic Search (find similar)
        3. Minimum Threshold Filter 
        4. Re-rank by combined score
        """
        start_time = time.time()
        logger.info(f"Searching for: '{query.query}'")
        
        query_embedding = embedding_service.encode_query(query.query)
        
        vector_results = vector_store.search(
            query_embedding, 
            top_k=query.top_k * 5  
        )
        
        if not vector_results:
            return SearchResponse(
                query=query.query, 
                results=[], 
                total_results=0, 
                search_time_ms=(time.time() - start_time) * 1000
            )
        
        query_keywords = set(re.findall(r'\w+', query.query.lower()))
        enhanced_results = []
        
        for result in vector_results:
            chunk_text = self._get_chunk_text(
                result['document_id'], 
                result['chunk_index']
            )
            
            if not chunk_text:
                continue
            
            chunk_words = set(re.findall(r'\w+', chunk_text.lower()))
            
            matching_keywords = query_keywords.intersection(chunk_words)
            keyword_score = len(matching_keywords) / len(query_keywords) if query_keywords else 0
            
            semantic_score = result['similarity_score']
            
            combined_score = (
                settings.KEYWORD_BOOST_WEIGHT * keyword_score +
                settings.SEMANTIC_WEIGHT * semantic_score
            )
            
            enhanced_results.append({
                **result,
                'keyword_score': keyword_score,
                'semantic_score': semantic_score,
                'combined_score': combined_score,
                'has_exact_match': keyword_score > 0.5 
            })
        
        filtered_results = [
            r for r in enhanced_results 
            if r['combined_score'] >= settings.MINIMUM_SIMILARITY_THRESHOLD
        ]
        
        if not filtered_results:
            logger.warning(f"No results above threshold {settings.MINIMUM_SIMILARITY_THRESHOLD}")
            return SearchResponse(
                query=query.query, 
                results=[], 
                total_results=0, 
                search_time_ms=(time.time() - start_time) * 1000
            )
        
        filtered_results.sort(key=lambda x: x['combined_score'], reverse=True)
        
        doc_matches = self._group_by_document(filtered_results)
        
        search_results = []
        for doc_id, matches in doc_matches.items():
            doc_info = self._get_document_info(doc_id)
            
            if not doc_info:
                continue
            
            if query.document_type_filter:
                if doc_info['document_type'] != query.document_type_filter.value:
                    continue
            
            best_match = matches[0]
            snippet = self._get_chunk_text(doc_id, best_match['chunk_index'])
            
            avg_combined_score = sum(m['combined_score'] for m in matches) / len(matches)
            avg_keyword_score = sum(m['keyword_score'] for m in matches) / len(matches)
            avg_semantic_score = sum(m['semantic_score'] for m in matches) / len(matches)
            has_exact_matches = any(m['has_exact_match'] for m in matches)
            
            result = SearchResult(
                document_id=doc_id,
                filename=doc_info['filename'],
                file_path=doc_info['file_path'],
                document_type=doc_info['document_type'],
                relevance_score=avg_combined_score,  # Use combined score
                matched_chunks=[m['chunk_index'] for m in matches],
                matched_pages=self._get_matched_pages(doc_id, matches),
                snippet=snippet[:500] + "..." if len(snippet) > 500 else snippet
            )
            
            search_results.append(result)
            
            logger.debug(f"Doc {doc_id}: combined={avg_combined_score:.3f}, "
                        f"keyword={avg_keyword_score:.3f}, semantic={avg_semantic_score:.3f}, "
                        f"exact_match={has_exact_matches}")
        
        search_results.sort(key=lambda x: x.relevance_score, reverse=True)
        
        search_results = search_results[:query.top_k]
        
        elapsed_ms = (time.time() - start_time) * 1000
        
        logger.info(f"Found {len(search_results)} documents in {elapsed_ms:.2f}ms")
        
        return SearchResponse(
            query=query.query,
            results=search_results,
            total_results=len(search_results),
            search_time_ms=elapsed_ms
        )
    
    def _group_by_document(self, vector_results: List[Dict]) -> Dict[int, List[Dict]]:
        """
        Group vector search results by document ID
        """
        grouped = defaultdict(list)
        
        for result in vector_results:
            doc_id = result['document_id']
            grouped[doc_id].append(result)
        
        for doc_id in grouped:
            grouped[doc_id].sort(
                key=lambda x: x.get('combined_score', x.get('similarity_score', 0)), 
                reverse=True
            )
        
        return dict(grouped)
    
    def _get_document_info(self, document_id: int) -> Dict:
        """Get document metadata from database"""
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM documents WHERE id = ?", 
                (document_id,)
            )
            row = cursor.fetchone()
        
        return dict(row) if row else None
    
    def _get_chunk_text(self, document_id: int, chunk_index: int) -> str:
        """Get text content of a specific chunk"""
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT text_content FROM document_chunks 
                WHERE document_id = ? AND chunk_index = ?
            """, (document_id, chunk_index))
            row = cursor.fetchone()
        
        return row['text_content'] if row else ""
    
    def _get_matched_pages(self, document_id: int, matches: List[Dict]) -> List[int]:
        """Get unique page numbers from matched chunks"""
        pages = set()
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            for match in matches:
                cursor.execute("""
                    SELECT page_numbers FROM document_chunks 
                    WHERE document_id = ? AND chunk_index = ?
                """, (document_id, match['chunk_index']))
                row = cursor.fetchone()
                
                if row:
                    page_nums = [int(p) for p in row['page_numbers'].split(',')]
                    pages.update(page_nums)
        
        return sorted(list(pages))

search_service = SearchService()