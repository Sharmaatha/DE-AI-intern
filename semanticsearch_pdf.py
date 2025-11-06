# semanticsearch with pdf files and reranking
import os
import fitz
import pickle
import numpy as np
from typing import List, Dict
from tqdm import tqdm
from sentence_transformers import SentenceTransformer, CrossEncoder
import faiss


class SemanticSearchEngine:
    def __init__(self, 
                 model_name: str = "multi-qa-mpnet-base-dot-v1",
                 use_reranker: bool = True):
        print(f"Loading embedding model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.dimension = self.model.get_sentence_embedding_dimension()

        print("Initializing FAISS index...")
        self.index = faiss.IndexFlatIP(self.dimension)

        self.documents, self.metadatas, self.ids = [], [], []
        self.use_reranker = use_reranker
        if use_reranker:
            print("Loading reranker model (cross-encoder)...")
            self.reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

        print("Search engine initialized successfully.\n")

    def extract_text_from_pdf(self, pdf_path: str) -> List[Dict[str, str]]:
        chunks = []
        try:
            doc = fitz.open(pdf_path)
            filename = os.path.basename(pdf_path)

            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text().strip()
                if not text:
                    continue
                chunks.append({
                    'text': text,
                    'source': filename,
                    'page': page_num + 1,
                    'id': f"{filename}_page_{page_num + 1}"
                })
            doc.close()
        except Exception as e:
            print(f"Error processing {pdf_path}: {e}")
        return chunks

    def chunk_text(self, text: str, chunk_size: int = 200, overlap: int = 50) -> List[str]:
        words = text.split()
        chunks = []
        for i in range(0, len(words), chunk_size - overlap):
            chunk = " ".join(words[i:i + chunk_size])
            if chunk:
                chunks.append(chunk)
        return chunks


    def add_pdfs(self, pdf_paths: List[str], use_chunking: bool = True):
        all_texts, all_metadatas, all_ids = [], [], []

        for path in pdf_paths:
            if not os.path.exists(path):
                print(f"Skipping missing file: {path}")
                continue

            for chunk in self.extract_text_from_pdf(path):
                if use_chunking:
                    for i, small_chunk in enumerate(self.chunk_text(chunk["text"])):
                        all_texts.append(small_chunk)
                        all_metadatas.append({
                            "source": chunk["source"],
                            "page": chunk["page"],
                            "chunk": i + 1
                        })
                        all_ids.append(f"{chunk['id']}_chunk_{i+1}")
                else:
                    all_texts.append(chunk["text"])
                    all_metadatas.append({
                        "source": chunk["source"],
                        "page": chunk["page"]
                    })
                    all_ids.append(chunk["id"])

        if not all_texts:
            print("No documents found for indexing.")
            return

        print(f"\nGenerating embeddings for {len(all_texts)} text chunks...")
        embeddings = self.model.encode(
            all_texts, 
            show_progress_bar=True, 
            convert_to_numpy=True, 
            normalize_embeddings=True
        )

        print("Adding vectors to FAISS index...")
        self.index.add(embeddings.astype('float32'))

        self.documents.extend(all_texts)
        self.metadatas.extend(all_metadatas)
        self.ids.extend(all_ids)

        print(f"Indexed {len(all_texts)} chunks successfully.\n")

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        if self.index.ntotal == 0:
            print("Index is empty — add PDFs first.")
            return []

        print(f"\nSearching for: '{query}'")
        query_emb = self.model.encode(
            [query], 
            convert_to_numpy=True, 
            normalize_embeddings=True
        ).astype("float32")

        distances, indices = self.index.search(query_emb, min(top_k * 3, self.index.ntotal))

        initial_results = []
        for i, idx in enumerate(indices[0]):
            if idx == -1:
                continue
            initial_results.append({
                "text": self.documents[idx],
                "metadata": self.metadatas[idx],
                "similarity_score": float(distances[0][i]),
                "id": self.ids[idx]
            })

        if self.use_reranker:
            pairs = [(query, r["text"]) for r in initial_results]
            scores = self.reranker.predict(pairs)
            for j, s in enumerate(scores):
                initial_results[j]["rerank_score"] = float(s)
            initial_results = sorted(initial_results, key=lambda x: x["rerank_score"], reverse=True)

        return initial_results[:top_k]

    def display_results(self, results: List[Dict]):
        if not results:
            print("No matching results found.\n")
            return

        print("\n" + "="*90)
        print(f"Top {len(results)} results:")
        print("="*90)
        for i, res in enumerate(results, 1):
            meta = res["metadata"]
            print(f"\nResult #{i}")
            print(f"Source: {meta.get('source')} | Page: {meta.get('page')} | "
                  f"Similarity: {res['similarity_score']:.4f}" +
                  (f" | Rerank: {res.get('rerank_score', 0):.4f}" if "rerank_score" in res else ""))
            print(f"Text snippet:\n{res['text'][:300]}...")
            print("-"*90)


    def save_index(self, filename="semantic_index.pkl"):
        faiss.write_index(self.index, filename.replace(".pkl", ".faiss"))
        with open(filename, "wb") as f:
            pickle.dump({
                "documents": self.documents,
                "metadatas": self.metadatas,
                "ids": self.ids,
                "dimension": self.dimension
            }, f)
        print(f"Saved index to {filename}")

    def load_index(self, filename="semantic_index.pkl"):
        self.index = faiss.read_index(filename.replace(".pkl", ".faiss"))
        with open(filename, "rb") as f:
            data = pickle.load(f)
        self.documents, self.metadatas, self.ids = data["documents"], data["metadatas"], data["ids"]
        print("Index loaded successfully.")


def create_sample_pdfs(output_dir="samplee_pdfs"):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    sample_docs = {
        "finance.pdf": "Annual revenue grew by 16% in 2024 with total revenue of $27M.",
        "hr.pdf": "Employees receive 20 paid vacation days and 16 weeks of parental leave.",
        "security.pdf": "Passwords must have 12 characters with symbols, numbers, and letters.",
        "product.pdf": "Our analytics platform uses FastAPI and React, supporting 1M events/sec."
    }

    files = []
    for name, text in sample_docs.items():
        path = os.path.join(output_dir, name)
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((50, 50), text)
        doc.save(path)
        doc.close()
        files.append(path)
    return files


def get_all_pdfs_from_directory(directory: str) -> List[str]:
    pdf_files = []
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            if filename.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(directory, filename))
    return pdf_files


def main():
    print("\n" + "="*80)
    print("Enhanced Semantic Search (with Reranking)")
    print("="*80)

    pdfs = create_sample_pdfs()
    
    additional_pdfs = get_all_pdfs_from_directory("samplee_pdfs")
    all_pdfs = list(set(pdfs + additional_pdfs))

    engine = SemanticSearchEngine(model_name="multi-qa-mpnet-base-dot-v1", use_reranker=True)
    engine.add_pdfs(all_pdfs)

    test_queries = [
        "What is the company's revenue?",
        "Tell me about vacation policy",
        "Password requirements for employees",
        "How fast is the analytics system?",
        "How much MMS was charged on Exxon?"
    ]

    print("\n" + "="*80)
    print("Running test queries...")
    print("="*80)
    for query in test_queries:
        results = engine.search(query, top_k=3)
        engine.display_results(results)

    print("\n" + "="*80)
    print("Test queries completed. Now you can search interactively.")
    print("="*80)
    
    while True:
        user_query = input("\nSearch query (or type 'exit' to quit): ").strip()
        
        if user_query.lower() in ['exit', 'quit', 'q']:
            print("Exiting search engine. Goodbye!")
            break
        
        if not user_query:
            print("Please enter a valid query.")
            continue
        
        results = engine.search(user_query, top_k=3)
        engine.display_results(results)


if __name__ == "__main__":
    main()