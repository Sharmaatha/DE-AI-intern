#semantic search producthunt data from postgres database 
import os
import pickle
import numpy as np
from typing import List, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer, CrossEncoder
from dotenv import load_dotenv
import faiss
import json
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

class ProductHuntSemanticSearch:
    def __init__(self, 
                 model_name: str = "multi-qa-mpnet-base-dot-v1",
                 use_reranker: bool = True):
        print(f"Loading embedding model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.dimension = self.model.get_sentence_embedding_dimension()

        print("Initializing FAISS index...")
        self.index = faiss.IndexFlatIP(self.dimension)

        self.products = []
        self.product_ids = []
        self.use_reranker = use_reranker
        
        if use_reranker:
            print("Loading reranker model (cross-encoder)...")
            self.reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

        print("Search engine initialized successfully.\n")

    def get_db_connection(self):
        return psycopg2.connect(**DB_CONFIG)

    def fetch_products_from_db(self):
        print("Fetching products from PostgreSQL database...")
        conn = self.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT id, name, tagline, description, product_url, website, 
                   thumbnail, votes_count, comments_count, created_at, 
                   featured_at, topics, media
            FROM products
            ORDER BY id
        """)
        
        products = cursor.fetchall()
        cursor.close()
        conn.close()
        
        print(f"Fetched {len(products)} products from database.\n")
        return products

    def prepare_searchable_text(self, product: Dict) -> str:
        name = product.get('name', '')
        tagline = product.get('tagline', '')
        description = product.get('description', '')
        topics = product.get('topics', [])
        
        if isinstance(topics, str):
            topics = topics.replace('{', '').replace('}', '').split(',')
        
        topics_text = ' '.join(topics) if topics else ''
        
        combined_text = f"{name} {tagline} {description} {topics_text}".strip()
        return combined_text

    def index_products(self):
        products = self.fetch_products_from_db()
        
        if not products:
            print("No products found in database.")
            return

        texts_to_embed = []
        
        for product in products:
            searchable_text = self.prepare_searchable_text(product)
            texts_to_embed.append(searchable_text)
            self.products.append(dict(product))
            self.product_ids.append(product['id'])

        print(f"Generating embeddings for {len(texts_to_embed)} products...")
        embeddings = self.model.encode(
            texts_to_embed, 
            show_progress_bar=True, 
            convert_to_numpy=True, 
            normalize_embeddings=True
        )

        print("Adding vectors to FAISS index...")
        self.index.add(embeddings.astype('float32'))

        print(f"Indexed {len(texts_to_embed)} products successfully.\n")

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        if self.index.ntotal == 0:
            print("Index is empty. Please index products first.")
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
            
            product = self.products[idx]
            searchable_text = self.prepare_searchable_text(product)
            
            initial_results.append({
                "product": product,
                "searchable_text": searchable_text,
                "similarity_score": float(distances[0][i])
            })

        if self.use_reranker:
            pairs = [(query, r["searchable_text"]) for r in initial_results]
            scores = self.reranker.predict(pairs)
            for j, s in enumerate(scores):
                initial_results[j]["rerank_score"] = float(s)
            initial_results = sorted(initial_results, key=lambda x: x["rerank_score"], reverse=True)

        return initial_results[:top_k]

    def display_results(self, results: List[Dict]):
        if not results:
            print("No matching results found.\n")
            return

        print("\n" + "="*100)
        print(f"Top {len(results)} results:")
        print("="*100)
        
        for i, res in enumerate(results, 1):
            product = res["product"]
            
            print(f"\n[Result #{i}]")
            print(f"Name: {product.get('name', 'N/A')}")
            print(f"Tagline: {product.get('tagline', 'N/A')}")
            print(f"Description: {product.get('description', 'N/A')[:200]}..." if product.get('description') else "Description: N/A")
            print(f"Product URL: {product.get('product_url', 'N/A')}")
            print(f"Website: {product.get('website', 'N/A')}")
            print(f"Votes: {product.get('votes_count', 0)} | Comments: {product.get('comments_count', 0)}")
            
            topics = product.get('topics', [])
            if isinstance(topics, str):
                topics = topics.replace('{', '').replace('}', '')
            print(f"Topics: {topics}")
            
            print(f"Similarity: {res['similarity_score']:.4f}" +
                  (f" | Rerank: {res.get('rerank_score', 0):.4f}" if "rerank_score" in res else ""))
            print("-"*100)

    def save_index(self, filename="producthunt_index.pkl"):
        faiss.write_index(self.index, filename.replace(".pkl", ".faiss"))
        with open(filename, "wb") as f:
            pickle.dump({
                "products": self.products,
                "product_ids": self.product_ids,
                "dimension": self.dimension
            }, f)
        print(f"Index saved to {filename}")

    def load_index(self, filename="producthunt_index.pkl"):
        self.index = faiss.read_index(filename.replace(".pkl", ".faiss"))
        with open(filename, "rb") as f:
            data = pickle.load(f)
        self.products = data["products"]
        self.product_ids = data["product_ids"]
        print("Index loaded successfully.")


def main():
    print("\n" + "="*100)
    print("ProductHunt Semantic Search Engine")
    print("="*100)

    engine = ProductHuntSemanticSearch(model_name="multi-qa-mpnet-base-dot-v1", use_reranker=True)
    
    engine.index_products()

    test_queries = [
        "AI tools for developers",
        "patent search and intellectual property",
        "productivity apps for teams",
        "design and creative tools"
    ]

    print("\n" + "="*100)
    print("Running test queries...")
    print("="*100)
    
    for query in test_queries:
        results = engine.search(query, top_k=3)
        engine.display_results(results)

    print("\n" + "="*100)
    print("Test queries completed. Now you can search interactively.")
    print("="*100)
    
    while True:
        user_query = input("\nSearch query (or type 'exit' to quit): ").strip()
        
        if user_query.lower() in ['exit', 'quit', 'q']:
            print("Exiting search engine. Goodbye!")
            break
        
        if not user_query:
            print("Please enter a valid query.")
            continue
        
        results = engine.search(user_query, top_k=5)
        engine.display_results(results)


if __name__ == "__main__":
    main()