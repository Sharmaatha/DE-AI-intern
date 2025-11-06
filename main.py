#fetch data from producthunt api and store in postgres database
import requests
import psycopg2
from psycopg2.extras import execute_batch
from fastapi import FastAPI
from datetime import datetime
import json
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

@app.on_event("startup")
def startup_event():
    """Initialize database on app startup"""
    init_database()

PH_API_URL = "https://api.producthunt.com/v2/api/graphql"
PH_TOKEN = os.getenv("PH_TOKEN")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5433)), 
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}


QUERY = """
query($first:Int,$order:PostsOrder,$postedAfter:DateTime,$postedBefore:DateTime){
  posts(order:$order,first:$first,postedAfter:$postedAfter,postedBefore:$postedBefore){
    edges{
      node{
        name
        tagline
        description
        url
        website
        votesCount
        commentsCount
        createdAt
        featuredAt
        thumbnail{
          url
        }
        media{
          url
          type
        }
        topics{
          edges{
            node{
              name
            }
          }
        }
      }
    }
  }
}
"""

def fetch_products(first=20, order="RANKING", posted_after=None, posted_before=None):
    headers = {
        "Authorization": PH_TOKEN, 
        "Content-Type": "application/json"
    }
    
    payload = {
        "query": QUERY,
        "variables": {
            "first": first,
            "order": order,
            "postedAfter": posted_after,
            "postedBefore": posted_before
        }
    }
    
    response = requests.post(PH_API_URL, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def extract_products(data):
    products = []
    
    for edge in data['data']['posts']['edges']:
        node = edge['node']
        topics = [t['node']['name'] for t in node['topics']['edges']]
        
        thumbnail_url = node['thumbnail']['url'] if node.get('thumbnail') else None
        
        media_list = []
        if node.get('media'):
            for m in node['media']:
                media_list.append({
                    'url': m.get('url'),
                    'type': m.get('type')
                })
        
        product = {
            'name': node['name'],
            'tagline': node['tagline'],
            'description': node.get('description'),
            'product_url': node['url'],
            'website': node.get('website'),
            'thumbnail': thumbnail_url,
            'votes_count': node.get('votesCount', 0),
            'comments_count': node.get('commentsCount', 0),
            'created_at': node['createdAt'],
            'featured_at': node.get('featuredAt'),
            'topics': topics,
            'media': json.dumps(media_list) if media_list else None
        }
        products.append(product)
    
    return products

def init_database():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            tagline TEXT,
            description TEXT,
            product_url TEXT,
            website TEXT,
            thumbnail TEXT,
            votes_count INTEGER,
            comments_count INTEGER,
            created_at TIMESTAMP,
            featured_at TIMESTAMP,
            topics TEXT[],
            media JSONB,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized with all columns")

def insert_products(products):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO products (name, tagline, description, product_url, website, 
                            thumbnail, votes_count, comments_count, created_at, 
                            featured_at, topics, media)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    data = [
        (p['name'], p['tagline'], p['description'], p['product_url'], 
         p['website'], p['thumbnail'], p['votes_count'], p['comments_count'],
         p['created_at'], p['featured_at'], p['topics'], p['media'])
        for p in products
    ]
    
    execute_batch(cur, insert_query, data)
    conn.commit()
    
    print(f"Inserted {len(products)} products")
    cur.close()
    conn.close()

@app.get("/")
def root():
    return {"message": "Product Hunt Complete Data API"}

@app.post("/fetch-and-store")
def fetch_and_store(first: int = 20, order: str = "RANKING", posted_after: str = None, posted_before: str = None):
    try:
        if posted_after:
            posted_after = posted_after.strip()
        if posted_before:
            posted_before = posted_before.strip()
        
        print(f"\n=== Fetching products: first={first}, order={order}, after={posted_after}, before={posted_before} ===")
        data = fetch_products(first, order, posted_after, posted_before)
        
        if 'errors' in data:
            print(f"API ERROR: {data['errors']}")
            return {
                "status": "error",
                "message": "Product Hunt API returned errors",
                "errors": data['errors']
            }
        
        print(f"API Response Keys: {data.keys()}")
        
        products = extract_products(data)
        print(f"Extracted {len(products)} products")
        
        if len(products) == 0:
            return {
                "status": "warning",
                "products_stored": 0,
                "message": "No products found",
                "raw_response": data
            }
        
        insert_products(products)
        print(f"Successfully stored {len(products)} products in database")
        
        return {
            "status": "success",
            "products_stored": len(products),
            "sample_data": products[0] if products else None
        }
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

@app.get("/products")
def get_products(limit: int = 100):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute(f"SELECT * FROM products ORDER BY fetched_at DESC LIMIT {limit}")
    rows = cur.fetchall()
    
    products = []
    for row in rows:
        products.append({
            "id": row[0],
            "name": row[1],
            "tagline": row[2],
            "description": row[3],
            "product_url": row[4],
            "website": row[5],
            "thumbnail": row[6],
            "votes_count": row[7],
            "comments_count": row[8],
            "created_at": str(row[9]),
            "featured_at": str(row[10]),
            "topics": row[11],
            "media": row[12],
            "fetched_at": str(row[13])
        })
    
    cur.close()
    conn.close()
    
    return {"count": len(products), "products": products}

if __name__ == "__main__":
    import uvicorn
    init_database()
    uvicorn.run(app, host="0.0.0.0", port=8000)