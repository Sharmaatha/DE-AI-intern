from fastapi import FastAPI
from loader import load_all_handlers

app = FastAPI()

@app.get("/fetch_all_handlers")
def fetch_all_handlers():
    handlers = ["elonmusk", "taylorswift13"] #fetch from db
    tweets = load_all_handlers(maxItems=1, handlers=handlers, use_static_file=False)
    return {"ok": True, "tweets": tweets}

@app.get("/fetch_from_file")
def fetch_from_file():
    tweets = load_all_handlers(maxItems=2,use_static_file=True)
    return {"ok": True, "tweets": tweets}





#http://127.0.0.1:8000/fetch_all_handlers
#python -m uvicorn main:app --reload