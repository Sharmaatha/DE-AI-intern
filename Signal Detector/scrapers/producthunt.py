import requests
from typing import List, Dict, Any
import logging

from config import PRODUCTHUNT_API_TOKEN, API_TIMEOUT

logger = logging.getLogger(__name__)


class ProductHuntScraper:
    """Fetch Product Hunt posts with the CORRECT URL (node.url)."""

    def __init__(self):
        self.api_token = PRODUCTHUNT_API_TOKEN
        self.base_url = "https://api.producthunt.com/v2/api/graphql"
        self.timeout = API_TIMEOUT
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

    def get_products_by_date(self, date: str, limit: int = 10, after_cursor: str = None) -> Dict[str, Any]:

        query = """
        query($postedAfter: DateTime!, $postedBefore: DateTime!, $first: Int!, $after: String) {
          posts(
            postedAfter: $postedAfter,
            postedBefore: $postedBefore,
            first: $first,
            order: RANKING,
            after: $after
          ) {
            edges {
              cursor
              node {
                id
                name
                slug
                url             
                tagline
                description
                votesCount
                createdAt
                website
                thumbnail { url }
                topics { edges { node { name } } }
              }
            }
            pageInfo {
                endCursor
                hasNextPage
            }
          }
        }
        """

        posted_after = f"{date}T00:00:00Z"
        posted_before = f"{date}T23:59:59Z"

        variables = {
            "postedAfter": posted_after,
            "postedBefore": posted_before,
            "first": limit
        }

        if after_cursor:
            variables["after"] = after_cursor

        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={"query": query, "variables": variables},
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                logger.error(data["errors"])
                return {"products": [], "endCursor": None, "hasNextPage": False}

            posts_data = data.get("data", {}).get("posts", {})
            edges = posts_data.get("edges", [])
            page_info = posts_data.get("pageInfo", {})
            
            # Safely get endCursor and hasNextPage
            end_cursor = page_info.get("endCursor")
            has_next_page = page_info.get("hasNextPage", False)

            if not edges:
                # If no edges, it means no products for this page/cursor
                return {"products": [], "endCursor": end_cursor, "hasNextPage": False}

            products = []

            for edge in edges:
                node = edge["node"]

                product_hunt_url = node.get("url")   

                topics = [
                    t["node"]["name"]
                    for t in node.get("topics", {}).get("edges", [])
                ]

                products.append({
                    "id": node.get("id"),
                    "name": node.get("name"),
                    "slug": node.get("slug"),
                    "tagline": node.get("tagline"),
                    "description": node.get("description") or "",
                    "votes_count": node.get("votesCount"),
                    "created_at": node.get("createdAt"),
                    "website": node.get("website"),
                    "product_hunt_url": product_hunt_url,   
                    "thumbnail_url": node.get("thumbnail", {}).get("url"),
                    "topics": topics
                })

            return {
                "products": products,
                "endCursor": end_cursor,
                "hasNextPage": has_next_page
            }

        except Exception as exc:
            logger.error(f"ProductHunt API error: {exc}")
            return {"products": [], "endCursor": None, "hasNextPage": False}