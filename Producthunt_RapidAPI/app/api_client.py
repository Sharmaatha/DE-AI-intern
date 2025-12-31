"""
ProductHunt API Client (Production-Grade)
"""

from datetime import datetime
import asyncio
import json
import time
import logging
import requests
from collections import deque

logger = logging.getLogger(__name__)


class ProductHuntAPIClient:
    def __init__(self, api_key: str):
        self.base_url = "https://producthunt-api.p.rapidapi.com"
        self.headers = {
            'X-Rapidapi-Key': api_key,
            'X-Rapidapi-Host': 'producthunt-api.p.rapidapi.com'
        }
        self.cache = {}
        self.cache_ttl = 3600
        self.rate_limit_remaining = None
        self.rate_limit_reset = None

    def _parse_rate_limit_headers(self, response):
        """Parse rate limit headers"""
        headers = response.headers
        remaining_headers = ['X-RateLimit-Remaining', 'X-Rate-Limit-Remaining']

        for header in remaining_headers:
            if header in headers:
                try:
                    self.rate_limit_remaining = int(headers[header])
                except (ValueError, TypeError):
                    pass

        return headers.get('Retry-After')

    def _exponential_backoff(self, attempt: int, base_delay: float = 2.0):
        """Calculate exponential backoff"""
        import random
        delay = min(base_delay * (2 ** attempt), 60.0)
        jitter = delay * 0.1
        return delay + random.uniform(-jitter, jitter)

    def _check_cache(self, cache_key: str):
        """Check cache"""
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                logger.info(f"Cache hit: {cache_key}")
                return data
            del self.cache[cache_key]
        return None

    def _update_cache(self, cache_key: str, data: dict):
        """Update cache"""
        self.cache[cache_key] = (data, time.time())

    async def _make_request(self, endpoint: str, params: dict, max_retries: int = 5):
        """Make async API request with retry logic"""
        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        cached = self._check_cache(cache_key)
        if cached:
            return cached

        for attempt in range(max_retries):
            try:
                # Use asyncio to run sync request in executor
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(endpoint, headers=self.headers, params=params, timeout=30)
                )

                retry_after = self._parse_rate_limit_headers(response)

                if response.status_code == 429:
                    delay = float(retry_after) if retry_after else self._exponential_backoff(attempt)
                    logger.warning(f"Rate limit hit. Waiting {delay:.2f}s")
                    await asyncio.sleep(delay)
                    continue

                if response.status_code >= 400:
                    if attempt < max_retries - 1:
                        delay = self._exponential_backoff(attempt)
                        await asyncio.sleep(delay)
                        continue
                    response.raise_for_status()

                data = response.json()
                self._update_cache(cache_key, data)
                return data

            except Exception as e:
                logger.error(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(self._exponential_backoff(attempt))
                else:
                    raise

        raise Exception(f"Max retries exceeded")

    async def check_task_status(self, task_id: str):
        """Check background task status"""
        endpoint = f"{self.base_url}/status/{task_id}"
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(endpoint, headers=self.headers, timeout=30)
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.warning(f"Status check error: {e}")
        return {'status': 'unknown'}

    async def get_task_results(self, task_id: str):
        """Get results from completed task"""
        endpoint = f"{self.base_url}/results/{task_id}"
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(endpoint, headers=self.headers, timeout=30)
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.warning(f"Results fetch error: {e}")
        return {'error': str(e)}

    async def wait_for_task(self, task_id: str, max_wait: int = 120):
        """Wait for background task completion"""
        logger.info(f"Waiting for task: {task_id}")
        elapsed = 0
        check_interval = 10

        while elapsed < max_wait:
            status_data = await self.check_task_status(task_id)
            task_status = status_data.get('status', 'unknown')

            if task_status == 'completed':
                results = await self.get_task_results(task_id)
                if 'error' not in results:
                    return results
                return None
            elif task_status == 'failed':
                return None

            await asyncio.sleep(check_interval)
            elapsed += check_interval

        return None

    async def get_launches_by_date(self, date_string: str, page: int = 1, limit: int = 100):
        """Get launches for a specific date"""
        date_obj = datetime.strptime(date_string, '%Y-%m-%d')

        endpoint = f"{self.base_url}/products/daily"
        params = {
            'year': date_obj.year,
            'month': date_obj.month,
            'day': date_obj.day,
            'page': page,
            'limit': limit
        }

        data = await self._make_request(endpoint, params)

        # Handle background tasks
        if 'task_id' in data and 'products' not in data:
            logger.info(f"Background task created: {data['task_id']}")
            task_results = await self.wait_for_task(data['task_id'])
            if task_results:
                data = task_results
            else:
                return {'products': [], 'error': 'Task failed'}

        return data

    async def get_all_launches_by_date(self, date_string: str, max_products: int):
        """Get all launches for a date with pagination"""
        all_products = []
        page = 1

        while len(all_products) < max_products:
            result = await self.get_launches_by_date(date_string, page=page)

            if 'error' in result:
                break

            products = result.get('products', [])
            if not products:
                break

            all_products.extend(products)

            if not result.get('pagination', {}).get('has_next_page', False):
                break

            page += 1
            await asyncio.sleep(2)  # Rate limiting

        return all_products[:max_products]
