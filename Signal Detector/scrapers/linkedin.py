import logging
import re
import requests
from typing import Optional, Dict, Any, List
from config import SERPER_API_KEY, API_TIMEOUT

logger = logging.getLogger(__name__)


class LinkedInScraper:
    def __init__(self):
        self.api_key = SERPER_API_KEY
        self.base_url = "https://google.serper.dev/search"
        self.timeout = API_TIMEOUT

    def _make_request(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            resp = requests.post(
                self.base_url,
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Serper request failed: {e}")
            return None

    def _extract_company_name_from_url(self, url: str) -> str:
        if not url:
            return ""
        identifier = url.rstrip("/").split("/")[-1]
        return identifier.replace("-", " ").title()

    def _is_valid_company_result(self, result: Dict[str, Any], expected_name: str) -> bool:
        title = result.get("title", "").lower()
        snippet = result.get("snippet", "").lower()
        expected = expected_name.lower()

        if "/in/" in result.get("link", ""):
            return False

        if expected and len(expected) > 3:
            if not any(part in title or snippet for part in expected.split() if len(part) > 2):
                return False

        garbage = ["top linkedin", "explore top", "linkedin content", "search for", "find people"]
        if any(g in title or g in snippet for g in garbage):
            return False

        return True

    def _extract_details(self, text: str) -> Dict[str, Any]:
        details = {}

        size_match = re.search(r"(\d[\d,]*\s*-\s*\d[\d,]*\s*employees)", text, re.I)
        if size_match:
            details["company_size"] = size_match.group(1)

        followers_match = re.search(r"([\d,]+)\s+followers", text, re.I)
        if followers_match:
            details["followers"] = followers_match.group(1).replace(",", "")

        founded_match = re.search(r"Founded:?\s*(\d{4})", text, re.I)
        if founded_match:
            details["founded"] = founded_match.group(1)

        hq_match = re.search(
            r"Headquarters:?\s*([A-Za-z ,]+)",
            text
        )
        if hq_match:
            details["headquarters"] = hq_match.group(1).strip()

        industries = [
            "Technology, Information and Internet",
            "Software Development",
            "Artificial Intelligence",
            "SaaS",
            "Fintech",
            "Developer Tools",
            "API",
            "Marketing",
            "E-Commerce"
        ]
        for industry in industries:
            if industry.lower() in text.lower():
                details["industry"] = industry
                break

        spec_match = re.search(
            r"Specialties:?\s*([^\.]+)",
            text,
            re.I
        )
        if spec_match:
            specialties = [s.strip() for s in spec_match.group(1).split(",")]
            if specialties:
                details["specialties"] = specialties

        web_match = re.search(r"(https?://[^\s]+)", text, re.I)
        if web_match:
            details["website"] = web_match.group(1)

        if re.search(r"Privately Held", text, re.I):
            details["company_type"] = "Privately Held"
        elif re.search(r"Public Company", text, re.I):
            details["company_type"] = "Public Company"

        return details

    def get_company_about_details(self, linkedin_url: str) -> Optional[Dict[str, Any]]:
        if not linkedin_url:
            return None

        identifier = linkedin_url.rstrip("/").split("/")[-1]
        if identifier.isdigit():
            logger.warning(f"Skipping numeric LinkedIn URL: {linkedin_url}")
            return None

        expected_name = self._extract_company_name_from_url(linkedin_url)

        payload = {
            "q": f'"{expected_name}" site:linkedin.com/company',
            "num": 10
        }
        logger.info(f"Serper search: {payload['q']}")

        data = self._make_request(payload)

        if not data or not data.get("organic"):
            logger.error(f"No Serper results for {expected_name}")
            return None

        valid_results = [
            r for r in data["organic"]
            if self._is_valid_company_result(r, expected_name)
        ][:3]

        if not valid_results:
            logger.error(f"No valid company pages found for {expected_name}")
            return None

        result = valid_results[0]

        enriched = {
            "url": linkedin_url,
            "company_name": expected_name,
            "title": result.get("title", expected_name),
            "snippet": result.get("snippet", ""),
            "website": "N/A",
            "industry": "N/A",
            "company_size": "N/A",
            "headquarters": "N/A",
            "founded": "N/A",
            "specialties": "N/A",
            "followers": "N/A",
            "company_type": "N/A"
        }

        def extract_from_attributes(attrs):
            info = {}
            for a in attrs:
                key = a.get("key", "").lower()
                val = a.get("value", "")

                if "industry" in key:
                    info["industry"] = val
                elif "company size" in key:
                    info["company_size"] = val
                elif "headquarters" in key:
                    info["headquarters"] = val
                elif "founded" in key:
                    info["founded"] = val
                elif "company type" in key:
                    info["company_type"] = val
                elif "specialties" in key:
                    info["specialties"] = [s.strip() for s in val.split(",")]
                elif "employees" in key and "size" in key:
                    info["company_size"] = val
            return info

        attributes = result.get("attributes", [])
        attributes_v2 = result.get("attributesV2", [])

        extra1 = extract_from_attributes(attributes)
        extra2 = extract_from_attributes(attributes_v2)

        for k, v in {**extra1, **extra2}.items():
            if v:
                enriched[k] = v

        about_block = (
            result.get("aboutThisResult", "") + " " +
            result.get("snippet", "")
        )

        m = re.search(r"([\d,]+)\s+followers", about_block, re.I)
        if m:
            enriched["followers"] = m.group(1).replace(",", "")

        cached = result.get("cachedPageContent") or result.get("cachedPageData") or ""

        if cached:
            cached_text = cached.replace("\n", " ")

            # Followers
            m = re.search(r"([\d,]+)\s+followers", cached_text, re.I)
            if m and enriched["followers"] == "N/A":
                enriched["followers"] = m.group(1).replace(",", "")

            # Company size
            m = re.search(r"(\d[\d,]*\s*-\s*\d[\d,]*\s*employees)", cached_text, re.I)
            if m and enriched["company_size"] == "N/A":
                enriched["company_size"] = m.group(1).strip()

            # Industry
            m = re.search(r"Industry[: ]+([A-Za-z ,&/]+)", cached_text, re.I)
            if m and enriched["industry"] == "N/A":
                enriched["industry"] = m.group(1).strip()

            # Headquarters
            m = re.search(r"Headquarters[: ]+([A-Za-z ,]+)", cached_text, re.I)
            if m and enriched["headquarters"] == "N/A":
                enriched["headquarters"] = m.group(1).strip()

            # Founded
            m = re.search(r"Founded[: ]+(\d{4})", cached_text, re.I)
            if m and enriched["founded"] == "N/A":
                enriched["founded"] = m.group(1)

            # Company type
            m = re.search(r"(Privately Held|Public Company|Partnership|Self-Employed)", cached_text, re.I)
            if m and enriched["company_type"] == "N/A":
                enriched["company_type"] = m.group(1)

            # Specialties
            m = re.search(r"Specialties[: ]+([^<]+)", cached_text, re.I)
            if m and enriched["specialties"] == "N/A":
                enriched["specialties"] = [s.strip() for s in m.group(1).split(",")]

        fallback = self._extract_details(result.get("snippet", ""))
        for k, v in fallback.items():
            if enriched.get(k) == "N/A" and v:
                enriched[k] = v

        return enriched
