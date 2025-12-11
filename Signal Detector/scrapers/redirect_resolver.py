import re
import logging
from urllib.parse import urlparse
from curl_cffi import requests

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---

BLOCKLIST = [
    # Social Media
    "facebook.com", "instagram.com", "twitter.com", "x.com", "t.co", "linkedin.com",
    "pinterest.com", "tiktok.com", "reddit.com", "discord.com", "discord.gg",
    "t.me", "telegram.me", "whatsapp.com", "youtube.com", "youtu.be", "vimeo.com",
    "medium.com", "substack.com", "github.com", "gitlab.com", "bitbucket.org",
    
    # Product Hunt Internals
    "producthunt.com", "ph-static", "openalternative.co", "ycombinator.com",
    
    # Analytics & Ads
    "google-analytics.com", "googletagmanager.com", "hotjar.com", "segment.io",
    "intercom.io", "mixpanel.com", "amplitude.com", "umami.is", "sentry.io",
    "cloudflare.com", "cloudflareinsights.com", "doubleclick.net", "nxgntools.com",
    
    # Common Infrastructure / Search (unless it's the specific target, these are usually noise)
    "google.com", "gstatic.com", "googleapis.com", "aws.amazon.com", "amazonaws.com",
    "microsoft.com", "apple.com", "w3.org", "schema.org", "gravatar.com", "akamaized.net"
]

ASSET_EXT = ('.png', '.jpg', '.svg', '.css', '.js', '.ico', '.woff2', '.xml', '.json')

def clean_url(u):
    """Remove escape characters from URLs."""
    if not u: return None
    return u.replace("\\/", "/")

def sanitize_to_root(url: str) -> str:
    """
    Truncates a URL to its root domain + scheme.
    Ex: https://january.capital/investment -> https://january.capital
    """
    try:
        parsed = urlparse(url)
        # Ensure we don't accidentally return 'https://' if netloc is empty
        if not parsed.netloc:
            return url
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return url

def is_blocked(link):
    """Check if domain matches blocklist."""
    try:
        domain = urlparse(link).netloc.lower()
        # If the link is invalid, treat as blocked
        if not domain: return True
        return any(b in domain for b in BLOCKLIST)
    except:
        return True

def extract_keywords(producthunt_url: str):
    """Extract product name keywords from ProductHunt URL slug."""
    try:
        parsed = urlparse(producthunt_url)
        path_parts = [p for p in parsed.path.split('/') if p]
        if not path_parts: return []
        
        # Take the last part of the path (the slug)
        slug = path_parts[-1].split('?')[0]
        # Split by hyphens and ignore short words
        keywords = [w.lower() for w in slug.split('-') if len(w) > 2]
        return keywords
    except Exception:
        return []

def resolve_redirect(redirect_url, producthunt_url, debug=False):
    """
    Robust resolver that handles JS redirects, Meta Refresh, and direct HTTP redirects.
    """
    if debug:
        print(f"Resolving: {redirect_url}")

    # 1. Fetch Content with improved stealth
    try:
        # Use a newer Chrome version and add Referer to look legitimate
        response = requests.get(
            redirect_url, 
            impersonate="chrome124", 
            headers={"Referer": "https://www.producthunt.com/"},
            timeout=15,
            allow_redirects=True 
        )
        content = response.text
    except Exception as e:
        logger.error(f"Error fetching {redirect_url}: {e}")
        return []

    # 2. CHECK: Did the HTTP client already follow the redirect?
    # If response.url is different from the input and NOT Product Hunt, we are done.
    if response.url != redirect_url and not is_blocked(response.url):
        clean_final = sanitize_to_root(response.url)
        if debug: logger.info(f"HTTP Redirect followed -> {clean_final}")
        return [clean_final]

    # 3. CHECK: Are we blocked?
    if "challenge-platform" in content or "Just a moment..." in content:
        if debug: logger.warning("⚠️ BLOCKED by Cloudflare (Challenge Page detected).")
        # If blocked, we can't do anything with this response.
        return []

    # 4. STRATEGY A: Regex Search (JS & Meta Refresh)
    patterns = [
        # Standard JS Redirects
        r'window\.location\.replace\(\s*["\'](http[^"\']+)["\']',
        r'window\.location\.href\s*=\s*["\'](http[^"\']+)["\']',
        r'window\.location\s*=\s*["\'](http[^"\']+)["\']',
        # Meta Refresh (Common fallback)
        r'meta\s+http-equiv=["\']refresh["\']\s+content=["\']\d+;\s*url=(http[^"\']+)["\']'
    ]

    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            candidate = clean_url(match.group(1))
            if not is_blocked(candidate):
                clean_final = sanitize_to_root(candidate)
                if debug: logger.info(f"Strategy A (Regex) found: {clean_final}")
                return [clean_final]

    # 5. STRATEGY B: Link Extraction + Keyword Filtering
    # If regex fails, scrape all links and find the one matching the product name
    
    # Extract all hrefs and json urls
    json_urls = re.findall(r'"(https?://[^"]+)"', content)
    html_urls = re.findall(r'href=["\'](https?://[^"\']+)["\']', content)
    all_links = json_urls + html_urls

    valid_links = []
    seen = set()

    # Filter invalid/blocked links
    for link in all_links:
        link = clean_url(link)
        if not link or link in seen: continue
        
        # Strip trailing slashes for cleaner comparison
        link = link.rstrip('/')
        
        if is_blocked(link): continue
        if link.lower().endswith(ASSET_EXT): continue
        
        seen.add(link)
        valid_links.append(link)

    # Filter by Keywords
    keywords = extract_keywords(producthunt_url)
    keyword_matches = []
    
    if keywords:
        for link in valid_links:
            # Check if ANY keyword exists in the domain or path
            if any(kw in link.lower() for kw in keywords):
                keyword_matches.append(link)

    if keyword_matches:
        # Sanitize and deduplicate
        final_list = list(dict.fromkeys([sanitize_to_root(x) for x in keyword_matches]))
        if debug: logger.info(f"Strategy B (Keywords) found: {final_list}")
        return final_list

    # 6. STRATEGY C: Blind Fallback
    # If we have valid external links but no keyword match, take the first one.
    if valid_links:
        # Usually the first external link on a redirect page is the target
        fallback = sanitize_to_root(valid_links[0])
        if debug: logger.info(f"Strategy C (Fallback) found: {fallback}")
        return [fallback]

    if debug: logger.warning("No valid redirect URLs found.")
    return []