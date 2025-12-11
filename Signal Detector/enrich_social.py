import logging
from typing import Optional, List
from datetime import datetime
from database import Database
from models.models import Product
from scrapers.ph_social import ProductHuntSocialScraper
from scrapers.linkedin import LinkedInScraper
from scrapers.twitter import TwitterScraper

# Import the helper we just created
from scrapers.aliter_api import fetch_company_data_from_api
# Import the UPDATED resolver
from scrapers.redirect_resolver import resolve_redirect   

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

def sanitize_twitter_link(url: str) -> Optional[str]:
    if not url:
        return None
    cleaned = url.split("?")[0].rstrip("/")
    cleaned = cleaned.replace("https://twitter.com", "https://x.com")
    handle = cleaned.split("/")[-1].lower()
    GENERIC = {
        "search", "home", "explore", "notifications", "messages",
        "i", "compose", "login", "signup"
    }
    BLACKLIST = {"producthunt", "product_hunt"}
    if handle in GENERIC or handle in BLACKLIST:
        return None
    return cleaned

def enrich_social_links(limit: Optional[int] = None, product_ids: Optional[List[int]] = None):
    ph_social = ProductHuntSocialScraper()
    linkedin_scraper = LinkedInScraper()
    twitter_scraper = TwitterScraper()

    with Database() as db:

        query = db.db.query(Product).filter(Product.status == 0)
        query = query.filter(Product.is_social_scraped == False) # Only enrich products not yet enriched

        if product_ids:
            query = query.filter(Product.id.in_(product_ids))
        elif limit:
            query = query.limit(limit)

        products = query.all()

        if not products:
            print("No products pending social enrichment (status=0)")
            return

        for product in products:
            print("\n=======")
            print(f"Enriching: {product.product_name} (ID: {product.id})")

            metadata = product.product_metadata or {}
            ph_data = metadata.get("product_hunt", {})

            ph_url = ph_data.get("product_hunt_url")
            redirect_url = ph_data.get("website")   

            linkedin_url = None
            twitter_url = None

            # 1. Scrape Socials from Product Hunt Page
            if ph_url:
                scrape_url = ph_url.split("?")[0]
                print(f"Extracting social links from: {scrape_url}")

                try:
                    socials = ph_social.extract_social_links(scrape_url)
                    linkedin_url = socials.get("linkedinUrl")
                    twitter_url = sanitize_twitter_link(socials.get("twitterUrl"))
                except Exception as e:
                    print(f"Playwright social extraction failed: {e}")

            # 2. Enrich LinkedIn
            if linkedin_url:
                print("→ Enriching LinkedIn…")
                try:
                    linkedin_data = linkedin_scraper.get_company_about_details(linkedin_url)
                    if linkedin_data:
                        metadata["linkedin"] = linkedin_data
                except Exception as e:
                    print(f"LinkedIn error: {e}")

            # 3. Enrich Twitter
            if twitter_url:
                print("→ Enriching Twitter…")
                try:
                    handle = twitter_scraper.extract_handle_from_url(twitter_url)
                    twitter_data = twitter_scraper.get_profile(handle)
                    if twitter_data:
                        metadata["twitter"] = twitter_data
                except Exception as e:
                    print(f"Twitter error: {e}")

            # 4. Resolve Redirects (FIXED LOGIC)
            redirect_urls = []
            resolved_website_url = None
            
            if redirect_url:
                try:
                    # Returns either a list ["url"] or a string "url" depending on implementation
                    result = resolve_redirect(redirect_url, ph_url, debug=True)
                    
                    # Normalize result to a list to avoid the "h" string bug
                    if isinstance(result, list):
                        redirect_urls = result
                    elif isinstance(result, str):
                        redirect_urls = [result]
                    
                    # Grab the first URL safely
                    if redirect_urls and len(redirect_urls) > 0:
                        resolved_website_url = redirect_urls[0]
                        
                except Exception as e:
                    print(f"Redirect resolver error: {e}")

            metadata["redirect_urls"] = redirect_urls   

            if resolved_website_url:
                print(f"→ Running Website Blackbox Scraper on: {resolved_website_url}")
                try:
                    website_data_list = fetch_company_data_from_api(resolved_website_url)
                    
                    # Check if list exists and has items before accessing [0]
                    if website_data_list and len(website_data_list) > 0:
                        website_data = website_data_list[0]
                        
                        # Use .get() to avoid KeyErrors if fields are missing
                        metadata['founded_year'] = website_data.get('founded_year')
                        metadata['org_name'] = website_data.get('organization_name')
                        metadata['industries'] = website_data.get('industries')
                        metadata['description'] = website_data.get('description')
                        
                        print("   ✓ Extracted specific fields to metadata")
                    else:
                         print("   ⚠ Blackbox returned no data.")
                         
                except Exception as e:
                    print(f"   ✗ Website scraper failed: {e}")

            # Update Product Object
            product.product_metadata = metadata
            product.linkedin_link = linkedin_url
            product.twitter_link = twitter_url

            product.is_social_media = bool(linkedin_url or twitter_url)
            product.is_social_scraped = True
            product.social_scrape_attempted_at = datetime.now()

            product.status = 1   

            db.db.commit()

            print(
                f"Completed {product.id} — LinkedIn={bool(linkedin_url)}, "
                f"Twitter={bool(twitter_url)}, Redirects={len(redirect_urls)}, Status=1"
            )