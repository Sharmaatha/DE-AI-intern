import logging
from typing import Dict, Any, List
from datetime import datetime
from database import Database
from scrapers.producthunt import ProductHuntScraper
from models.models import Company 
from models.models import Product


logger = logging.getLogger(__name__)


def print_separator(char="=", length=60):
    print("\n" + char * length)


def print_section_header(title: str):
    print_separator()
    print(title)
    print_separator()


def scrape_producthunt_only(date_str: str, limit: int = 10) -> List[int]:
    print_section_header(f"STEP 1: Scraping Product Hunt for {date_str}")

    ph_scraper = ProductHuntScraper()

    with Database() as db:
        # Get current scrape progress for the date
        scrape_progress = db.get_or_create_scrape_progress(datetime.strptime(date_str, "%Y-%m-%d").date())
        current_cursor = scrape_progress.last_cursor
        has_next_page = scrape_progress.has_next_page

        if not has_next_page:
            print(f"All products for {date_str} have already been scraped. Skipping.")
            return []

        print("Fetching ProductHunt products...")
        scrape_result = ph_scraper.get_products_by_date(date_str, limit=limit, after_cursor=current_cursor)
        products = scrape_result["products"]
        new_end_cursor = scrape_result.get("endCursor", None)
        new_has_next_page = scrape_result.get("hasNextPage", False)

        print(f"Found {len(products)} products\n")

        if not products:
            print("✗ No new products found — exiting.")
            # If no products are found, it means we reached the end for this date
            db.update_scrape_progress(datetime.strptime(date_str, "%Y-%m-%d").date(), None, False)
            return []

        launch_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        product_ids = []

        for idx, p in enumerate(products, 1):
            print_section_header(f"Processing {idx}/{len(products)}: {p['name']}")

            # Check if product already exists for this product name
            existing_product = db.db.query(Product).filter(Product.product_name == p["name"]).first()
            if existing_product:
                print(f"Product '{p['name']}' already exists (ID: {existing_product.id}), skipping insertion.")
                product_ids.append(existing_product.id) 
                continue

            company_name = p["name"]
            company_id = db.get_or_create_company(company_name)

            # Fetch the Company object to check its is_signal status
            company = db.db.query(Company).filter(Company.id == company_id).first()

            if company and company.is_signal:
                print(f"Company '{company_name}' (ID: {company_id}) is marked as a signal company. Skipping product '{p['name']}'.")
                continue

            print(f"Company ID: {company_id}")
            metadata = {"product_hunt": p}

            product_id = db.insert_product(
                company_id=company_id,
                product_name=p["name"],
                metadata=metadata,
                launch_date=launch_date,
                twitter_link=None,
                linkedin_link=None,
            )
            product_ids.append(product_id)
            print(f"Product ID: {product_id} stored")
            print(f"Launch date: {launch_date}")
            print(f"PH created_at: {p.get('created_at', 'N/A')}")
            print(f"Social scraping: PENDING (is_social_scraped=False)")
            print(f"LLM review: PENDING (is_reviewed=False)\n")
        
        # Update scrape progress for the date
        db.update_scrape_progress(datetime.strptime(date_str, "%Y-%m-%d").date(), new_end_cursor, new_has_next_page)

        print_separator()
        print(f"STEP 1 COMPLETE: {len(product_ids)} products stored/processed")
        print(f"Product IDs: {product_ids}")
        print("Next: Run STEP 2 to extract social links")
        print_separator()
        
        return product_ids


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--limit", type=int, default=10, help="Number of products to fetch")

    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date — use YYYY-MM-DD")
        exit(1)

    scrape_producthunt_only(args.date, args.limit)