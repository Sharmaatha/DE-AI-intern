import logging
from typing import Optional, List
from datetime import datetime
from database import Database
from models.models import Product
from llm.signal_detector import SignalDetector
from models.models import Company

logger = logging.getLogger(__name__)

def print_separator(char="=", length=60):
    print("\n" + char * length)

def print_section_header(title: str):
    print_separator()
    print(title)
    print_separator()

def analyze_signals(limit: Optional[int] = None, product_ids: Optional[List[int]] = None):
    print_section_header("STEP 3: LLM Signal Analysis")

    signal_detector = SignalDetector()

    with Database() as db:
        query = db.db.query(Product).filter(
            Product.status == 1,
            Product.is_reviewed == False
        )

        if product_ids:
            query = query.filter(Product.id.in_(product_ids))
            print(f"Processing specific product IDs: {product_ids}")
        elif limit:
            query = query.limit(limit)

        pending_products = query.all()

        if not pending_products:
            print("No products pending LLM review (status=1, is_reviewed=False)")
            print_separator()
            print("STEP 3 COMPLETE:")
            print("Success: 0")
            print("Failed: 0")
            print("Total Processed: 0")
            print_separator()
            return

        print(f"Found {len(pending_products)} products pending LLM review\n")

        success_count = 0
        failed_count = 0

        for idx, product in enumerate(pending_products, 1):
            print_section_header(
                f"Analyzing {idx}/{len(pending_products)}: {product.product_name}"
            )

            metadata = product.product_metadata or {}
            ph_data = metadata.get("product_hunt", {})

            if not ph_data:
                print("No Product Hunt data found, skipping LLM analysis")
                product.is_reviewed = True
                product.reviewed_at = datetime.now()
                product.status = 2  
                db.db.commit()
                failed_count += 1
                continue

            print("Running LLM signal detection...")
            try:
                result = signal_detector.analyze(metadata)

                if result:
                    product.signal_score = result.signal_score
                    product.signal_strength = result.signal_strength
                    product.is_signal = result.is_signal
                    product.rationale = result.rationale
                    product.category_fit = result.category_fit
                    product.traction_assessment = result.traction_assessment
                    product.team_assessment = result.team_assessment
                    product.early_stage_indicators = result.early_stage_indicators
                    product.is_reviewed = True
                    product.reviewed_at = datetime.now()
                    product.status = 2
                    
                    if product.is_signal:
                        company = db.db.query(Company).filter(Company.id == product.company_id).first()
                        if company and not company.is_signal:
                            company.is_signal = True
                            db.db.add(company)
                    
                    db.db.commit()

                    print("\nLLM Analysis Complete:")
                    print(f"Signal Score: {result.signal_score}")
                    print(f"Signal Strength: {result.signal_strength}")
                    print(f"Is Signal: {result.is_signal}")
                    print(f"Category Fit: {result.category_fit}")
                    print(f"Traction: {result.traction_assessment}")
                    print(f"Rationale: {result.rationale[:100]}...")
                    print("Status updated to 2 (Complete)")
                    success_count += 1
                else:
                    print("LLM analysis returned no result")
                    product.is_reviewed = True
                    product.reviewed_at = datetime.now()
                    product.status = 2  
                    db.db.commit()
                    failed_count += 1

            except Exception as e:
                print(f"LLM analysis error: {e}")
                logger.error(f"LLM analysis error for {product.product_name}: {e}")
                product.is_reviewed = True
                product.reviewed_at = datetime.now()
                product.status = 2  
                db.db.commit()
                failed_count += 1

        print_separator()
        print("STEP 3 COMPLETE:")
        print(f"Success: {success_count}")
        print(f"Failed: {failed_count}")
        print(f"Total Processed: {len(pending_products)}")
        print_separator()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of products to analyze"
    )
    args = parser.parse_args()

    analyze_signals(limit=args.limit)