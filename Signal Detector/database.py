from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
from datetime import date
from contextlib import contextmanager
from config import DB_CONFIG, CREATED_BY
from models.models import Base, Company, Product, ScrapeProgress
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import JSONB

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db():
    """Create all tables if they don't exist."""
    try:
        print("Initializing database — creating tables if missing...")
        Base.metadata.create_all(bind=engine)
        print("Database initialized — tables verified/created.")
    except Exception as e:
        print("Database initialization failed:", e)
        raise

class Database:
    def __init__(self):
        self.db: Session = SessionLocal()

    def close(self):
        if self.db:
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.db.rollback()
        self.close()

    def get_or_create_company(self, company_name: str) -> int:
        try:
            company = self.db.query(Company).filter_by(company_name=company_name).first()
            if company:
                return company.id
            new_company = Company(
                company_name=company_name,
                created_by=CREATED_BY,
                updated_by=CREATED_BY
            )
            self.db.add(new_company)
            self.db.commit()
            self.db.refresh(new_company)
            return new_company.id
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"DB error creating company: {e}")

    def insert_product(
        self,
        company_id: int,
        product_name: str,
        metadata: dict,
        launch_date: Optional[date] = None,
        twitter_link: Optional[str] = None,
        linkedin_link: Optional[str] = None,
    ) -> int:
        try:
            product = Product(
                company_id=company_id,
                product_name=product_name,
                product_metadata=metadata,
                launch_date=launch_date,
                twitter_link=twitter_link,
                linkedin_link=linkedin_link,
                is_social_media=bool(twitter_link or linkedin_link),
                created_by=CREATED_BY,
                updated_by=CREATED_BY,
                status=0  
            )
            self.db.add(product)
            self.db.commit()
            self.db.refresh(product)
            return product.id
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"DB error inserting product: {e}")

    def get_product_by_name(self, product_name: str) -> Optional[Product]:
        return self.db.query(Product).filter(Product.product_name == product_name).first()

    def get_or_create_scrape_progress(self, date: date) -> "ScrapeProgress":
        progress = self.db.query(ScrapeProgress).filter_by(date=date).first()
        if not progress:
            progress = ScrapeProgress(date=date)
            self.db.add(progress)
            self.db.commit()
            self.db.refresh(progress)
        return progress

    def update_scrape_progress(self, date: date, last_cursor: Optional[str], has_next_page: bool):
        progress = self.db.query(ScrapeProgress).filter_by(date=date).first()
        if progress:
            progress.last_cursor = last_cursor
            progress.has_next_page = has_next_page
            self.db.commit()
        else:
            # Should not happen if get_or_create_scrape_progress is used first
            raise Exception(f"ScrapeProgress for date {date} not found.")

    def update_product_status(self, product_id: int, status: int):
        """Update product status (0=Pending, 1=Enriched, 2=Complete)"""
        try:
            product = self.db.query(Product).filter(Product.id == product_id).first()
            if product:
                product.status = status
                self.db.commit()
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"DB error updating status: {e}")

    def update_product_signal(
        self,
        product_id: int,
        signal_score: int,
        signal_strength: str,
        is_signal: bool,
        rationale: str,
        category_fit: str,
        traction_assessment: str,
        team_assessment: str,
        early_stage_indicators: str,
    ):
        product = self.db.query(Product).filter(Product.id == product_id).first()
        if product:
            product.signal_score = signal_score
            product.signal_strength = signal_strength
            product.is_signal = is_signal
            product.rationale = rationale
            product.category_fit = category_fit
            product.traction_assessment = traction_assessment
            product.team_assessment = team_assessment
            product.early_stage_indicators = early_stage_indicators
            try:
                self.db.commit()
            except SQLAlchemyError as e:
                self.db.rollback()
                raise Exception(f"DB error updating signal: {e}")