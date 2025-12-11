from sqlalchemy import Column, Integer, String, Boolean, JSON, ForeignKey, DateTime, Text, Date
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column
from datetime import datetime, date
from typing import Optional, List

class Base(DeclarativeBase):
    """Base class for all models"""
    pass

class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    company_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    created_by: Mapped[str] = mapped_column(String, nullable=False)
    updated_by: Mapped[str] = mapped_column(String, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
    is_signal: Mapped[bool] = mapped_column(Boolean, default=False)

    products: Mapped[List["Product"]] = relationship("Product", back_populates="company", cascade="all, delete-orphan")

class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    product_name: Mapped[str] = mapped_column(String, nullable=False)
    launch_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    product_metadata: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=True)
    is_social_media: Mapped[bool] = mapped_column(Boolean, default=False)
    twitter_link: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    linkedin_link: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_by: Mapped[str] = mapped_column(String, nullable=False)
    updated_by: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
    is_social_scraped: Mapped[bool] = mapped_column(Boolean, default=False)
    social_scrape_attempted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    is_reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    is_signal = Column(Boolean, default=False)
    signal_score = Column(Integer, nullable=True)
    signal_strength = Column(String(20), nullable=True)
    rationale = Column(Text, nullable=True)
    category_fit = Column(String(50), nullable=True)
    traction_assessment = Column(String(50), nullable=True)
    team_assessment = Column(String(50), nullable=True)
    early_stage_indicators = Column(String(50), nullable=True)
    
    status: Mapped[int] = mapped_column(Integer, default=0, index=True)
    company: Mapped["Company"] = relationship("Company", back_populates="products")

#status
    # 0 = Pending (scraped, not enriched)
    # 1 = Enriched (social data added, not analyzed)
    # 2 = Complete (LLM analysis done)

class ScrapeProgress(Base):
    __tablename__ = "scrape_progress"

    date: Mapped[date] = mapped_column(Date, primary_key=True, index=True, nullable=False)
    last_cursor: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    has_next_page: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)

    