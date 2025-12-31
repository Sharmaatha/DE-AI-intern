"""
Pydantic models for ProductHunt Scraper API
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict
from datetime import datetime, date
from enum import Enum


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ScrapeRequest(BaseModel):
    dates: List[str] = Field(
        ...,
        description="List of dates in YYYY-MM-DD format",
        json_schema_extra={"example": ["2025-12-12", "2025-12-11"]}
    )
    max_products_per_date: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum products to fetch per date"
    )

    @field_validator('dates')
    @classmethod
    def validate_dates(cls, v):
        for date_str in v:
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")
        return v


class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    created_at: str
    message: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: int = Field(ge=0, le=100)
    total_dates: int
    processed_dates: int
    total_products: int
    created_at: str
    updated_at: str
    error: Optional[str] = None


class ProductSummary(BaseModel):
    name: str
    daily_rank: int
    votes_count: int
    domain: str


class DateResults(BaseModel):
    date: str
    total_products: int
    top_products: List[ProductSummary]
