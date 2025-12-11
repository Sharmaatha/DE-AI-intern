from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Dict, Any
from datetime import datetime

class CompanyBase(BaseModel):
    company_name: str = Field(..., min_length=1, max_length=255)

class CompanyCreate(CompanyBase):
    pass

class CompanyResponse(CompanyBase):
    id: int
    created_by: str
    updated_by: str
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class ProductBase(BaseModel):
    product_name: str = Field(..., min_length=1, max_length=255)
    product_metadata: Dict[str, Any] = Field(default_factory=dict)
    twitter_link: Optional[str] = Field(None, max_length=500)
    linkedin_link: Optional[str] = Field(None, max_length=500)

class ProductCreate(ProductBase):
    company_id: int

class ProductResponse(ProductBase):
    id: int
    company_id: int
    is_social_media: bool
    created_by: str
    updated_by: str
    created_at: datetime
    updated_at: datetime
    status: int  

    model_config = ConfigDict(from_attributes=True)

class ScrapeRequest(BaseModel):
    date: str = Field(..., pattern=r"^\d{2}-\d{2}-\d{4}$", description="Date in DD-MM-YYYY format")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "date": "17-11-2025"
            }
        }
    )

class ScrapeResponse(BaseModel):
    status: str
    date_used: str
    message: Optional[str] = None