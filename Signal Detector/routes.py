from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.orm import Session
from database import SessionLocal
from models.models import Product
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, validator

router = APIRouter()

class SignalResponse(BaseModel):
    id: int
    company_id: Optional[int]
    company_name: Optional[str]
    name: Optional[str]
    tagline: Optional[str]
    description: Optional[str]
    thumbnail_url: Optional[str]
    topics: List[str]
    votes: int
    signal_score: Optional[float]
    signal_strength: Optional[str]
    is_signal: bool
    created_at: Optional[str]
    launch_date: Optional[str]
    is_social_scraped: bool
    is_reviewed: bool
    status: int

    class Config:
        from_attributes = True

class SignalsListResponse(BaseModel):
    data: List[SignalResponse]

class DateCheckResponse(BaseModel):
    exists: bool
    count: int
    error: Optional[str] = None

class DateQuery(BaseModel):
    date: Optional[str] = Field(None, pattern=r'^\d{4}-\d{2}-\d{2}$')
    
    @validator('date')
    def validate_date_format(cls, v):
        if v:
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise ValueError('Invalid date format. Use YYYY-MM-DD')
        return v


@router.get("/signals", response_model=SignalsListResponse)
def get_signals(date: Optional[str] = Query(None, pattern=r'^\d{4}-\d{2}-\d{2}$|^$')):
    """
    Get all signals, optionally filtered by launch date (YYYY-MM-DD)
    """
    db: Session = SessionLocal()
    
    try:
        query = db.query(Product)
        
        if date:
            try:
                target_date = datetime.strptime(date, "%Y-%m-%d").date()
                query = query.filter(Product.launch_date == target_date)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
            
        products = query.all()

        result = []
        for p in products:
            md = p.product_metadata or {}
            ph = md.get("product_hunt", {})
            
            if p.launch_date:
                display_date = p.launch_date.isoformat()
            else:
                ph_created_at = ph.get("created_at")
                display_date = ph_created_at if ph_created_at else (p.created_at.isoformat() if p.created_at else None)
            
            result.append(SignalResponse(
                id=p.id,
                company_id=p.company_id,
                company_name=p.company.company_name if p.company else None,
                name=ph.get("name"),
                tagline=ph.get("tagline"),
                description=ph.get("description"),
                thumbnail_url=ph.get("thumbnail_url") or ph.get("thumbnail"),
                topics=ph.get("topics", []),
                votes=ph.get("votes_count", 0),
                signal_score=p.signal_score,
                signal_strength=p.signal_strength,
                is_signal=p.is_signal,
                created_at=display_date,
                launch_date=p.launch_date.isoformat() if p.launch_date else None,
                is_social_scraped=p.is_social_scraped,
                is_reviewed=p.is_reviewed,
                status=p.status,
            ))

        return SignalsListResponse(data=result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/signals/check-date/{date}", response_model=DateCheckResponse)
def check_date_exists(date: str):
    """
    Check if products exist for a given launch date (YYYY-MM-DD)
    """
    db: Session = SessionLocal()
    
    try:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        count = db.query(Product).filter(Product.launch_date == target_date).count()
        return DateCheckResponse(exists=count > 0, count=count)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

