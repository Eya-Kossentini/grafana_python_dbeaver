from datetime import date
from typing import List, Optional
from pydantic import BaseModel


class QualityItem(BaseModel):
    production_day: date
    station_id: int
    total_bookings: int
    good_count: int
    fail_count: int
    scrap_count: int
    quality_pct: Optional[float]
    defect_rate_pct: Optional[float]


class QualityResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[QualityItem]