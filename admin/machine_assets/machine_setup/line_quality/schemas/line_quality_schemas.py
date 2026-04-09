from pydantic import BaseModel
from typing import List, Optional


class LineProductionQualityItem(BaseModel):
    line_id: int
    line_name: Optional[str] = None
    description: Optional[str] = None
    total_bookings: int
    good_count: int
    fail_count: int
    scrap_count: int
    quality_rate_pct: float


class LineProductionQualityResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[LineProductionQualityItem]