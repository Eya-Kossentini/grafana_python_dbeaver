from pydantic import BaseModel
from typing import List


class DefectRateItem(BaseModel):
    station_id: int
    total_bookings: int
    good_count: int
    fail_count: int
    scrap_count: int
    defect_count: int
    defect_rate_pct: float


class DefectRateResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[DefectRateItem]