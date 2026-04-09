from datetime import date
from typing import List
from pydantic import BaseModel


class ScrapByDayResult(BaseModel):
    production_day: date
    station_id: int
    total_bookings: int
    scrap_count: int
    scrap_rate_pct: float


class ScrapByDayResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[ScrapByDayResult]

