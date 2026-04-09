from datetime import date
from typing import List
from pydantic import BaseModel


class PerformanceItem(BaseModel):
    production_day: date
    station_id: int
    run_time_hours: float
    micro_stop_hours: float
    performance_pct: float


class PerformanceResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[PerformanceItem]