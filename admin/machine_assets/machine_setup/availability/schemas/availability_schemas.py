from datetime import date
from typing import List
from pydantic import BaseModel


class AvailabilityItem(BaseModel):
    production_day: date
    station_id: int
    run_time_hours: float
    micro_stop_hours: float
    breakdown_hours: float
    planned_stop_hours: float
    availability_pct: float


class AvailabilityResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[AvailabilityItem]