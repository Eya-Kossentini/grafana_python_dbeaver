from datetime import date
from pydantic import BaseModel
from typing import List


class OeeResult(BaseModel):
    production_day: date
    station_id: int
    availability_pct: float
    performance_pct: float
    quality_pct: float
    quality_missing: bool
    oee_pct: float


class OeeResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[OeeResult]