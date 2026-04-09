from typing import List, Optional
from pydantic import BaseModel


class DashboardOverviewItem(BaseModel):
    station_id: int
    production_day: Optional[str] = None

    oee_pct: float
    availability_pct: float
    performance_pct: float
    quality_pct: float

    mtbf_hours: Optional[float] = None
    mttr_hours: Optional[float] = None


class DashboardOverviewResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[DashboardOverviewItem]