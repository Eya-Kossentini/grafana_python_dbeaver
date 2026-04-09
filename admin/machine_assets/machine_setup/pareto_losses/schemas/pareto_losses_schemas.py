from datetime import date
from typing import List
from pydantic import BaseModel


class ParetoLossesResult(BaseModel):
    station_id: int
    production_day: date
    loss_type: str
    loss_hours: float
    loss_pct: float
    cumulative_pct: float
    pareto_rank: int
    is_critical:bool


class ParetoLossesResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[ParetoLossesResult]