from typing import List, Optional
from pydantic import BaseModel


class MTBFResult(BaseModel):
    station_id: int
    run_time_hours: float
    failure_count: int
    mtbf_hours: Optional[float] = None 


class MTBFResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[MTBFResult]