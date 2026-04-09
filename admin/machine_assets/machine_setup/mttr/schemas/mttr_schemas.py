from typing import List, Optional
from pydantic import BaseModel


class MTTRResult(BaseModel):
    station_id: int
    repair_time_hours: float
    failure_count: int
    mttr_hours: Optional[float] = None


class MTTRResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[MTTRResult]