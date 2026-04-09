from typing import List, Optional
from pydantic import BaseModel


class FailureLossDiagnosticResult(BaseModel):
    station_id: int
    top_failure_group: Optional[str] = None
    top_failure_count: Optional[int] = None
    top_failure_pct: Optional[float] = None
    top_loss_type: Optional[str] = None
    top_loss_hours: Optional[float] = None
    top_loss_pct: Optional[float] = None
    criticality_level: str
    diagnosis: str


class FailureLossDiagnosticResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[FailureLossDiagnosticResult]