from typing import List, Optional
from pydantic import BaseModel


class ReliabilityDiagnosticResult(BaseModel):
    station_id: int
    mtbf_hours: Optional[float] = None
    top_loss_type: Optional[str] = None
    top_loss_pct: Optional[float] = None
    pareto_rank: Optional[int] = None
    criticality_level: str
    diagnosis: str


class ReliabilityDiagnosticResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[ReliabilityDiagnosticResult]