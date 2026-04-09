from datetime import date
from typing import List
from pydantic import BaseModel

from enum import Enum


class DowntimeTypeEnum(str, Enum):
    BREAKDOWN = "BREAKDOWN"
    MICRO_STOP = "MICRO_STOP"
    PART_SHORTAGE = "PART_SHORTAGE"
    RATE_DEVIATION = "RATE_DEVIATION"
    SETUP = "SETUP"
    PLANNED_MAINTENANCE = "PLANNED_MAINTENANCE"
    PLANNED_STOP = "PLANNED_STOP"
    CLEANING = "CLEANING"
    NO_PRODUCTION_BREAK = "NO_PRODUCTION_BREAK"
    OTHER_STOP = "OTHER_STOP"
    
    
class DowntimeByStationResult(BaseModel):
    station_id: int
    production_day: date
    downtime_type: str
    downtime_hours: float
    downtime_minutes: float
    downtime_events: int


class DowntimeByStationResponse(BaseModel):
    title: str
    kpi: str
    count: int
    results: List[DowntimeByStationResult]