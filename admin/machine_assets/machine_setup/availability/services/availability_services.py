from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional
from fastapi import HTTPException

from admin.machine_assets.machine_setup.availability.repositories.availability_repository import KPIAvailabilityRepository
from admin.machine_assets.machine_setup.availability.schemas.availability_schemas import AvailabilityItem


from admin.db_timescale import save_availability 


class KPIAvailabilityService:
    
    # RUNNING : tous les IDs où description = "Running"
    RUNNING_IDS = {14, 26, 32, 38, 44, 50, 56, 62, 68}
    
    # MICRO_STOP : Minor Stoppages, Cleaning, Rate Deviation
    MICRO_STOP_IDS = {1, 2, 3, 17, 18, 27, 28, 33, 34, 39, 40, 45, 46, 51, 52, 57, 58, 63, 64}
    
    # BREAKDOWN : Change Over, Part Shortage, Machine Breakdown
    BREAKDOWN_IDS = {4, 5, 6, 19, 20, 29, 30, 35, 36, 41, 42, 47, 48, 53, 54, 59, 60, 65, 66}
    
    # PLANNED_STOP : Maintenance, Inventory, Fire Drills, Trial, Meeting, Break
    PLANNED_STOP_IDS = {7, 8, 10, 11, 12, 13, 16, 21, 22, 23, 24, 25, 31, 37, 43, 49, 55, 61, 67}

    def __init__(self, kpi_availability_repository: KPIAvailabilityRepository) -> None:
        self.kpi_availability_repository = kpi_availability_repository
        self._category_ids_cache = None

    @staticmethod
    def _parse_datetime(value: Optional[str]):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _parse_date(value: Optional[str]):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {value}")

    @staticmethod
    def _split_event_by_day(start_dt: datetime, end_dt: datetime):
        current_day_start = datetime.combine(start_dt.date(), datetime.min.time(), tzinfo=start_dt.tzinfo)

        while current_day_start < end_dt:
            next_day_start = current_day_start + timedelta(days=1)
            overlap_start = max(start_dt, current_day_start)
            overlap_end = min(end_dt, next_day_start)

            if overlap_end > overlap_start:
                yield current_day_start.date(), (overlap_end - overlap_start).total_seconds()

            current_day_start = next_day_start
            
    def get_availability(self, station_id=None, date_from=None, date_to=None, token=None):
        running_ids = self.RUNNING_IDS
        micro_stop_ids = self.MICRO_STOP_IDS
        breakdown_ids = self.BREAKDOWN_IDS
        planned_stop_ids = self.PLANNED_STOP_IDS
        
        events = self.kpi_availability_repository.get_machine_condition_data(
            station_id=station_id,
            token=token)
 
        filter_date_from = self._parse_date(date_from)
        filter_date_to = self._parse_date(date_to)

        machine_time = defaultdict(lambda: {
            "run_time_s": 0.0,
            "micro_stop_s": 0.0,
            "breakdown_s": 0.0,
            "planned_stop_s": 0.0,
        })

        for event in events:
            event_station_id = event.get("station_id")
            condition_id = event.get("condition_id")

            if event_station_id is None or condition_id is None:
                continue

            if station_id is not None and event_station_id != station_id:
                continue

            start_dt = self._parse_datetime(event.get("date_from"))
            end_dt = self._parse_datetime(event.get("date_to"))

            if start_dt is None:
                continue

            if end_dt is None:
                end_dt = self._parse_datetime(event.get("updated_at"))

            if end_dt is None or end_dt <= start_dt:
                continue

            for production_day, duration_seconds in self._split_event_by_day(start_dt, end_dt):
                if filter_date_from and production_day < filter_date_from:
                    continue
                if filter_date_to and production_day > filter_date_to:
                    continue

                key = (production_day, event_station_id)
                
                if condition_id in running_ids:        # ← au lieu de self.RUNNING_IDS
                    machine_time[key]["run_time_s"] += duration_seconds
                elif condition_id in micro_stop_ids:   # ← au lieu de self.MICRO_STOP_IDS
                    machine_time[key]["micro_stop_s"] += duration_seconds
                elif condition_id in breakdown_ids:    # ← au lieu de self.BREAKDOWN_IDS
                    machine_time[key]["breakdown_s"] += duration_seconds
                elif condition_id in planned_stop_ids: # ← au lieu de self.PLANNED_STOP_IDS
                    machine_time[key]["planned_stop_s"] += duration_seconds
                
        if not machine_time:
            raise HTTPException(status_code=404, detail="No availability KPI data found")

        
        results = []
        for (production_day, current_station_id), values in sorted(machine_time.items()):
            run_time_s = values["run_time_s"]
            micro_stop_s = values["micro_stop_s"]
            breakdown_s = values["breakdown_s"]
            planned_stop_s = values["planned_stop_s"]

            loading_time_s = run_time_s + micro_stop_s + breakdown_s
            availability_pct = (100.0 * run_time_s / loading_time_s) if loading_time_s > 0 else 0.0

            results.append({
                "production_day": production_day,
                "station_id": current_station_id,
                "run_time_hours": round(run_time_s / 3600.0, 2),
                "micro_stop_hours": round(micro_stop_s / 3600.0, 2),
                "breakdown_hours": round(breakdown_s / 3600.0, 2),
                "planned_stop_hours": round(planned_stop_s / 3600.0, 2),
                "availability_pct": round(availability_pct, 2),
            })
        
    
        for row in results:
            item = AvailabilityItem(
                production_day=row["production_day"],
                station_id=row["station_id"],
                run_time_hours=row["run_time_hours"],
                micro_stop_hours=row["micro_stop_hours"],
                breakdown_hours=row["breakdown_hours"],
                planned_stop_hours=row["planned_stop_hours"],
                availability_pct=row["availability_pct"],
            )
            save_availability(item)
      

        return {
            "title": "Availability KPI",
            "kpi": "availability",
            "count": len(results),
            "results": results
        }