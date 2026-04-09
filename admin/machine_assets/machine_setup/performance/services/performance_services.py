from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional
from fastapi import HTTPException

from admin.machine_assets.machine_setup.performance.repositories.performance_repository import KPIPerformanceRepository


class KPIPerformanceService:
    RUNNING_IDS = {14}
    MICRO_STOP_IDS = {1}

    def __init__(self, kpi_performance_repository: KPIPerformanceRepository) -> None:
        self.kpi_performance_repository = kpi_performance_repository

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

    def get_performance(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None
    ):
        events = self.kpi_performance_repository.get_machine_condition_data(
            station_id=station_id,
            token=token
        )

        filter_date_from = self._parse_date(date_from)
        filter_date_to = self._parse_date(date_to)

        machine_time = defaultdict(lambda: {
            "run_time_s": 0.0,
            "micro_stop_s": 0.0,
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

                if condition_id in self.RUNNING_IDS:
                    machine_time[key]["run_time_s"] += duration_seconds
                elif condition_id in self.MICRO_STOP_IDS:
                    machine_time[key]["micro_stop_s"] += duration_seconds

        if not machine_time:
            raise HTTPException(status_code=404, detail="No performance KPI data found")

        results = []
        for (production_day, current_station_id), values in sorted(machine_time.items()):
            run_time_s = values["run_time_s"]
            micro_stop_s = values["micro_stop_s"]

            net_operating_time_s = run_time_s + micro_stop_s
            performance_pct = (100.0 * run_time_s / net_operating_time_s) if net_operating_time_s > 0 else 0.0

            results.append({
                "production_day": production_day,
                "station_id": current_station_id,
                "run_time_hours": round(run_time_s / 3600.0, 2),
                "micro_stop_hours": round(micro_stop_s / 3600.0, 2),
                "performance_pct": round(performance_pct, 2),
            })

        return {
            "title": "Performance KPI",
            "kpi": "performance",
            "count": len(results),
            "results": results
        }