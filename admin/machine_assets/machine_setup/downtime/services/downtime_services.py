from collections import defaultdict
from datetime import datetime
from typing import Optional

from admin.machine_assets.machine_setup.downtime.repositories.downtime_repository import (
    KPIDowntimeRepository,
)


class KPIDowntimeService:
    def __init__(self, downtime_repository: KPIDowntimeRepository) -> None:
        self.downtime_repository = downtime_repository

    def get_downtime_by_station(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        downtime_type: Optional[str] = None,
        token: Optional[str] = None,
    ):
        conditions_data = self.downtime_repository.get_machine_conditions(token=token)
        condition_data = self.downtime_repository.get_machine_condition_data(token=token)

        def normalize_day(value):
            return str(value)[:10] if value is not None else None

        def in_date_range(day: Optional[str]) -> bool:
            if day is None:
                return False
            if date_from is not None and day < date_from:
                return False
            if date_to is not None and day > date_to:
                return False
            return True

        def map_downtime_type(description: str) -> str:
            desc = (description or "").strip().lower()

            if desc == "machine breakdown":
                return "BREAKDOWN"

            if desc == "minor stoppages & waiting":
                return "MICRO_STOP"

            if desc == "part shortage":
                return "PART_SHORTAGE"

            if desc == "rate deviation & others":
                return "RATE_DEVIATION"

            if desc == "change over & setup":
                return "SETUP"

            if desc in ("preventive maintenance", "maintenance"):
                return "PLANNED_MAINTENANCE"

            if desc in ("inventory check", "meeting", "fire drills", "trial & pilot run"):
                return "PLANNED_STOP"

            if desc == "cleaning":
                return "CLEANING"

            if desc == "no production & break":
                return "NO_PRODUCTION_BREAK"

            return "OTHER_STOP"

        # map condition_id -> condition_description
        condition_map = {
            item["id"]: item.get("condition_description", "")
            for item in conditions_data
            if item.get("id") is not None
        }

        grouped = defaultdict(lambda: {
            "downtime_seconds": 0.0,
            "downtime_events": 0,
        })

        for item in condition_data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and current_station_id != station_id:
                continue

            condition_id = item.get("condition_id")
            description = condition_map.get(condition_id, "")

            # Exclure Running
            if (description or "").strip().lower() == "running":
                continue

            production_day = normalize_day(item.get("date_from"))
            if not in_date_range(production_day):
                continue

            start_raw = item.get("date_from")
            end_raw = item.get("date_to") or item.get("updated_at")

            duration_seconds = 0.0
            if start_raw and end_raw:
                try:
                    start_dt = datetime.fromisoformat(start_raw)
                    end_dt = datetime.fromisoformat(end_raw)
                    duration_seconds = max((end_dt - start_dt).total_seconds(), 0.0)
                except Exception:
                    duration_seconds = 0.0

            current_downtime_type = map_downtime_type(description)

            if downtime_type is not None and current_downtime_type.upper() != downtime_type.upper():
                continue

            key = (current_station_id, production_day, current_downtime_type)
            grouped[key]["downtime_seconds"] += duration_seconds
            grouped[key]["downtime_events"] += 1

        results = []

        for (st_id, prod_day, dt_type), values in grouped.items():
            downtime_seconds = values["downtime_seconds"]
            downtime_hours = round(downtime_seconds / 3600.0, 2)
            downtime_minutes = round(downtime_seconds / 60.0, 2)
            downtime_events = values["downtime_events"]

            results.append({
                "station_id": st_id,
                "production_day": prod_day,
                "downtime_type": dt_type,
                "downtime_hours": downtime_hours,
                "downtime_minutes": downtime_minutes,
                "downtime_events": downtime_events,
            })

        results.sort(
            key=lambda x: (
                x["production_day"],
                x["station_id"],
                -x["downtime_hours"],
            )
        )

        return {
            "title": "Downtime By Station KPI",
            "kpi": "downtime_by_station",
            "count": len(results),
            "results": results,
        }