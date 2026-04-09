from datetime import datetime
from typing import Optional

from admin.machine_assets.machine_setup.mttr.repositories.mttr_repository import KPIMTTRRepository


class KPIMTTRService:
    def __init__(self, mttr_repository: KPIMTTRRepository) -> None:
        self.mttr_repository = mttr_repository

    def get_mttr(
        self,
        station_id: Optional[int] = None,
        token: Optional[str] = None,
    ):
        data = self.mttr_repository.get_machine_condition_data(token=token)

        repair_time_map = {}
        failure_map = {}

        for item in data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and current_station_id != station_id:
                continue

            condition_id = item.get("condition_id")
            date_from = item.get("date_from")
            date_to = item.get("date_to")
            updated_at = item.get("updated_at")

            duration_seconds = 0.0

            if date_from:
                try:
                    start_dt = datetime.fromisoformat(date_from)

                    if date_to:
                        end_dt = datetime.fromisoformat(date_to)
                    elif updated_at:
                        end_dt = datetime.fromisoformat(updated_at)
                    else:
                        end_dt = start_dt

                    duration_seconds = max((end_dt - start_dt).total_seconds(), 0.0)

                except Exception:
                    duration_seconds = 0.0

            # Machine Breakdown = 6
            if condition_id == 6:
                repair_time_map[current_station_id] = (
                    repair_time_map.get(current_station_id, 0.0) + duration_seconds
                )

                failure_map[current_station_id] = (
                    failure_map.get(current_station_id, 0) + 1
                )

        all_station_ids = sorted(set(repair_time_map.keys()) | set(failure_map.keys()))

        results = []
        for st_id in all_station_ids:
            repair_time_hours = round(repair_time_map.get(st_id, 0.0) / 3600.0, 2)
            failure_count = failure_map.get(st_id, 0)
            mttr_hours = round(repair_time_hours / failure_count, 2) if failure_count > 0 else None

            results.append({
                "station_id": st_id,
                "repair_time_hours": repair_time_hours,
                "failure_count": failure_count,
                "mttr_hours": mttr_hours,
            })

        return {
            "title": "MTTR KPI",
            "kpi": "mttr",
            "count": len(results),
            "results": results,
        }