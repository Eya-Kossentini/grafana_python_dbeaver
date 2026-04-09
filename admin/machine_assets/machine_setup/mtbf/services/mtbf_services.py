from datetime import datetime
from typing import Optional

from admin.machine_assets.machine_setup.mtbf.repositories.mtbf_repository import KPIMTBFRepository


class KPIMTBFService:
    def __init__(self, mtbf_repository: KPIMTBFRepository) -> None:
        self.mtbf_repository = mtbf_repository

    def get_mtbf(
        self,
        station_id: Optional[int] = None,
        token: Optional[str] = None,
    ):
        data = self.mtbf_repository.get_machine_condition_data(token=token)

        run_time_map = {}
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

            # Running = 14
            if condition_id == 14:
                run_time_map[current_station_id] = (
                    run_time_map.get(current_station_id, 0.0) + duration_seconds
                )

            # Machine Breakdown = 6
            elif condition_id == 6:
                failure_map[current_station_id] = (
                    failure_map.get(current_station_id, 0) + 1
                )

        all_station_ids = sorted(set(run_time_map.keys()) | set(failure_map.keys()))

        results = []
        for st_id in all_station_ids:
            run_time_hours = round(run_time_map.get(st_id, 0.0) / 3600.0, 2)
            failure_count = failure_map.get(st_id, 0)

            mtbf_hours = round(run_time_hours / failure_count, 2) if failure_count > 0 else None

            results.append({
                "station_id": st_id,
                "run_time_hours": run_time_hours,
                "failure_count": failure_count,
                "mtbf_hours": mtbf_hours,
            })

        return {
            "title": "MTBF KPI",
            "kpi": "mtbf",
            "count": len(results),
            "results": results,
        }