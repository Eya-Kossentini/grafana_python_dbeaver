from datetime import datetime
from typing import Optional

from admin.machine_assets.machine_setup.mtbf.repositories.mtbf_repository import KPIMTBFRepository
from admin.machine_assets.machine_setup.mtbf.schemas.mtbf_schemas import MTBFResult
from admin.db_timescale import save_mtbf


class KPIMTBFService:
    RUNNING_CODES = {"1000"}
    BREAKDOWN_CODES = {"2000"}

    def __init__(self, mtbf_repository: KPIMTBFRepository) -> None:
        self.mtbf_repository = mtbf_repository

    @staticmethod
    def _parse_datetime(value: Optional[str]):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    def _build_condition_code_map(self, token: Optional[str] = None):
        conditions = self.mtbf_repository.get_all_machine_conditions(token=token)

        return {
            cond.get("id"): str(cond.get("condition_name"))
            for cond in conditions
            if cond.get("id") is not None and cond.get("condition_name") is not None
        }

    def get_mtbf(self, station_id: Optional[int] = None, token: Optional[str] = None):
        data = self.mtbf_repository.get_machine_condition_data(token=token)
        condition_code_map = self._build_condition_code_map(token=token)

        run_time_map = {}
        failure_map = {}

        for item in data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and int(current_station_id) != int(station_id):
                continue

            condition_id = item.get("condition_id")
            condition_code = condition_code_map.get(condition_id)

            start_dt = self._parse_datetime(item.get("date_from"))
            end_dt = self._parse_datetime(item.get("date_to")) or self._parse_datetime(item.get("updated_at"))

            if start_dt is None or end_dt is None or end_dt <= start_dt:
                continue

            duration_seconds = (end_dt - start_dt).total_seconds()

            if condition_code in self.RUNNING_CODES:
                run_time_map[current_station_id] = (
                    run_time_map.get(current_station_id, 0.0) + duration_seconds
                )

            elif condition_code in self.BREAKDOWN_CODES:
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

        for row in results:
            item = MTBFResult(
                station_id=row["station_id"],
                run_time_hours=row["run_time_hours"],
                failure_count=row["failure_count"],
                mtbf_hours=row["mtbf_hours"]
            )
            save_mtbf(item)

        return {
            "title": "MTBF KPI",
            "kpi": "mtbf",
            "count": len(results),
            "results": results,
        }