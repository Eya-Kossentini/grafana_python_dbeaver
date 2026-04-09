from typing import Optional, Dict, Any, List
from fastapi import HTTPException

from admin.machine_assets.machine_setup.line_quality.repositories.line_quality_repository import KPILineProductionQualityRepository
from admin.machine_assets.machine_setup.line_quality.schemas.line_quality_schemas import (
    LineProductionQualityItem,
    LineProductionQualityResponse,
)


class KPILineProductionQualityService:
    def __init__(self, repository: KPILineProductionQualityRepository) -> None:
        self.repository = repository

    def get_line_production_quality(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        station_id: Optional[int] = None,
        line_id: Optional[int] = None,
        token: Optional[str] = None
    ) -> LineProductionQualityResponse:
        try:
            bookings = self.repository.get_bookings(
                start_date=start_date,
                end_date=end_date,
                station_id=station_id,
                token=token
            )

            if not isinstance(bookings, list):
                raise HTTPException(
                    status_code=500,
                    detail=f"Bookings must be a list, got {type(bookings).__name__}"
                )

            unique_station_ids = set()
            for booking in bookings:
                if not isinstance(booking, dict):
                    continue

                st_id = booking.get("station_id")
                if st_id is None:
                    continue

                try:
                    unique_station_ids.add(int(st_id))
                except (TypeError, ValueError):
                    continue

            station_to_lines: Dict[int, List[Dict[str, Any]]] = {}

            for st_id in unique_station_ids:
                lines = self.repository.get_lines_by_station(
                    station_id=st_id,
                    token=token
                )

                if not isinstance(lines, list):
                    continue

                valid_lines = []
                for line in lines:
                    if isinstance(line, dict):
                        valid_lines.append(line)

                station_to_lines[st_id] = valid_lines

            grouped: Dict[int, Dict[str, Any]] = {}

            for booking in bookings:
                if not isinstance(booking, dict):
                    continue

                st_id = booking.get("station_id")
                state = str(booking.get("state", "")).strip().lower()

                if st_id is None:
                    continue

                try:
                    st_id = int(st_id)
                except (TypeError, ValueError):
                    continue

                lines_for_station = station_to_lines.get(st_id, [])
                if not lines_for_station:
                    continue

                for line in lines_for_station:
                    current_line_id = line.get("id")
                    if current_line_id is None:
                        continue

                    try:
                        current_line_id = int(current_line_id)
                    except (TypeError, ValueError):
                        continue

                    if line_id is not None and current_line_id != line_id:
                        continue

                    if current_line_id not in grouped:
                        grouped[current_line_id] = {
                            "line_id": current_line_id,
                            "line_name": line.get("name"),
                            "description": line.get("description"),
                            "total_bookings": 0,
                            "good_count": 0,
                            "fail_count": 0,
                            "scrap_count": 0,
                            "quality_rate_pct": 0.0,
                        }

                    grouped[current_line_id]["total_bookings"] += 1

                    if state == "pass":
                        grouped[current_line_id]["good_count"] += 1
                    elif state == "fail":
                        grouped[current_line_id]["fail_count"] += 1
                    elif state == "scrap":
                        grouped[current_line_id]["scrap_count"] += 1

            results: List[LineProductionQualityItem] = []

            for item in grouped.values():
                total = item["total_bookings"]
                good = item["good_count"]

                item["quality_rate_pct"] = round((good * 100.0 / total), 2) if total > 0 else 0.0
                results.append(LineProductionQualityItem(**item))

            results.sort(key=lambda x: x.line_id)

            return LineProductionQualityResponse(
                title="Line Production Quality",
                kpi="line_production_quality",
                count=len(results),
                results=results
            )

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Service error: {str(e)}"
            )