from typing import Optional, Dict, Any, List
from fastapi import HTTPException

from admin.machine_assets.machine_setup.defect_rate.repositories.defect_rate_repository import KPIDefectRateRepository
from admin.machine_assets.machine_setup.defect_rate.schemas.defect_rate_schemas import (
    DefectRateItem,
    DefectRateResponse,
)


class KPIDefectRateService:
    def __init__(self, kpi_defect_rate_repository: KPIDefectRateRepository) -> None:
        self.kpi_defect_rate_repository = kpi_defect_rate_repository

    def get_defect_rate(
        self,
        station_id: Optional[int] = None,
        token: Optional[str] = None
    ) -> DefectRateResponse:
        try:
            bookings = self.kpi_defect_rate_repository.get_bookings(
                station_id=station_id,
                token=token
            )

            if not isinstance(bookings, list):
                raise HTTPException(
                    status_code=500,
                    detail=f"Bookings must be a list, got {type(bookings).__name__}"
                )

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
                except (ValueError, TypeError):
                    continue

                if st_id not in grouped:
                    grouped[st_id] = {
                        "station_id": st_id,
                        "total_bookings": 0,
                        "good_count": 0,
                        "fail_count": 0,
                        "scrap_count": 0,
                        "defect_count": 0,
                        "defect_rate_pct": 0.0,
                    }

                grouped[st_id]["total_bookings"] += 1

                if state == "pass":
                    grouped[st_id]["good_count"] += 1
                elif state == "fail":
                    grouped[st_id]["fail_count"] += 1
                    grouped[st_id]["defect_count"] += 1
                elif state == "scrap":
                    grouped[st_id]["scrap_count"] += 1
                    grouped[st_id]["defect_count"] += 1

            results: List[DefectRateItem] = []

            for item in grouped.values():
                total = item["total_bookings"]
                defects = item["defect_count"]
                item["defect_rate_pct"] = round((defects * 100.0 / total), 2) if total > 0 else 0.0
                results.append(DefectRateItem(**item))

            results.sort(key=lambda x: x.station_id)

            return DefectRateResponse(
                title="Defect Rate KPI by Station",
                kpi="defect_rate",
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