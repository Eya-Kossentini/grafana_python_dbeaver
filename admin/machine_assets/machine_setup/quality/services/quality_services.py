from collections import defaultdict
from datetime import datetime
from typing import Optional
from fastapi import HTTPException

from admin.machine_assets.machine_setup.quality.repositories.quality_repository import KPIQualityRepository


class KPIQualityService:
    def __init__(self, kpi_quality_repository: KPIQualityRepository) -> None:
        self.kpi_quality_repository = kpi_quality_repository

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

    def get_quality(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None
    ):
        bookings = self.kpi_quality_repository.get_bookings(
            start_date=date_from,
            end_date=date_to,
            station_id=station_id,
            token=token
        )

        filter_date_from = self._parse_date(date_from)
        filter_date_to = self._parse_date(date_to)

        quality_data = defaultdict(lambda: {
            "total_bookings": 0,
            "good_count": 0,
            "fail_count": 0,
            "scrap_count": 0,
        })

        for booking in bookings:
            booking_station_id = booking.get("station_id")
            booking_date_raw = booking.get("date_of_booking")

            if booking_station_id is None or booking_date_raw is None:
                continue

            if station_id is not None and booking_station_id != station_id:
                continue

            booking_dt = self._parse_datetime(booking_date_raw)
            if booking_dt is None:
                continue

            production_day = booking_dt.date()

            if filter_date_from and production_day < filter_date_from:
                continue
            if filter_date_to and production_day > filter_date_to:
                continue

            key = (production_day, booking_station_id)
            quality_data[key]["total_bookings"] += 1

            state = str(booking.get("state", "")).strip().lower()

            if state == "pass":
                quality_data[key]["good_count"] += 1
            elif state == "fail":
                quality_data[key]["fail_count"] += 1
            elif state == "scrap":
                quality_data[key]["scrap_count"] += 1

        if not quality_data:
            raise HTTPException(status_code=404, detail="No quality KPI data found")

        results = []
        for (production_day, current_station_id), values in sorted(quality_data.items()):
            total_bookings = values["total_bookings"]
            good_count = values["good_count"]
            fail_count = values["fail_count"]
            scrap_count = values["scrap_count"]

            quality_pct = round((100.0 * good_count / total_bookings), 2) if total_bookings > 0 else None
            defect_rate_pct = round((100.0 * (fail_count + scrap_count) / total_bookings), 2) if total_bookings > 0 else None

            results.append({
                "production_day": production_day,
                "station_id": current_station_id,
                "total_bookings": total_bookings,
                "good_count": good_count,
                "fail_count": fail_count,
                "scrap_count": scrap_count,
                "quality_pct": quality_pct,
                "defect_rate_pct": defect_rate_pct,
            })

        return {
            "title": "Quality KPI",
            "kpi": "quality",
            "count": len(results),
            "results": results
        }