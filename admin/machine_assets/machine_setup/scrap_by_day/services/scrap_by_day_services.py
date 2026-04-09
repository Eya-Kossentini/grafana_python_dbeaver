from collections import defaultdict
from typing import Optional

from admin.machine_assets.machine_setup.scrap_by_day.repositories.scrap_by_day_repository import (
    KPIScrapByDayRepository,
)


class KPIScrapByDayService:
    def __init__(self, scrap_by_day_repository: KPIScrapByDayRepository) -> None:
        self.scrap_by_day_repository = scrap_by_day_repository

    def get_scrap_by_day(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ):
        bookings_data = self.scrap_by_day_repository.get_bookings_data(token=token)

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

        grouped = defaultdict(lambda: {
            "total_bookings": 0,
            "scrap_count": 0,
        })

        for item in bookings_data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and current_station_id != station_id:
                continue

            production_day = normalize_day(item.get("date_of_booking"))
            if not in_date_range(production_day):
                continue

            state = str(item.get("state", "")).strip().lower()

            key = (production_day, current_station_id)
            grouped[key]["total_bookings"] += 1

            if state == "scrap":
                grouped[key]["scrap_count"] += 1

        results = []

        for (production_day, current_station_id), values in grouped.items():
            total_bookings = values["total_bookings"]
            scrap_count = values["scrap_count"]
            scrap_rate_pct = round((100.0 * scrap_count / total_bookings), 2) if total_bookings > 0 else 0.0

            results.append({
                "production_day": production_day,
                "station_id": current_station_id,
                "total_bookings": total_bookings,
                "scrap_count": scrap_count,
                "scrap_rate_pct": scrap_rate_pct,
            })

        results.sort(key=lambda x: (x["production_day"], x["station_id"]))

        return {
            "title": "Scrap By Day KPI",
            "kpi": "scrap_by_day",
            "count": len(results),
            "results": results,
        }