from collections import defaultdict
from datetime import datetime
from typing import Optional
import logging

from fastapi import HTTPException

from admin.machine_assets.machine_setup.quality.repositories.quality_repository import KPIQualityRepository
from admin.machine_assets.machine_setup.quality.schemas.quality_schemas import QualityItem
from admin.db_timescale import save_quality

logger = logging.getLogger(__name__)


class KPIQualityService:
    def __init__(self, kpi_quality_repository: KPIQualityRepository) -> None:
        self.kpi_quality_repository = kpi_quality_repository

    @staticmethod
    def _parse_datetime(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    @classmethod
    def _parse_date(cls, value):
        """Accepte 'YYYY-MM-DD' ou ISO datetime, retourne une date ou None."""
        if not value:
            return None
        dt = cls._parse_datetime(value)
        if dt is not None:
            return dt.date()
        raise HTTPException(status_code=400, detail=f"Invalid date format: {value}")

    def get_quality(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ):
        # Parser les filtres de date en amont
        filter_date_from = self._parse_date(date_from)
        filter_date_to = self._parse_date(date_to)

        bookings = self.kpi_quality_repository.get_bookings(
            start_date=date_from,
            end_date=date_to,
            station_id=station_id,
            token=token,
        )
        machine_condition_data = self.kpi_quality_repository.get_machine_condition_data(
            start_date=date_from,
            end_date=date_to,
            station_id=station_id,
            token=token,
        )

        logger.info(
            "Quality KPI fetch — bookings=%d, mcd=%d, station=%s, from=%s, to=%s",
            len(bookings), len(machine_condition_data), station_id, date_from, date_to,
        )

        # Option B : on garde uniquement les stations connues côté MCD
        mcd_stations = set()
        for row in machine_condition_data:
            station = row.get("station_id")
            if station is None:
                continue
            try:
                mcd_stations.add(int(station))
            except (ValueError, TypeError):
                continue

        # Agréger les bookings sur (production_day, station_id)
        quality_data = defaultdict(lambda: {
            "total_bookings": 0,
            "good_count": 0,
            "fail_count": 0,
            "scrap_count": 0,
        })

        for booking in bookings:
            b_station_raw = booking.get("station_id")
            b_date_raw = booking.get("date_of_booking")

            if b_station_raw is None or b_date_raw is None:
                continue

            try:
                b_station = int(b_station_raw)
            except (ValueError, TypeError):
                continue

            # Filtre station explicite (endpoint by-station)
            if station_id is not None and b_station != int(station_id):
                continue

            # Filtre par les stations connues du MCD
            if b_station not in mcd_stations:
                continue

            b_dt = self._parse_datetime(b_date_raw)
            if b_dt is None:
                continue

            production_day = b_dt.date()

            # Filtres de date applicatifs (sécurité si l'API amont ne les respecte pas)
            if filter_date_from and production_day < filter_date_from:
                continue
            if filter_date_to and production_day > filter_date_to:
                continue

            key = (production_day, b_station)
            quality_data[key]["total_bookings"] += 1

            state = str(booking.get("state", "")).strip().lower()
            if state == "pass":
                quality_data[key]["good_count"] += 1
            elif state == "fail":
                quality_data[key]["fail_count"] += 1
            elif state == "scrap":
                quality_data[key]["scrap_count"] += 1

        if not quality_data:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No quality KPI data found "
                    f"(bookings={len(bookings)}, mcd_stations={len(mcd_stations)})"
                ),
            )

        results = []
        for (production_day, current_station_id), v in sorted(quality_data.items()):
            total = v["total_bookings"]
            good = v["good_count"]
            fail = v["fail_count"]
            scrap = v["scrap_count"]

            results.append({
                "production_day": production_day,
                "station_id": current_station_id,
                "total_bookings": total,
                "good_count": good,
                "fail_count": fail,
                "scrap_count": scrap,
                "quality_pct": round(100.0 * good / total, 2),
                "defect_rate_pct": round(100.0 * (fail + scrap) / total, 2),
            })

        for row in results:
            save_quality(QualityItem(**row))

        return {
            "title": "Quality KPI",
            "kpi": "quality",
            "count": len(results),
            "results": results,
        }