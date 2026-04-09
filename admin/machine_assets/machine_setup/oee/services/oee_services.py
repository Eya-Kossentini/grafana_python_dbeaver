from typing import Optional
from fastapi import HTTPException

from admin.machine_assets.machine_setup.availability.services.availability_services import KPIAvailabilityService
from admin.machine_assets.machine_setup.performance.services.performance_services import KPIPerformanceService
from admin.machine_assets.machine_setup.quality.services.quality_services import KPIQualityService


class KPIOeeService:
    def __init__(
        self,
        kpi_availability_service: KPIAvailabilityService,
        kpi_performance_service: KPIPerformanceService,
        kpi_quality_service: KPIQualityService,
    ) -> None:
        self.kpi_availability_service = kpi_availability_service
        self.kpi_performance_service = kpi_performance_service
        self.kpi_quality_service = kpi_quality_service

    def get_oee(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ):
        def normalize_day(value):
            return str(value)[:10] if value is not None else None

        try:
            availability_data = self.kpi_availability_service.get_availability(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            )
        except HTTPException as e:
            if e.status_code == 404:
                availability_data = {
                    "title": "Availability KPI",
                    "kpi": "availability",
                    "count": 0,
                    "results": []
                }
            else:
                raise

        try:
            performance_data = self.kpi_performance_service.get_performance(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            )
        except HTTPException as e:
            if e.status_code == 404:
                performance_data = {
                    "title": "Performance KPI",
                    "kpi": "performance",
                    "count": 0,
                    "results": []
                }
            else:
                raise

        try:
            quality_data = self.kpi_quality_service.get_quality(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            )
        except HTTPException as e:
            if e.status_code == 404:
                quality_data = {
                    "title": "Quality KPI",
                    "kpi": "quality",
                    "count": 0,
                    "results": []
                }
            else:
                raise

        availability_map = {
            (normalize_day(item["production_day"]), item["station_id"]): item
            for item in availability_data.get("results", [])
        }

        performance_map = {
            (normalize_day(item["production_day"]), item["station_id"]): item
            for item in performance_data.get("results", [])
        }

        quality_map = {
            (normalize_day(item["production_day"]), item["station_id"]): item
            for item in quality_data.get("results", [])
        }

        all_keys = (
            set(availability_map.keys())
            | set(performance_map.keys())
            | set(quality_map.keys())
        )

        results = []

        for key in sorted(all_keys):
            production_day, current_station_id = key

            availability_item = availability_map.get(key, {})
            performance_item = performance_map.get(key, {})
            quality_item = quality_map.get(key, {})

            availability_pct = float(availability_item.get("availability_pct", 0) or 0)
            performance_pct = float(performance_item.get("performance_pct", 0) or 0)

            quality_exists = key in quality_map
            quality_pct = float(quality_item.get("quality_pct", 0) or 0)

            oee_pct = round((availability_pct * performance_pct * quality_pct) / 10000, 2)

            if not (availability_pct > 0 or performance_pct > 0 or quality_pct > 0):
                continue

            results.append({
                "production_day": production_day,
                "station_id": current_station_id,
                "availability_pct": round(availability_pct, 2),
                "performance_pct": round(performance_pct, 2),
                "quality_pct": round(quality_pct, 2),
                "quality_missing": not quality_exists,
                "oee_pct": oee_pct,
            })

        return {
            "title": "OEE KPI",
            "kpi": "oee",
            "count": len(results),
            "results": results,
        }