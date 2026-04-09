from typing import Optional
from fastapi import HTTPException

from admin.machine_assets.machine_setup.availability.repositories.availability_repository import KPIAvailabilityRepository
from admin.machine_assets.machine_setup.performance.repositories.performance_repository import KPIPerformanceRepository
from admin.machine_assets.machine_setup.quality.repositories.quality_repository import KPIQualityRepository
from admin.machine_assets.machine_setup.mtbf.repositories.mtbf_repository import KPIMTBFRepository
from admin.machine_assets.machine_setup.mttr.repositories.mttr_repository import KPIMTTRRepository

from admin.machine_assets.machine_setup.availability.services.availability_services import KPIAvailabilityService
from admin.machine_assets.machine_setup.performance.services.performance_services import KPIPerformanceService
from admin.machine_assets.machine_setup.quality.services.quality_services import KPIQualityService
from admin.machine_assets.machine_setup.oee.services.oee_services import KPIOeeService
from admin.machine_assets.machine_setup.mtbf.services.mtbf_services import KPIMTBFService
from admin.machine_assets.machine_setup.mttr.services.mttr_services import KPIMTTRService


class KPIDashboardOverviewService:
    def __init__(
        self,
        oee_service: KPIOeeService,
        availability_service: KPIAvailabilityService,
        performance_service: KPIPerformanceService,
        quality_service: KPIQualityService,
        mtbf_service: KPIMTBFService,
        mttr_service: KPIMTTRService,
    ) -> None:
        self.oee_service = oee_service
        self.availability_service = availability_service
        self.performance_service = performance_service
        self.quality_service = quality_service
        self.mtbf_service = mtbf_service
        self.mttr_service = mttr_service

    def get_dashboard_overview(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ):
        def normalize_day(value):
            return str(value)[:10] if value is not None else None

        # Safe fetch pattern, same idea as OEE aggregation
        try:
            oee_data = self.oee_service.get_oee(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            )
        except HTTPException as e:
            if e.status_code == 404:
                oee_data = {"title": "OEE KPI", "kpi": "oee", "count": 0, "results": []}
            else:
                raise

        try:
            availability_data = self.availability_service.get_availability(
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
                    "results": [],
                }
            else:
                raise

        try:
            performance_data = self.performance_service.get_performance(
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
                    "results": [],
                }
            else:
                raise

        try:
            quality_data = self.quality_service.get_quality(
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
                    "results": [],
                }
            else:
                raise

        try:
            mtbf_data = self.mtbf_service.get_mtbf(
                station_id=station_id,
                token=token,
            )
        except HTTPException as e:
            if e.status_code == 404:
                mtbf_data = {"title": "MTBF KPI", "kpi": "mtbf", "count": 0, "results": []}
            else:
                raise

        try:
            mttr_data = self.mttr_service.get_mttr(
                station_id=station_id,
                token=token,
            )
        except HTTPException as e:
            if e.status_code == 404:
                mttr_data = {"title": "MTTR KPI", "kpi": "mttr", "count": 0, "results": []}
            else:
                raise

        oee_map = {
            (normalize_day(item.get("production_day")), item["station_id"]): item
            for item in oee_data.get("results", [])
        }

        availability_map = {
            (normalize_day(item.get("production_day")), item["station_id"]): item
            for item in availability_data.get("results", [])
        }

        performance_map = {
            (normalize_day(item.get("production_day")), item["station_id"]): item
            for item in performance_data.get("results", [])
        }

        quality_map = {
            (normalize_day(item.get("production_day")), item["station_id"]): item
            for item in quality_data.get("results", [])
        }

        mtbf_map = {
            item["station_id"]: item
            for item in mtbf_data.get("results", [])
        }

        mttr_map = {
            item["station_id"]: item
            for item in mttr_data.get("results", [])
        }

        all_keys = (
            set(oee_map.keys())
            | set(availability_map.keys())
            | set(performance_map.keys())
            | set(quality_map.keys())
        )

        results = []
        for key in sorted(all_keys):
            production_day, current_station_id = key

            oee_item = oee_map.get(key, {})
            availability_item = availability_map.get(key, {})
            performance_item = performance_map.get(key, {})
            quality_item = quality_map.get(key, {})

            mtbf_item = mtbf_map.get(current_station_id, {})
            mttr_item = mttr_map.get(current_station_id, {})

            results.append({
                "station_id": current_station_id,
                "production_day": production_day,
                "oee_pct": round(float(oee_item.get("oee_pct", 0) or 0), 2),
                "availability_pct": round(float(availability_item.get("availability_pct", 0) or 0), 2),
                "performance_pct": round(float(performance_item.get("performance_pct", 0) or 0), 2),
                "quality_pct": round(float(quality_item.get("quality_pct", 0) or 0), 2),
                "mtbf_hours": mtbf_item.get("mtbf_hours"),
                "mttr_hours": mttr_item.get("mttr_hours"),
            })

        return {
            "title": "Dashboard Overview KPI",
            "kpi": "dashboard_overview",
            "count": len(results),
            "results": results,
        }


def build_dashboard_overview_service() -> KPIDashboardOverviewService:
    availability_repository = KPIAvailabilityRepository()
    performance_repository = KPIPerformanceRepository()
    quality_repository = KPIQualityRepository()

    availability_service = KPIAvailabilityService(availability_repository)
    performance_service = KPIPerformanceService(performance_repository)
    quality_service = KPIQualityService(quality_repository)

    oee_service = KPIOeeService(
        kpi_availability_service=availability_service,
        kpi_performance_service=performance_service,
        kpi_quality_service=quality_service,
    )

    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"

    mtbf_repository = KPIMTBFRepository(
        machine_condition_data_url=machine_condition_data_url
    )
    mttr_repository = KPIMTTRRepository(
        machine_condition_data_url=machine_condition_data_url
    )

    mtbf_service = KPIMTBFService(mtbf_repository=mtbf_repository)
    mttr_service = KPIMTTRService(mttr_repository=mttr_repository)

    return KPIDashboardOverviewService(
        oee_service=oee_service,
        availability_service=availability_service,
        performance_service=performance_service,
        quality_service=quality_service,
        mtbf_service=mtbf_service,
        mttr_service=mttr_service,
    )