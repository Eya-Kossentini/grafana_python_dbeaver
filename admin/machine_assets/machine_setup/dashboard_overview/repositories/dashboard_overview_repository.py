from typing import Optional, Dict, Any

from admin.machine_assets.machine_setup.availability.services.availability_services import KPIAvailabilityService
from admin.machine_assets.machine_setup.performance.services.performance_services import KPIPerformanceService
from admin.machine_assets.machine_setup.quality.services.quality_services import KPIQualityService
from admin.machine_assets.machine_setup.oee.services.oee_services import KPIOeeService
from admin.machine_assets.machine_setup.mtbf.services.mtbf_services import KPIMTBFService
from admin.machine_assets.machine_setup.mttr.services.mttr_services import KPIMTTRService


class DashboardOverviewRepository:
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

    def fetch_kpis(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "oee": self.oee_service.get_oee(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            ),
            "availability": self.availability_service.get_availability(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            ),
            "performance": self.performance_service.get_performance(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            ),
            "quality": self.quality_service.get_quality(
                station_id=station_id,
                date_from=date_from,
                date_to=date_to,
                token=token,
            ),
            "mtbf": self.mtbf_service.get_mtbf(
                station_id=station_id,
                token=token,
            ),
            "mttr": self.mttr_service.get_mttr(
                station_id=station_id,
                token=token,
            ),
        }