from typing import Optional
from fastapi import APIRouter, Query, Security, HTTPException, Path

from admin.dependencies import oauth2_scheme

from admin.machine_assets.machine_setup.availability.repositories.availability_repository import KPIAvailabilityRepository
from admin.machine_assets.machine_setup.performance.repositories.performance_repository import KPIPerformanceRepository
from admin.machine_assets.machine_setup.quality.repositories.quality_repository import KPIQualityRepository

from admin.machine_assets.machine_setup.availability.services.availability_services import KPIAvailabilityService
from admin.machine_assets.machine_setup.performance.services.performance_services import KPIPerformanceService
from admin.machine_assets.machine_setup.quality.services.quality_services import KPIQualityService
from admin.machine_assets.machine_setup.oee.services.oee_services import KPIOeeService

from admin.machine_assets.machine_setup.oee.schemas.oee_schemas import OeeResponse


router = APIRouter(
    prefix="/oee_KPI",
    tags=["OEE KPI"],
)


def build_oee_service() -> KPIOeeService:
    availability_repository = KPIAvailabilityRepository()
    performance_repository = KPIPerformanceRepository()
    quality_repository = KPIQualityRepository()

    availability_service = KPIAvailabilityService(availability_repository)
    performance_service = KPIPerformanceService(performance_repository)
    quality_service = KPIQualityService(quality_repository)

    return KPIOeeService(
        kpi_availability_service=availability_service,
        kpi_performance_service=performance_service,
        kpi_quality_service=quality_service,
    )


@router.get("", response_model=OeeResponse, summary="Get OEE KPI")
def get_oee(
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_oee_service()
        return service.get_oee(token=token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=OeeResponse, summary="Get OEE KPI by station")
def get_oee_by_station(
    station_id: int = Path(description="Station ID"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_oee_service()
        return service.get_oee(
            station_id=station_id,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")