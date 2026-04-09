from typing import Optional
from fastapi import APIRouter, Path, Query, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.performance.repositories.performance_repository import KPIPerformanceRepository
from admin.machine_assets.machine_setup.performance.services.performance_services import KPIPerformanceService
from admin.machine_assets.machine_setup.performance.schemas.performance_schemas import PerformanceResponse

router = APIRouter(
    prefix="/performance_KPI",
    tags=["Performance KPI"],
)


@router.get(
    "",
    response_model=PerformanceResponse,
    summary="Get Performance KPI"
)
def get_performance(
    token: str = Security(oauth2_scheme),
):
    try:
        repository = KPIPerformanceRepository()
        service = KPIPerformanceService(repository)
        return service.get_performance(
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")
    

@router.get(
    "/by-station/{station_id}",
    response_model=PerformanceResponse,
    summary="Get Performance KPI by station"
)
def get_performance(
    station_id: int = Path(description= "Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        repository = KPIPerformanceRepository()
        service = KPIPerformanceService(repository)
        return service.get_performance(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")