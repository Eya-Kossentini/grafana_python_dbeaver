from typing import Optional
from fastapi import APIRouter, Query, Security, HTTPException, Path

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.quality.repositories.quality_repository import KPIQualityRepository
from admin.machine_assets.machine_setup.quality.services.quality_services import KPIQualityService
from admin.machine_assets.machine_setup.quality.schemas.quality_schemas import QualityResponse

router = APIRouter(
    prefix="/quality_KPI",
    tags=["Quality KPI"],
)

@router.get(
    "",
    response_model=QualityResponse,
    summary="Get Quality KPI"
)
def get_quality(
    token: str = Security(oauth2_scheme)
):
    try:
        repository = KPIQualityRepository()
        service = KPIQualityService(repository)
        return service.get_quality(
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")
    

@router.get(
    "/by-station/{station_id}",
    response_model=QualityResponse,
    summary="Get Quality KPI by station"
)
def get_quality_by_station(
    station_id: int = Path(description= "Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme)
):
    try:
        repository = KPIQualityRepository()
        service = KPIQualityService(repository)
        return service.get_quality(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")