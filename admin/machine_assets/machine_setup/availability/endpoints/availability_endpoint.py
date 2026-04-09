from typing import Optional
from fastapi import APIRouter, Path, Query, Security, HTTPException, Depends

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.availability.repositories.availability_repository import KPIAvailabilityRepository
from admin.machine_assets.machine_setup.availability.services.availability_services import KPIAvailabilityService
from admin.machine_assets.machine_setup.availability.schemas.availability_schemas import AvailabilityResponse

router = APIRouter(
    prefix="/availability_KPI",
    tags=["Availability KPI"],
)


@router.get(
    "",
    response_model=AvailabilityResponse,
    summary="Get Availability KPI"
)
def get_availability(
   token: str = Security(oauth2_scheme),
):
    try:
        repository = KPIAvailabilityRepository()
        service = KPIAvailabilityService(repository)
        
        return service.get_availability(
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get(
    "/by-station/{station_id}",
    response_model=AvailabilityResponse,
    summary="Get Availability KPI by station"
)
def get_availability_by_station(
    station_id: int = Path(description= "Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        repository = KPIAvailabilityRepository()
        service = KPIAvailabilityService(repository)
        
        return service.get_availability(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))