from typing import Optional
from fastapi import APIRouter, Query, Security, HTTPException, Depends, Path

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.line_quality.services.line_quality_services import KPILineProductionQualityService
from admin.machine_assets.machine_setup.line_quality.repositories.line_quality_repository import KPILineProductionQualityRepository
from admin.machine_assets.machine_setup.line_quality.schemas.line_quality_schemas import LineProductionQualityResponse

router = APIRouter(
    prefix="/line-production-quality",
    tags=["kpi_Line_Production_Quality"],
)


def get_line_production_quality_service() -> KPILineProductionQualityService:
    repository = KPILineProductionQualityRepository(
        bookings_url="https://core_demo.momes-solutions.com/bookings/bookings/",
        lines_by_station_base_url="https://core_demo.momes-solutions.com/lines/lines/lines/by-station"
    )
    return KPILineProductionQualityService(repository)


@router.get(
    "/by-station/{station_id}",
    response_model=LineProductionQualityResponse,
    summary="Get production quality by station"
)
def get_line_production_quality_by_station(
    station_id: int = Path(description= "Station ID"),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    token: str = Security(oauth2_scheme),
    service: KPILineProductionQualityService = Depends(get_line_production_quality_service)
): 
    try:
        return service.get_line_production_quality(
            station_id=station_id,
            line_id=None,
            start_date=start_date,
            end_date=end_date,
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")
    

@router.get(
    "/by-line/{line_id}",
    response_model=LineProductionQualityResponse,
    summary="Get production quality by line"
)
def get_line_production_quality_by_line(
    line_id: int = Path(description= "Line ID"),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    token: str = Security(oauth2_scheme),
    service: KPILineProductionQualityService = Depends(get_line_production_quality_service)
): 
    try:
        return service.get_line_production_quality(
            station_id=None,
            line_id=line_id,
            start_date=start_date,
            end_date=end_date,
            token=token
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")