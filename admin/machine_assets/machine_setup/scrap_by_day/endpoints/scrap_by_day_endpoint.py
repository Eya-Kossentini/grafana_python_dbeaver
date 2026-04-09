from typing import Optional
from fastapi import APIRouter, Query, Path, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.scrap_by_day.repositories.scrap_by_day_repository import (
    KPIScrapByDayRepository,
)
from admin.machine_assets.machine_setup.scrap_by_day.services.scrap_by_day_services import (
    KPIScrapByDayService,
)
from admin.machine_assets.machine_setup.scrap_by_day.schemas.scrap_by_day_schemas import (
    ScrapByDayResponse,
)


router = APIRouter(
    prefix="/scrap_by_day_KPI",
    tags=["Scrap By Day KPI"],
)


def build_scrap_by_day_service() -> KPIScrapByDayService:
    bookings_url = "https://core_demo.momes-solutions.com/bookings/bookings/"
    repository = KPIScrapByDayRepository(bookings_url=bookings_url)
    return KPIScrapByDayService(scrap_by_day_repository=repository)


@router.get("", response_model=ScrapByDayResponse, summary="Get Scrap By Day KPI")
def get_scrap_by_day(
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_scrap_by_day_service()
        return service.get_scrap_by_day(
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=ScrapByDayResponse, summary="Get Scrap By Day KPI by station")
def get_scrap_by_day_by_station(
    station_id: int = Path(description="Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_scrap_by_day_service()
        return service.get_scrap_by_day(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")