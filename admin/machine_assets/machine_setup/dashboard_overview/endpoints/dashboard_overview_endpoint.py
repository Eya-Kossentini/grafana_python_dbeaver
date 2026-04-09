from typing import Optional
from fastapi import APIRouter, Path, Query, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.dashboard_overview.services.dashboard_overview_services import (
    build_dashboard_overview_service,
)
from admin.machine_assets.machine_setup.dashboard_overview.schemas.dashboard_overview_schemas import (
    DashboardOverviewResponse,
)

router = APIRouter(
    prefix="/dashboard_overview_KPI",
    tags=["Dashboard Overview KPI"],
)


@router.get(
    "",
    response_model=DashboardOverviewResponse,
    summary="Get Dashboard Overview KPI"
)
def get_dashboard_overview(
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_dashboard_overview_service()
        return service.get_dashboard_overview(
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get(
    "/by-station/{station_id}",
    response_model=DashboardOverviewResponse,
    summary="Get Dashboard Overview KPI by station"
)
def get_dashboard_overview_by_station(
    station_id: int = Path(description="Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_dashboard_overview_service()
        return service.get_dashboard_overview(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")