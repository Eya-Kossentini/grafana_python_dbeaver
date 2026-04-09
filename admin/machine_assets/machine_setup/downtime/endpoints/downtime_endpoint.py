from typing import Optional
from fastapi import APIRouter, Query, Path, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.downtime.repositories.downtime_repository import (
    KPIDowntimeRepository,
)
from admin.machine_assets.machine_setup.downtime.services.downtime_services import (
    KPIDowntimeService,
)
from admin.machine_assets.machine_setup.downtime.schemas.downtime_schemas import (
    DowntimeByStationResponse,
    DowntimeTypeEnum,
)


router = APIRouter(
    prefix="/downtime_by_station_KPI",
    tags=["Downtime By Station KPI"],
)


def build_downtime_service() -> KPIDowntimeService:
    machine_conditions_url = "https://core_demo.momes-solutions.com/machine-conditions/machine-conditions/"
    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"

    repository = KPIDowntimeRepository(
        machine_conditions_url=machine_conditions_url,
        machine_condition_data_url=machine_condition_data_url,
    )

    return KPIDowntimeService(downtime_repository=repository)


@router.get("", response_model=DowntimeByStationResponse, summary="Get Downtime By Station KPI")
def get_downtime_by_station(
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    downtime_type: Optional[DowntimeTypeEnum] = Query(default=None, description="Downtime type"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_downtime_service()
        return service.get_downtime_by_station(
            date_from=date_from,
            date_to=date_to,
            downtime_type=downtime_type.value if downtime_type else None,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=DowntimeByStationResponse, summary="Get Downtime By Station KPI by station")
def get_downtime_by_station_id(
    station_id: int = Path(description="Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    downtime_type: Optional[DowntimeTypeEnum] = Query(default=None, description="Downtime type"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_downtime_service()
        return service.get_downtime_by_station(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            downtime_type=downtime_type.value if downtime_type else None,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")