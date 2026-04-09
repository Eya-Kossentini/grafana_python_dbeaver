from typing import Optional
from fastapi import APIRouter, Query, Security, HTTPException, Path

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.pareto_losses.repositories.pareto_losses_repository import (
    KPIParetoLossesRepository,
)
from admin.machine_assets.machine_setup.pareto_losses.services.pareto_losses_services import (
    KPIParetoLossesService,
)
from admin.machine_assets.machine_setup.pareto_losses.schemas.pareto_losses_schemas import (
    ParetoLossesResponse,
)


router = APIRouter(
    prefix="/pareto_losses_KPI",
    tags=["Pareto Losses KPI"],
)


def build_pareto_losses_service() -> KPIParetoLossesService:
    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"
    bookings_url = "https://core_demo.momes-solutions.com/bookings/bookings/"

    repository = KPIParetoLossesRepository(
        machine_condition_data_url=machine_condition_data_url,
        bookings_url=bookings_url,
    )

    return KPIParetoLossesService(
        pareto_losses_repository=repository
    )


@router.get("", response_model=ParetoLossesResponse, summary="Get Pareto Losses KPI")
def get_pareto_losses(
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    only_critical: Optional[bool] = Query(default=False, description="Show only Pareto critical losses (<=80%)"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_pareto_losses_service()
        return service.get_pareto_losses(
            date_from=date_from,
            date_to=date_to,
            token=token,
            only_critical=only_critical,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=ParetoLossesResponse, summary="Get Pareto Losses KPI by station")
def get_pareto_losses_by_station(
    station_id: int = Path(description="Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    only_critical: Optional[bool] = Query(default=False, description="Show only Pareto critical losses (<=80%)"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_pareto_losses_service()
        return service.get_pareto_losses(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
            only_critical=only_critical,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")