from typing import Optional
from fastapi import APIRouter, Query, Path, Security, HTTPException

from admin.dependencies import oauth2_scheme

from admin.machine_assets.machine_setup.pareto_losses.repositories.pareto_losses_repository import (
    KPIParetoLossesRepository,
)
from admin.machine_assets.machine_setup.pareto_losses.services.pareto_losses_services import (
    KPIParetoLossesService,
)

from admin.machine_assets.machine_setup.failure_loss_diagnostic.repositories.failure_loss_diagnostic_repository import (
    KPIFailureLossDiagnosticRepository,
)
from admin.machine_assets.machine_setup.failure_loss_diagnostic.services.failure_loss_diagnostic_services import (
    KPIFailureLossDiagnosticService,
)
from admin.machine_assets.machine_setup.failure_loss_diagnostic.schemas.failure_loss_diagnostic_schemas import (
    FailureLossDiagnosticResponse,
)


router = APIRouter(
    prefix="/failure_loss_diagnostic_KPI",
    tags=["Failure Loss Diagnostic KPI"],
)


def build_failure_loss_diagnostic_service() -> KPIFailureLossDiagnosticService:
    bookings_url = "https://core_demo.momes-solutions.com/bookings/bookings/"
    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"

    diagnostic_repository = KPIFailureLossDiagnosticRepository(
        bookings_url=bookings_url,
    )

    pareto_losses_repository = KPIParetoLossesRepository(
        machine_condition_data_url=machine_condition_data_url,
        bookings_url=bookings_url,
    )
    pareto_losses_service = KPIParetoLossesService(
        pareto_losses_repository=pareto_losses_repository
    )

    return KPIFailureLossDiagnosticService(
        failure_loss_diagnostic_repository=diagnostic_repository,
        pareto_losses_service=pareto_losses_service,
    )


@router.get("", response_model=FailureLossDiagnosticResponse, summary="Get Failure Loss Diagnostic KPI")
def get_failure_loss_diagnostic(
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_failure_loss_diagnostic_service()
        return service.get_failure_loss_diagnostic(
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=FailureLossDiagnosticResponse, summary="Get Failure Loss Diagnostic KPI by station")
def get_failure_loss_diagnostic_by_station(
    station_id: int = Path(description="Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_failure_loss_diagnostic_service()
        return service.get_failure_loss_diagnostic(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")