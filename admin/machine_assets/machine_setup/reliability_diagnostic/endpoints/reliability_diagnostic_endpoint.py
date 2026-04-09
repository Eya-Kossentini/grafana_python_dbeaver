from typing import Optional
from fastapi import APIRouter, Query, Path, Security, HTTPException

from admin.dependencies import oauth2_scheme

from admin.machine_assets.machine_setup.mtbf.repositories.mtbf_repository import KPIMTBFRepository
from admin.machine_assets.machine_setup.mtbf.services.mtbf_services import KPIMTBFService

from admin.machine_assets.machine_setup.pareto_losses.repositories.pareto_losses_repository import KPIParetoLossesRepository
from admin.machine_assets.machine_setup.pareto_losses.services.pareto_losses_services import KPIParetoLossesService

from admin.machine_assets.machine_setup.reliability_diagnostic.services.reliability_diagnostic_services import (
    KPIReliabilityDiagnosticService,
)
from admin.machine_assets.machine_setup.reliability_diagnostic.schemas.reliability_diagnostic_schemas import (
    ReliabilityDiagnosticResponse,
)


router = APIRouter(
    prefix="/reliability_diagnostic_KPI",
    tags=["Reliability Diagnostic KPI"],
)


def build_reliability_diagnostic_service() -> KPIReliabilityDiagnosticService:
    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"
    bookings_url = "https://core_demo.momes-solutions.com/bookings/bookings/"

    mtbf_repository = KPIMTBFRepository(
        machine_condition_data_url=machine_condition_data_url
    )
    mtbf_service = KPIMTBFService(mtbf_repository=mtbf_repository)

    pareto_repository = KPIParetoLossesRepository(
        machine_condition_data_url=machine_condition_data_url,
        bookings_url=bookings_url,
    )
    pareto_service = KPIParetoLossesService(
        pareto_losses_repository=pareto_repository
    )

    return KPIReliabilityDiagnosticService(
        mtbf_service=mtbf_service,
        pareto_losses_service=pareto_service,
    )


@router.get("", response_model=ReliabilityDiagnosticResponse, summary="Get Reliability Diagnostic KPI")
def get_reliability_diagnostic(
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_reliability_diagnostic_service()
        return service.get_reliability_diagnostic(
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=ReliabilityDiagnosticResponse, summary="Get Reliability Diagnostic KPI by station")
def get_reliability_diagnostic_by_station(
    station_id: int = Path(description="Station ID"),
    date_from: Optional[str] = Query(default=None, description="Start date in format YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="End date in format YYYY-MM-DD"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_reliability_diagnostic_service()
        return service.get_reliability_diagnostic(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")