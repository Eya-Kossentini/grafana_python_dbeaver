from typing import Optional
from fastapi import APIRouter, Path, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.mtbf.repositories.mtbf_repository import KPIMTBFRepository
from admin.machine_assets.machine_setup.mtbf.services.mtbf_services import KPIMTBFService
from admin.machine_assets.machine_setup.mtbf.schemas.mtbf_schemas import MTBFResponse


router = APIRouter(
    prefix="/mtbf_KPI",
    tags=["MTBF KPI"],
)


def build_mtbf_service() -> KPIMTBFService:
    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"
    repository = KPIMTBFRepository(machine_condition_data_url=machine_condition_data_url)
    return KPIMTBFService(mtbf_repository=repository)


@router.get("", response_model=MTBFResponse, summary="Get MTBF KPI")
def get_mtbf(
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_mtbf_service()
        return service.get_mtbf(token=token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=MTBFResponse, summary="Get MTBF KPI by station")
def get_mtbf_by_station(
    station_id: int = Path(description="Station ID"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_mtbf_service()
        return service.get_mtbf(station_id=station_id, token=token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")