from typing import Optional
from fastapi import APIRouter, Path, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.mttr.repositories.mttr_repository import KPIMTTRRepository
from admin.machine_assets.machine_setup.mttr.services.mttr_services import KPIMTTRService
from admin.machine_assets.machine_setup.mttr.schemas.mttr_schemas import MTTRResponse


router = APIRouter(
    prefix="/mttr_KPI",
    tags=["MTTR KPI"],
)


def build_mttr_service() -> KPIMTTRService:
    machine_condition_data_url = "https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"
    repository = KPIMTTRRepository(
        machine_condition_data_url=machine_condition_data_url
    )
    return KPIMTTRService(mttr_repository=repository)


@router.get("", response_model=MTTRResponse, summary="Get MTTR KPI")
def get_mttr(
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_mttr_service()
        return service.get_mttr(token=token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")


@router.get("/by-station/{station_id}", response_model=MTTRResponse, summary="Get MTTR KPI by station")
def get_mttr_by_station(
    station_id: int = Path(description="Station ID"),
    token: str = Security(oauth2_scheme),
):
    try:
        service = build_mttr_service()
        return service.get_mttr(station_id=station_id, token=token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")