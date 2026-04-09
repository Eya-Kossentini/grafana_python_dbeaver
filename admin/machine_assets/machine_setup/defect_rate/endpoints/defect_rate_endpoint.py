from fastapi import APIRouter, Path, Query, Security, HTTPException

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.defect_rate.services.defect_rate_services import KPIDefectRateService
from admin.machine_assets.machine_setup.defect_rate.repositories.defect_rate_repository import KPIDefectRateRepository
from admin.machine_assets.machine_setup.defect_rate.schemas.defect_rate_schemas import DefectRateResponse

router = APIRouter(
    prefix="/defect_rate",
    tags=["KPI Defect Rate"],
)


@router.get("/defect_rate", response_model=DefectRateResponse)
def get_defect_rate_kpi(
    station_id: int = Path(description= "Station ID"),
    token: str = Security(oauth2_scheme)
):
    try:
        repository = KPIDefectRateRepository()
        service = KPIDefectRateService(repository)

        return service.get_defect_rate(
            station_id=station_id,
            token=token
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")