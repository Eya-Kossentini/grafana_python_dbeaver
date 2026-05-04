from fastapi import APIRouter, Query, Security, HTTPException
from typing import Optional

from admin.dependencies import oauth2_scheme
from admin.machine_assets.machine_setup.defect_rate.services.defect_rate_services import KPIDefectRateService
from admin.machine_assets.machine_setup.defect_rate.repositories.defect_rate_repository import KPIDefectRateRepository
from admin.machine_assets.machine_setup.defect_rate.schemas.defect_rate_schemas import DefectRateResponse

router = APIRouter(
    prefix="/defect_rate",
    tags=["KPI Defect Rate"],
)


@router.get("/", response_model=DefectRateResponse)  # ✅ "/" au lieu de "/defect_rate" (évite le doublon)
def get_defect_rate_kpi(
    station_id: Optional[int] = Query(default=None, description="Filter by station ID"),
    station_name: Optional[str] = Query(default=None, description="Filter by station name"),  # ✅ ajouté
    token: str = Security(oauth2_scheme)
):
    try:
        repository = KPIDefectRateRepository()
        service = KPIDefectRateService(repository)

        return service.get_defect_rate(
            station_id=station_id,
            station_name=station_name,  # ✅ passé au service
            token=token
        )
    except HTTPException:
        raise  # ✅ laisse passer les HTTPException proprement
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Endpoint error: {str(e)}")