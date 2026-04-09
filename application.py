"""Application module."""

from fastapi import FastAPI, Query
from admin import endpoints 

from admin.machine_assets.machine_setup.defect_rate.endpoints import defect_rate_endpoint
from admin.machine_assets.machine_setup.line_quality.endpoints import line_quality_endpoint
from admin.machine_assets.machine_setup.availability.endpoints import availability_endpoint
from admin.machine_assets.machine_setup.performance.endpoints import performance_endpoint
from admin.machine_assets.machine_setup.quality.endpoints import quality_endpoint
from admin.machine_assets.machine_setup.oee.endpoints import oee_endpoint
from admin.machine_assets.machine_setup.pareto_losses.endpoints import pareto_losses_endpoint
from admin.machine_assets.machine_setup.mtbf.endpoints import mtbf_endpoint
from admin.machine_assets.machine_setup.mttr.endpoints import mttr_endpoint
from admin.machine_assets.machine_setup.reliability_diagnostic.endpoints import reliability_diagnostic_endpoint
from admin.machine_assets.machine_setup.downtime.endpoints import downtime_endpoint
from admin.machine_assets.machine_setup.failure_loss_diagnostic.endpoints import failure_loss_diagnostic_endpoint
from admin.machine_assets.machine_setup.scrap_by_day.endpoints import scrap_by_day_endpoint
from admin.machine_assets.machine_setup.dashboard_overview.endpoints import dashboard_overview_endpoint

from admin.machine_assets.machine_setup.availability.repositories.availability_repository import KPIAvailabilityRepository
from admin.machine_assets.machine_setup.availability.services.availability_services import KPIAvailabilityService

from containers import Container

app = FastAPI(title="Availability KPI API")

repository = KPIAvailabilityRepository()
service = KPIAvailabilityService(repository)

@app.get("/availability")
def get_availability(
    station_id: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    token: str | None = Query(default=None),
):
    return service.get_availability(
        station_id=station_id,
        date_from=date_from,
        date_to=date_to,
        token=token,
    )

def create_app() -> FastAPI:
    container = Container()
    db = container.db()
    db.create_database()

    # Register provider
    container.providers["kpi_defect_rate_service"] = container.kpi_defect_rate_service
    container.providers["KPILineProductionQualityService"] = container.KPILineProductionQualityService
    
    
    # Wire container
    container.wire(packages=["admin.machine_assets"])

    app = FastAPI()

    app.include_router(endpoints.router, prefix="/auth", tags=["auth"])
    app.include_router(defect_rate_endpoint.router)  
    app.include_router(line_quality_endpoint.router)  
    app.include_router(availability_endpoint.router)
    app.include_router(performance_endpoint.router)
    app.include_router(quality_endpoint.router)
    app.include_router(oee_endpoint.router)
    app.include_router(pareto_losses_endpoint.router)
    app.include_router(mtbf_endpoint.router)
    app.include_router(mttr_endpoint.router)
    app.include_router(reliability_diagnostic_endpoint.router)
    app.include_router(downtime_endpoint.router)
    app.include_router(failure_loss_diagnostic_endpoint.router)
    app.include_router(scrap_by_day_endpoint.router)
    app.include_router(dashboard_overview_endpoint.router)
   
    return app


app = create_app()