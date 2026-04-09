"""Containers module."""


from dependency_injector import containers, providers
from fastapi.security import OAuth2PasswordBearer

import os
from admin.machine_assets.machine_setup.defect_rate.repositories import defect_rate_repository
from admin.machine_assets.machine_setup.line_quality.repositories import line_quality_repository
from admin.machine_assets.machine_setup.oee.repositories import oee_repository
from admin.machine_assets.machine_setup.availability.repositories import availability_repository
from admin.machine_assets.machine_setup.performance.repositories import performance_repository
from admin.machine_assets.machine_setup.quality.repositories import quality_repository


from admin.machine_assets.machine_setup.line_quality.services import line_quality_services
from admin.machine_assets.machine_setup.defect_rate.services import defect_rate_services
from admin.machine_assets.machine_setup.oee.services import oee_services
from admin.machine_assets.machine_setup.availability.services import availability_services
from admin.machine_assets.machine_setup.performance.services import performance_services
from admin.machine_assets.machine_setup.quality.services import quality_services

from admin.machine_assets.machine_setup.pareto_losses.services import pareto_losses_services
from admin.machine_assets.machine_setup.mtbf.services import mtbf_services
from admin.machine_assets.machine_setup.mttr.services import mttr_services
from admin.machine_assets.machine_setup.downtime.services import downtime_services

from admin.machine_assets.machine_setup.scrap_by_day.services import scrap_by_day_services
from admin.machine_assets.machine_setup.dashboard_overview.repositories import dashboard_overview_repository
from admin.machine_assets.machine_setup.dashboard_overview.services import dashboard_overview_services

from admin.machine_assets.machine_setup.oee.services.oee_services import KPIOeeService

from database import Database
from auth_client.auth_service import AuthService


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        modules=[
            "admin.machine_assets.machine_setup.defect_rate.endpoints.defect_rate_endpoint",
            "admin.machine_assets.machine_setup.line_quality.services.line_quality_services",
            "admin.machine_assets.machine_setup.availability.endpoints.availability_endpoint",
            "admin.machine_assets.machine_setup.performance.endpoints.performance_endpoint",
            "admin.machine_assets.machine_setup.quality.endpoints.quality_endpoint",
            "admin.machine_assets.machine_setup.oee.endpoints.oee_endpoint"
        ],
        packages=[],
    )

    oauth2_scheme = providers.Object(OAuth2PasswordBearer(tokenUrl="auth/token"))
    config = providers.Configuration(yaml_files=["config.yml"])
    db = providers.Singleton(Database, db_url=config.db.url)
    db_url = os.environ.get("DATABASE_URL") or config.db.url
    db = providers.Singleton(Database, db_url)
    
    auth_service = providers.Factory(
        AuthService,
        db=db,
    )

    
    kpi_defect_rate_repository = providers.Factory(
        defect_rate_repository.KPIDefectRateRepository,
        session_factory = db.provided.session,
        bookings_url=config.bookings_url.from_value("https://core_demo.momes-solutions.com/bookings/bookings/")
    )
    
 
    kpi_defect_rate_service = providers.Factory(
        defect_rate_services.KPIDefectRateService,
        kpi_defect_rate_repository=kpi_defect_rate_repository
    )
    
    KPILineProductionQualityRepository = providers.Factory(
        line_quality_repository.KPILineProductionQualityRepository,
        session_factory = db.provided.session,
        bookings_url=config.bookings_url.from_value("https://core_demo.momes-solutions.com/bookings/bookings/"),
        lines_url=config.lines_url.from_value("https://core_demo.momes-solutions.com/lines/lines/")
    )
    
 
    KPILineProductionQualityService = providers.Factory(
        line_quality_services.KPILineProductionQualityService,
        KPILineProductionQualityRepository=KPILineProductionQualityRepository
    )

    
    KPIAvailabilityRepository = providers.Factory(
        availability_repository.KPIAvailabilityRepository,
        machine_condition_data_url=config.machine_condition_data_url.from_value("https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"),
        machine_conditions_url=config.machine_conditions_url.from_value("https://core_demo.momes-solutions.com/machine-conditions/machine-conditions/"),
    )
    
    KPIAvailabilityService = providers.Factory(
        availability_services.KPIAvailabilityService,
        kpi_availability_repository=KPIAvailabilityRepository,
    )

    KPIPerformanceRepository = providers.Factory(
        performance_repository.KPIPerformanceRepository,
        machine_condition_data_url=config.machine_condition_data_url.from_value("https://core_demo.momes-solutions.com/machine-condition-data/machine-condition-data/"),
        machine_conditions_url=config.machine_conditions_url.from_value("https://core_demo.momes-solutions.com/machine-conditions/machine-conditions/"),
    )
    
    KPIPerformanceService = providers.Factory(
        performance_services.KPIPerformanceService,
        kpi_performance_repository=KPIPerformanceRepository,
    )

    KPIQualityRepository = providers.Factory(
        quality_repository.KPIQualityRepository,
        bookings_url=config.bookings_url.from_value("https://core_demo.momes-solutions.com/bookings/bookings/"),
    )
        
    KPIQualityService = providers.Factory(
        quality_services.KPIQualityService,
        kpi_quality_repository=KPIQualityRepository,
    )

    KPIOeeService = providers.Factory(
        oee_services.KPIOeeService,
        kpi_availability_service=KPIAvailabilityService,
        kpi_performance_service=KPIPerformanceService,
        kpi_quality_service=KPIQualityService,
    )  
    
    KPIParetoLossesService = providers.Factory(
    pareto_losses_services.KPIParetoLossesService,
    )
    
    KPIMTBFService = providers.Factory(
        mtbf_services.KPIMTBFService,
    )

    KPIMTTRService = providers.Factory(
        mttr_services.KPIMTTRService,
    )
    
    KPIDowntimeService = providers.Factory(
        downtime_services.KPIDowntimeService,
    )
    
    KPIScrapByDayService = providers.Factory(
        scrap_by_day_services.KPIScrapByDayService,
    )
    
    
    DashboardOverviewRepository = providers.Factory(
        dashboard_overview_repository.DashboardOverviewRepository,
        oee_service=KPIOeeService,
        availability_service=KPIAvailabilityService,
        performance_service=KPIPerformanceService,
        quality_service=KPIQualityService,
        mtbf_service=KPIMTBFService,
        mttr_service=KPIMTTRService,
    )

    KPIDashboardOverviewService = providers.Factory(
        dashboard_overview_services.KPIDashboardOverviewService,
        repository=DashboardOverviewRepository,
    )
    
      # Repository
  
   
    def init_resources(self):
        """Initialize resources like database tables."""
        db = self.db()
        db.create_database()