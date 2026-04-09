from typing import Optional

from admin.machine_assets.machine_setup.mtbf.services.mtbf_services import KPIMTBFService
from admin.machine_assets.machine_setup.pareto_losses.services.pareto_losses_services import KPIParetoLossesService


class KPIReliabilityDiagnosticService:
    def __init__(
        self,
        mtbf_service: KPIMTBFService,
        pareto_losses_service: KPIParetoLossesService,
    ) -> None:
        self.mtbf_service = mtbf_service
        self.pareto_losses_service = pareto_losses_service

    def get_reliability_diagnostic(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ):
        mtbf_data = self.mtbf_service.get_mtbf(
            station_id=station_id,
            token=token,
        )

        pareto_data = self.pareto_losses_service.get_pareto_losses(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
            only_critical=False,
        )

        mtbf_map = {
            item["station_id"]: item
            for item in mtbf_data.get("results", [])
        }

        # garder seulement la perte dominante (pareto_rank = 1) par station
        pareto_rank_1_map = {}
        for item in pareto_data.get("results", []):
            st_id = item["station_id"]
            if st_id not in pareto_rank_1_map:
                pareto_rank_1_map[st_id] = item
            else:
                if item.get("pareto_rank", 999) < pareto_rank_1_map[st_id].get("pareto_rank", 999):
                    pareto_rank_1_map[st_id] = item

        all_station_ids = sorted(set(mtbf_map.keys()) | set(pareto_rank_1_map.keys()))

        results = []

        for st_id in all_station_ids:
            mtbf_item = mtbf_map.get(st_id, {})
            pareto_item = pareto_rank_1_map.get(st_id, {})

            mtbf_hours = mtbf_item.get("mtbf_hours")
            top_loss_type = pareto_item.get("loss_type")
            top_loss_pct = pareto_item.get("loss_pct")
            pareto_rank = pareto_item.get("pareto_rank")

            criticality_level, diagnosis = self._build_diagnosis(
                mtbf_hours=mtbf_hours,
                top_loss_type=top_loss_type,
                top_loss_pct=top_loss_pct,
            )

            results.append({
                "station_id": st_id,
                "mtbf_hours": mtbf_hours,
                "top_loss_type": top_loss_type,
                "top_loss_pct": top_loss_pct,
                "pareto_rank": pareto_rank,
                "criticality_level": criticality_level,
                "diagnosis": diagnosis,
            })

        return {
            "title": "Reliability Diagnostic KPI",
            "kpi": "reliability_diagnostic",
            "count": len(results),
            "results": results,
        }

    def _build_diagnosis(
        self,
        mtbf_hours: Optional[float],
        top_loss_type: Optional[str],
        top_loss_pct: Optional[float],
    ):
        if top_loss_type == "BREAKDOWN":
            if mtbf_hours is not None and mtbf_hours <= 5:
                return "HIGH", "Frequent failures with breakdown as dominant loss"
            return "HIGH", "Breakdown is the dominant loss"

        if top_loss_type in ("MICRO_STOP",):
            if mtbf_hours is not None and mtbf_hours <= 5:
                return "MEDIUM", "Low reliability with micro-stop dominant losses"
            return "MEDIUM", "Micro-stop is the dominant loss"

        if top_loss_type in ("REWORK", "SCRAP"):
            return "MEDIUM", "Quality-related losses dominate station performance"

        if mtbf_hours is None and top_loss_type is None:
            return "LOW", "No significant loss or failure detected"

        if mtbf_hours is not None and mtbf_hours > 10:
            return "LOW", "Station reliability appears acceptable"

        return "LOW", "No major reliability issue detected"