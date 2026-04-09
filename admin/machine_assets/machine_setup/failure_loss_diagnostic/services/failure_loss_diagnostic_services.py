from collections import defaultdict
from typing import Optional

from admin.machine_assets.machine_setup.failure_loss_diagnostic.repositories.failure_loss_diagnostic_repository import (
    KPIFailureLossDiagnosticRepository,
)
from admin.machine_assets.machine_setup.pareto_losses.services.pareto_losses_services import (
    KPIParetoLossesService,
)


class KPIFailureLossDiagnosticService:
    def __init__(
        self,
        failure_loss_diagnostic_repository: KPIFailureLossDiagnosticRepository,
        pareto_losses_service: KPIParetoLossesService,
    ) -> None:
        self.failure_loss_diagnostic_repository = failure_loss_diagnostic_repository
        self.pareto_losses_service = pareto_losses_service

    def get_failure_loss_diagnostic(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
    ):
        bookings_data = self.failure_loss_diagnostic_repository.get_bookings_data(token=token)

        pareto_data = self.pareto_losses_service.get_pareto_losses(
            station_id=station_id,
            date_from=date_from,
            date_to=date_to,
            token=token,
            only_critical=False,
        )

        # fallback basé sur state: FAIL / SCRAP
        failure_counts_by_station = defaultdict(lambda: defaultdict(int))

        for item in bookings_data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and current_station_id != station_id:
                continue

            state = str(item.get("state", "")).strip().lower()

            if state == "fail":
                failure_key = "FAIL"
            elif state == "scrap":
                failure_key = "SCRAP"
            else:
                continue

            failure_counts_by_station[current_station_id][failure_key] += 1

        failure_map = {}
        for st_id, counts in failure_counts_by_station.items():
            total_failures = sum(counts.values())
            if total_failures == 0:
                continue

            sorted_counts = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
            top_failure_group, top_failure_count = sorted_counts[0]
            top_failure_pct = round((100.0 * top_failure_count / total_failures), 2)

            failure_map[st_id] = {
                "top_failure_group": top_failure_group,
                "top_failure_count": top_failure_count,
                "top_failure_pct": top_failure_pct,
            }

        pareto_map = {}
        for item in pareto_data.get("results", []):
            st_id = item.get("station_id")
            if st_id is None:
                continue

            if st_id not in pareto_map or item.get("pareto_rank", 999) < pareto_map[st_id].get("pareto_rank", 999):
                pareto_map[st_id] = item

        all_station_ids = sorted(set(failure_map.keys()) | set(pareto_map.keys()))

        results = []
        for st_id in all_station_ids:
            failure_item = failure_map.get(st_id, {})
            pareto_item = pareto_map.get(st_id, {})

            top_failure_group = failure_item.get("top_failure_group")
            top_failure_count = failure_item.get("top_failure_count")
            top_failure_pct = failure_item.get("top_failure_pct")

            top_loss_type = pareto_item.get("loss_type")
            top_loss_hours = pareto_item.get("loss_hours")
            top_loss_pct = pareto_item.get("loss_pct")

            criticality_level, diagnosis = self._diagnose(
                top_failure_group=top_failure_group,
                top_loss_type=top_loss_type,
            )

            results.append({
                "station_id": st_id,
                "top_failure_group": top_failure_group,
                "top_failure_count": top_failure_count,
                "top_failure_pct": top_failure_pct,
                "top_loss_type": top_loss_type,
                "top_loss_hours": top_loss_hours,
                "top_loss_pct": top_loss_pct,
                "criticality_level": criticality_level,
                "diagnosis": diagnosis,
            })

        return {
            "title": "Failure Loss Diagnostic KPI",
            "kpi": "failure_loss_diagnostic",
            "count": len(results),
            "results": results,
        }

    def _diagnose(self, top_failure_group: Optional[str], top_loss_type: Optional[str]):
        if top_failure_group == "FAIL" and top_loss_type == "REWORK":
            return "HIGH", "Failure-driven losses are dominated by rework"

        if top_failure_group == "SCRAP" and top_loss_type == "SCRAP":
            return "HIGH", "Scrap is the dominant quality loss"

        if top_loss_type == "BREAKDOWN":
            return "HIGH", "Breakdown is the dominant operational loss"

        if top_loss_type in ("MICRO_STOP", "SETUP"):
            return "MEDIUM", "Process-related losses dominate station performance"

        if top_failure_group and not top_loss_type:
            return "MEDIUM", "Failures detected without dominant mapped loss"

        return "LOW", "No major failure-loss pattern detected"