from collections import defaultdict
from typing import Optional

from admin.machine_assets.machine_setup.pareto_losses.repositories.pareto_losses_repository import (
    KPIParetoLossesRepository,
)


class KPIParetoLossesService:
    def __init__(
        self,
        pareto_losses_repository: KPIParetoLossesRepository,
    ) -> None:
        self.pareto_losses_repository = pareto_losses_repository

    def get_pareto_losses(
        self,
        station_id: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        token: Optional[str] = None,
        only_critical: Optional[bool] = False,
    ):
        def normalize_day(value):
            return str(value)[:10] if value is not None else None

        def in_date_range(day: str | None) -> bool:
            if day is None:
                return False
            if date_from is not None and day < date_from:
                return False
            if date_to is not None and day > date_to:
                return False
            return True

        machine_data = self.pareto_losses_repository.get_machine_condition_data(token=token)
        bookings_data = self.pareto_losses_repository.get_bookings_data(token=token)

        losses_agg = defaultdict(float)

        for item in machine_data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and current_station_id != station_id:
                continue

            production_day = normalize_day(item.get("event_time"))
            if not in_date_range(production_day):
                continue

            condition_id = item.get("condition_id")
            duration_seconds = float(item.get("duration_seconds", 0) or 0)

            if condition_id in (8, 12, 16):
                loss_type = "BREAKDOWN"
            elif condition_id in (5, 6):
                loss_type = "MICRO_STOP"
            else:
                continue

            loss_hours = duration_seconds / 3600.0
            losses_agg[(current_station_id, production_day, loss_type)] += loss_hours

        for item in bookings_data:
            current_station_id = item.get("station_id")
            if current_station_id is None:
                continue

            if station_id is not None and current_station_id != station_id:
                continue

            production_day = normalize_day(item.get("date_of_booking"))
            if not in_date_range(production_day):
                continue

            state = str(item.get("state", "")).strip().lower()

            if state == "scrap":
                loss_type = "SCRAP"
            elif state == "fail":
                loss_type = "REWORK"
            else:
                continue

            losses_agg[(current_station_id, production_day, loss_type)] += 1.0

        grouped = defaultdict(list)

        for (st_id, prod_day, loss_type), loss_hours in losses_agg.items():
            grouped[(st_id, prod_day)].append({
                "station_id": st_id,
                "production_day": prod_day,
                "loss_type": loss_type,
                "loss_hours": round(loss_hours, 2),
            })

        results = []

        for (st_id, prod_day), rows in grouped.items():
            rows_sorted = sorted(
                rows,
                key=lambda x: (-x["loss_hours"], x["loss_type"])
            )

            total_loss = sum(row["loss_hours"] for row in rows_sorted)
            cumulative = 0.0

            for index, row in enumerate(rows_sorted, start=1):
                loss_hours = row["loss_hours"]
                loss_pct = round((100.0 * loss_hours / total_loss), 2) if total_loss > 0 else 0.0

                cumulative += loss_hours
                cumulative_pct = round((100.0 * cumulative / total_loss), 2) if total_loss > 0 else 0.0

                is_critical = cumulative_pct <= 80

                if only_critical and not is_critical:
                    continue

                results.append({
                    "station_id": row["station_id"],
                    "production_day": row["production_day"],
                    "loss_type": row["loss_type"],
                    "loss_hours": loss_hours,
                    "loss_pct": loss_pct,
                    "cumulative_pct": cumulative_pct,
                    "pareto_rank": index,
                    "is_critical": is_critical,
                })

        results.sort(
            key=lambda x: (x["production_day"], x["station_id"], x["pareto_rank"])
        )

        return {
            "title": "Pareto Losses KPI",
            "kpi": "pareto_losses",
            "count": len(results),
            "results": results,
        }