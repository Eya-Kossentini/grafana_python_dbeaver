"""
Détection d'anomalies KPI — Z-Score & Moving Average Deviation
==================================================================
Objectif : Compléter l'approche IQR / Isolation Forest avec deux
           méthodes statistiques complémentaires, plus simples à
           expliquer à l'oral :

  1. Z-Score          → écart à la moyenne en nombre d'écarts-types
                         (warning si |z| > 2, critical si |z| > 3)
  2. Moving Average    → écart relatif à la moyenne mobile des 7
     Deviation            derniers jours (warning ≥ 50%, critical ≥ 100%)

Source : PostgreSQL (table oee_kpi)
"""

import os
import sys
import urllib3
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
from sqlalchemy import create_engine

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ─────────────────────────────────────────────────────────────────────────────
# Connexion PostgreSQL (SQLAlchemy — élimine le UserWarning)
# ─────────────────────────────────────────────────────────────────────────────

engine = create_engine("postgresql+psycopg2://postgres:admin123@localhost:5435/postgres")


# ─────────────────────────────────────────────────────────────────────────────
# Palette
# ─────────────────────────────────────────────────────────────────────────────
C_LINE     = "#4A90D9"
C_MA       = "#7ED321"
C_WARNING  = "#F5A623"
C_CRITICAL = "#D0021B"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Z-SCORE PAR GROUPE (FONCTION)
# Calcul du z-score (écart à la moyenne / écart-type) par station, et
# classification en normal / warning (|z|>2) / critical (|z|>3).
# ══════════════════════════════════════════════════════════════════════════════

def add_zscore(group: pd.DataFrame, col: str, direction: str = "low") -> pd.DataFrame:
    """
    Ajoute z_score et severity sur un groupe (par station).
    direction='low'  → anomalie si oee chute   (z < -2 / -3)
    direction='high' → anomalie si valeur monte (z >  2 /  3)
    """
    g = group.copy()
    mean, std = g[col].mean(), g[col].std()
    g["z_score"] = (g[col] - mean) / std if std > 0 else 0.0
    g["severity"] = "normal"
    if direction == "low":
        g.loc[g["z_score"] < -2, "severity"] = "warning"
        g.loc[g["z_score"] < -3, "severity"] = "critical"
    else:
        g.loc[g["z_score"] >  2, "severity"] = "warning"
        g.loc[g["z_score"] >  3, "severity"] = "critical"
    return g


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MOVING AVERAGE DEVIATION (FONCTION)
# Calcul de l'écart relatif entre la valeur du jour et la moyenne
# mobile des `window` jours précédents (sans le jour J), avec
# classification normal / warning / critical selon `threshold_pct`.
# ══════════════════════════════════════════════════════════════════════════════

def add_moving_average(
    group: pd.DataFrame,
    col: str,
    window: int = 7,
    threshold_pct: float = 0.5,
    direction: str = "both",
) -> pd.DataFrame:
    """
    Détecte les anomalies par écart à la moyenne glissante.

    Principe :
        ma   = moyenne des `window` jours précédents  (sans le jour J)
        écart = |valeur_J - ma| / ma

        Anomalie si écart > threshold_pct
          → warning  si 0.5 ≤ écart < 1.0  (ex: OEE normal=78%, aujourd'hui=39% → écart=50%)
          → critical si écart ≥ 1.0         (doublement ou chute de moitié)

    Exemple concret :
        scrap normal (MA7) = 3%
        aujourd'hui        = 11%
        écart              = |11-3|/3 = 2.67  → CRITICAL  ✓

    Args:
        group          : DataFrame d'une seule station, trié par date
        col            : colonne KPI à analyser (ex: 'oee_pct')
        window         : fenêtre glissante en jours (défaut 7)
        threshold_pct  : seuil de déclenchement warning (défaut 0.5 = 50%)
        direction      : 'low'  → anomalie si chute seulement
                         'high' → anomalie si hausse seulement
                         'both' → les deux sens (défaut)

    Returns:
        DataFrame enrichi avec :
            ma_{col}          : moyenne mobile sur `window` jours
            ma_deviation_pct  : écart relatif en %  (ex: 2.67 → 267%)
            ma_severity       : 'normal' | 'warning' | 'critical'
    """
    g = group.copy().sort_values("production_day").reset_index(drop=True)

    # Moyenne glissante des jours PRÉCÉDENTS (shift=1 → exclut le jour J)
    ma_col = f"ma_{col}"
    g[ma_col] = (
        g[col]
        .shift(1)                                  # décalage d'un jour
        .rolling(window=window, min_periods=3)     # fenêtre sur les jours passés
        .mean()
    )

    # Écart relatif : (valeur - MA) / MA
    g["ma_raw_deviation"] = g[col] - g[ma_col]
    g["ma_deviation_pct"] = (
        (g[col] - g[ma_col]).abs() / g[ma_col].abs()
    ).where(g[ma_col].notna() & (g[ma_col] != 0), other=np.nan)

    # Filtre directionnel
    if direction == "low":
        deviation_mask = g["ma_raw_deviation"] < 0
    elif direction == "high":
        deviation_mask = g["ma_raw_deviation"] > 0
    else:
        deviation_mask = pd.Series(True, index=g.index)

    # Sévérité
    g["ma_severity"] = "normal"
    warn_mask = (
        deviation_mask
        & g["ma_deviation_pct"].notna()
        & (g["ma_deviation_pct"] >= threshold_pct)
        & (g["ma_deviation_pct"] < threshold_pct * 2)
    )
    crit_mask = (
        deviation_mask
        & g["ma_deviation_pct"].notna()
        & (g["ma_deviation_pct"] >= threshold_pct * 2)
    )
    g.loc[warn_mask, "ma_severity"] = "warning"
    g.loc[crit_mask, "ma_severity"] = "critical"

    return g


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — GRAPHIQUES MOVING AVERAGE
# Génère, pour un KPI donné : le graphique temporel par station
# (courbe réelle + MA + anomalies), l'histogramme warning/critical par
# station, et l'export CSV des anomalies détectées.
# ══════════════════════════════════════════════════════════════════════════════

def plot_moving_average(df: pd.DataFrame, col: str = "oee_pct",
                        window: int = 7, threshold_pct: float = 0.5,
                        direction: str = "low") -> None:

    kpi_label = col.replace("_pct", " (%)").replace("_", " ").upper()
    print(f"\n→ Moving Average Deviation — {kpi_label}  (fenêtre={window}j, seuil={threshold_pct*100:.0f}%)")

    # Calcul MA par station
    parts = []
    for station in df["station_name"].dropna().unique():
        g = df[df["station_name"] == station].copy()
        parts.append(add_moving_average(g, col, window=window,
                                        threshold_pct=threshold_pct,
                                        direction=direction))
    df_ma = pd.concat(parts).reset_index(drop=True)

    stations = sorted(df_ma["station_name"].dropna().unique())
    n        = len(stations)
    ma_col   = f"ma_{col}"

    fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(16, 4 * n), sharex=False)
    if n == 1:
        axes = [axes]

    fig.suptitle(
        f"Moving Average Deviation — {kpi_label}  (MA{window}j  ·  seuil ±{threshold_pct*100:.0f}%)",
        fontsize=14, fontweight="bold", y=1.001
    )

    all_anom = []
    for ax, station in zip(axes, stations):
        # ↓ CORRECTION : utiliser df_ma au lieu de df
        grp = (
            df_ma[df_ma["station_name"] == station]
            .copy()
            .sort_values("production_day")
        )
        warn = grp[grp["ma_severity"] == "warning"]
        crit = grp[grp["ma_severity"] == "critical"]
        all_anom.append(grp[grp["ma_severity"] != "normal"])

        # Courbe KPI
        ax.plot(grp["production_day"], grp[col],
                color=C_LINE, linewidth=1.2, alpha=0.65, label=kpi_label)

        # Courbe MA
        ax.plot(grp["production_day"], grp[ma_col],
                color=C_MA, linewidth=2, linestyle="--",
                label=f"Moyenne mobile {window}j")

        # Remplissage entre KPI et MA
        ax.fill_between(
            grp["production_day"],
            grp[col], grp[ma_col],
            where=grp[ma_col].notna(),
            alpha=0.08, color=C_WARNING, label="Écart KPI vs MA"
        )

        # Points anomalies
        if not warn.empty:
            ax.scatter(warn["production_day"], warn[col],
                       color=C_WARNING, s=70, zorder=5,
                       label=f"Warning ≥{threshold_pct*100:.0f}% (n={len(warn)})")
        if not crit.empty:
            ax.scatter(crit["production_day"], crit[col],
                       color=C_CRITICAL, s=90, marker="X", zorder=6,
                       label=f"Critical ≥{threshold_pct*200:.0f}% (n={len(crit)})")

        # Annotations sur les pires anomalies
        top = grp.nlargest(3, "ma_deviation_pct") if not grp["ma_deviation_pct"].isna().all() else pd.DataFrame()
        for _, row in top.iterrows():
            if row["ma_severity"] != "normal" and pd.notna(row["ma_deviation_pct"]):
                ax.annotate(
                    f"{row['ma_deviation_pct']*100:.0f}%",
                    xy=(row["production_day"], row[col]),
                    xytext=(0, 10), textcoords="offset points",
                    fontsize=7, color=C_CRITICAL,
                    arrowprops=dict(arrowstyle="-", color=C_CRITICAL, lw=0.8)
                )

        ax.set_title(
            f"Station {station}  |  Anomalies MA : {len(warn)+len(crit)}  "
            f"(warning={len(warn)}, critical={len(crit)})",
            fontsize=10, loc="left"
        )
        ax.set_ylabel(kpi_label)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
        ax.legend(fontsize=8, loc="lower right", ncol=3)
        ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    fname = f"outputs_moving_anomaly/figures/ma_deviation_{col}.png"
    plt.savefig(fname, dpi=150, bbox_inches="tight")
    plt.show()
    print(f"  ✓ {fname}")

    # ── Histogramme par station ───────────────────────────────────────────────
    anom_all = pd.concat(all_anom) if all_anom else pd.DataFrame()
    if anom_all.empty:
        print("  ℹ Aucune anomalie MA détectée.")
        return anom_all   # retourne df vide pour l'export

    counts = (
        anom_all.groupby(["station_name", "ma_severity"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=["warning", "critical"], fill_value=0)
    )
    counts["total"] = counts.sum(axis=1)
    counts = counts.sort_values("total", ascending=False)

    fig2, ax2 = plt.subplots(figsize=(max(8, len(counts) * 1.4), 5))
    x     = range(len(counts))
    width = 0.4
    b_warn = ax2.bar([i - width/2 for i in x], counts["warning"],
                     width=width, color=C_WARNING, label="Warning")
    b_crit = ax2.bar([i + width/2 for i in x], counts["critical"],
                     width=width, color=C_CRITICAL, label="Critical")

    for bar in list(b_warn) + list(b_crit):
        if bar.get_height() > 0:
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                     str(int(bar.get_height())),
                     ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax2.set_xticks(list(x))
    ax2.set_xticklabels(counts.index, rotation=20, ha="right")
    ax2.set_title(f"Anomalies Moving Average — {kpi_label}", fontsize=13, fontweight="bold")
    ax2.set_xlabel("Station")
    ax2.set_ylabel("Nb anomalies")
    ax2.legend(fontsize=9)
    ax2.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fname2 = f"outputs_moving_anomaly/figures/ma_anomalies_par_station_{col}.png"
    plt.savefig(fname2, dpi=150, bbox_inches="tight")
    plt.show()
    print(f"  ✓ {fname2}")

    # ── Export CSV anomalies ──────────────────────────────────────────────────
    out_cols = ["production_day", "station_id", "station_name", col, ma_col,
                "ma_raw_deviation", "ma_deviation_pct", "ma_severity"]
    out_cols = [c for c in out_cols if c in anom_all.columns]
    anom_all[out_cols].to_csv(
        f"outputs_moving_anomaly/csv/ma_anomalies_{col}.csv", index=False
    )
    print(f"  ✓ ma_anomalies_{col}.csv")

    print(f"\n  ===== ANOMALIES MOVING AVERAGE — {kpi_label} =====")
    display = anom_all[out_cols].copy()
    if "ma_deviation_pct" in display.columns:
        display["ma_deviation_pct"] = (display["ma_deviation_pct"] * 100).round(1).astype(str) + "%"
    print(display.to_string(index=False))

    return df_ma   # ← retourne df_ma enrichi pour réutilisation dans __main__


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — GRAPHIQUES OEE (Z-SCORE)
# Génère, pour l'OEE : le graphique temporel par station (bande ±2σ +
# anomalies), la heatmap station × semaine, l'histogramme par station,
# et l'export CSV des anomalies Z-Score.
# ══════════════════════════════════════════════════════════════════════════════

def plot_oee(df: pd.DataFrame) -> None:
    """Un sous-graphe par station — lisible même avec 10+ stations."""

    # Z-Score calculé station par station
    parts = []
    for station in df["station_name"].dropna().unique():
        g = df[df["station_name"] == station].copy()
        parts.append(add_zscore(g, "oee_pct", direction="low"))
    df = pd.concat(parts).reset_index(drop=True)

    stations = sorted(df["station_name"].dropna().unique())
    n = len(stations)
    print(f"\n  → {n} station(s) détectée(s) : {stations}")

    # ── A. Time-series par station ───────────────────────────────────────────
    fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(16, 4 * n), sharex=False)
    if n == 1:
        axes = [axes]

    fig.suptitle("Z-Score Anomaly Detection — OEE par Station",
                 fontsize=15, fontweight="bold", y=1.001)

    all_anomalies = []

    for ax, station in zip(axes, stations):
        grp = (
        df[df["station_name"] == station]
        .copy()
        .sort_values("production_day")
        )
        warn = grp[grp["severity"] == "warning"]
        crit = grp[grp["severity"] == "critical"]
        all_anomalies.append(grp[grp["severity"] != "normal"])

        grp = grp.sort_values("production_day")

        grp["ma7"] = (
            grp["oee_pct"]
            .rolling(7, min_periods=1)
            .mean()
        )
        mean_v, std_v = grp["oee_pct"].mean(), grp["oee_pct"].std()

        ax.plot(grp["production_day"], grp["oee_pct"],
                color=C_LINE, linewidth=1.2, alpha=0.7, label="OEE (%)")
        ax.plot(grp["production_day"], grp["ma7"],
                color=C_MA, linewidth=1.8, linestyle="--", label="Moy. mobile 7j")

        # Bande normale ±2σ
        ax.axhspan(max(0, mean_v - 2*std_v), min(100, mean_v + 2*std_v),
                   alpha=0.07, color=C_LINE)
        ax.axhline(mean_v, color=C_LINE, linewidth=0.8, linestyle=":", alpha=0.5)

        if not warn.empty:
            ax.scatter(warn["production_day"], warn["oee_pct"],
                       color=C_WARNING, s=70, zorder=5,
                       label=f"Warning z<-2 (n={len(warn)})")
        if not crit.empty:
            ax.scatter(crit["production_day"], crit["oee_pct"],
                       color=C_CRITICAL, s=90, marker="X", zorder=6,
                       label=f"Critical z<-3 (n={len(crit)})")

        ax.set_title(
            f"{station}  |  "
            f"Moy={mean_v:.1f}%  σ={std_v:.1f}  "
            f"Anomalies={len(warn)+len(crit)}",
            fontsize=10, loc="left"
        )
        ax.set_ylabel("OEE (%)")
        ax.set_ylim(0, 105)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
        ax.legend(fontsize=8, loc="lower right", ncol=4)
        ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig("outputs_moving_anomaly/figures/oee_par_station.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("  ✓ oee_par_station.png")

    # ── B. Heatmap anomalies Station × Semaine ───────────────────────────────
    df["week"]       = df["production_day"].dt.to_period("W").astype(str)
    df["is_anomaly"] = (df["severity"] != "normal").astype(int)
    pivot = df.pivot_table(
        index="station_name", columns="week",
        values="is_anomaly", aggfunc="sum", fill_value=0
    )

    fig2, ax2 = plt.subplots(
        figsize=(max(12, len(pivot.columns) * 0.55), max(4, len(pivot) * 0.7 + 2))
    )
    im = ax2.imshow(pivot.values, aspect="auto", cmap="YlOrRd", interpolation="nearest")
    ax2.set_xticks(range(len(pivot.columns)))
    ax2.set_xticklabels(pivot.columns, rotation=55, ha="right", fontsize=7)
    ax2.set_yticks(range(len(pivot.index)))
    ax2.set_yticklabels(pivot.index, fontsize=9)
    ax2.set_title("Heatmap — Anomalies OEE  (Station × Semaine)",
                  fontsize=13, fontweight="bold")
    plt.colorbar(im, ax=ax2, label="Nb anomalies / semaine")
    plt.tight_layout()
    plt.savefig("outputs_moving_anomaly/figures/oee_heatmap.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("  ✓ oee_heatmap.png")

    # ── C. Histogramme anomalies par station ────────────────────────────────
    anom_all = pd.concat(all_anomalies)
    if anom_all.empty:
        print("  ℹ Aucune anomalie OEE détectée.")
        return

    counts = anom_all.groupby("station_name")["severity"].value_counts().unstack(fill_value=0)
    colors = []
    for sid in counts.index:
        has_crit = "critical" in counts.columns and counts.loc[sid, "critical"] > 0
        colors.append(C_CRITICAL if has_crit else C_WARNING)

    total = counts.sum(axis=1).sort_values(ascending=False)

    fig3, ax3 = plt.subplots(figsize=(max(8, len(total) * 1.2), 5))
    bars = ax3.bar(
        total.index,
        total.values,
        color=[colors[list(counts.index).index(i)] for i in total.index]
    )
    for bar, val in zip(bars, total.values):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                 str(val), ha="center", va="bottom", fontsize=10, fontweight="bold")

    patches = [
        mpatches.Patch(color=C_CRITICAL, label="Contient Critical (z<-3)"),
        mpatches.Patch(color=C_WARNING,  label="Warning seulement (z<-2)"),
    ]
    ax3.legend(handles=patches, fontsize=9)
    ax3.set_title("Nombre d'anomalies OEE par Station", fontsize=13, fontweight="bold")
    ax3.set_xlabel("Station")
    ax3.set_ylabel("Anomalies")
    ax3.set_xticklabels(total.index, rotation=20, ha="right")
    ax3.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("outputs_moving_anomaly/figures/oee_anomalies_par_station.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("  ✓ oee_anomalies_par_station.png")

    # ── D. Export CSV ─────────────────────────────────────────────────────────
    anom_all.to_csv("zscore_oee_anomalies.csv", index=False)
    print("  ✓ zscore_oee_anomalies.csv")

    # ── E. Rapport console ────────────────────────────────────────────────────
    print("\n  ===== ANOMALIES OEE =====")
    print(anom_all[[
        "production_day", "station_id",
        "oee_pct", "z_score", "severity"
    ]].to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — MODE DÉMO (SANS API)
# Génère un jeu de données synthétique (3 stations, n jours) avec
# anomalies injectées volontairement — utile pour tester les fonctions
# de détection sans dépendre de la base PostgreSQL.
# ══════════════════════════════════════════════════════════════════════════════

def demo_data(n: int = 60) -> pd.DataFrame:
    rng   = np.random.default_rng(42)
    dates = pd.date_range(end=pd.Timestamp.today(), periods=n)
    rows  = []
    for sid in [1, 2, 3]:
        oee = np.clip(78 + rng.normal(0, 5, n), 10, 98)
        oee[sid * 5]     -= 30   # anomalie critique
        oee[sid * 5 + 2] -= 18   # anomalie warning
        for i, d in enumerate(dates):
            rows.append({
                "production_day":   d,
                "station_id":       sid,
                "oee_pct":          round(oee[i], 2),
                "availability_pct": round(oee[i] + rng.uniform(-5, 5), 2),
                "performance_pct":  round(oee[i] + rng.uniform(-5, 5), 2),
                "quality_pct":      round(oee[i] + rng.uniform(-5, 5), 2),
            })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — POINT D'ENTRÉE PRINCIPAL
# Orchestration complète : chargement OEE depuis PostgreSQL →
# Z-Score → Moving Average → exports CSV (4 fichiers) → exports
# PostgreSQL (2 tables) → rapport texte consolidé.
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    os.makedirs("outputs_moving_anomaly/figures", exist_ok=True)
    os.makedirs("outputs_moving_anomaly/csv",     exist_ok=True)
    os.makedirs("outputs_moving_anomaly/reports", exist_ok=True)

    engine = create_engine(
        "postgresql+psycopg2://postgres:admin123@localhost:5435/postgres"
    )

    df = pd.read_sql("SELECT * FROM oee_kpi", engine)
    df["production_day"] = pd.to_datetime(df["production_day"])
    df.columns = df.columns.str.strip()

    engine.dispose()

    # ── Génération des graphiques OEE (Z-Score) ───────────────────────────────
    print("\n→ Génération des graphiques OEE (Z-Score)...")
    plot_oee(df)

    # ── Moving Average sur OEE ────────────────────────────────────────────────
    print("\n→ Moving Average Deviation — OEE...")
    plot_moving_average(df, col="oee_pct", window=7,
                        threshold_pct=0.5, direction="low")

    # ── Export CSV complet (toutes stations, toutes colonnes MA) ──────────────
    print("\n→ Export CSV complet (Moving Average)...")

    parts_ma = []
    for station in df["station_name"].dropna().unique():
        g = df[df["station_name"] == station].copy()
        g = add_moving_average(g, "oee_pct", window=7,
                               threshold_pct=0.5, direction="low")
        parts_ma.append(g)

    df_ma_full = pd.concat(parts_ma, ignore_index=True)

    ma_all_cols = [
        "production_day", "station_id", "station_name",
        "oee_pct", "ma_oee_pct",
        "ma_raw_deviation", "ma_deviation_pct", "ma_severity"
    ]
    ma_all_cols = [c for c in ma_all_cols if c in df_ma_full.columns]

    df_ma_full[ma_all_cols].to_csv(
        "outputs_moving_anomaly/csv/ma_oee_all.csv", index=False
    )
    print("  ✓ ma_oee_all.csv")

    df_ma_full[df_ma_full["ma_severity"] != "normal"][ma_all_cols].to_csv(
        "outputs_moving_anomaly/csv/ma_oee_anomalies.csv", index=False
    )
    print("  ✓ ma_oee_anomalies.csv")

    # ── Export Z-Score CSV ────────────────────────────────────────────────────
    parts_zs = []
    for station in df["station_name"].dropna().unique():
        g = df[df["station_name"] == station].copy()
        g = add_zscore(g, "oee_pct", direction="low")
        parts_zs.append(g)

    df_zs_full = pd.concat(parts_zs, ignore_index=True)

    zs_cols = [
        "production_day", "station_id", "station_name",
        "oee_pct", "z_score", "severity"
    ]
    zs_cols = [c for c in zs_cols if c in df_zs_full.columns]

    df_zs_full[zs_cols].to_csv(
        "outputs_moving_anomaly/csv/zscore_oee_all.csv", index=False
    )
    print("  ✓ zscore_oee_all.csv")

    df_zs_full[df_zs_full["severity"] != "normal"][zs_cols].to_csv(
        "outputs_moving_anomaly/csv/zscore_oee_anomalies.csv", index=False
    )
    print("  ✓ zscore_oee_anomalies.csv")

    # ── Export PostgreSQL ─────────────────────────────────────────────────────
    print("\n→ Export PostgreSQL...")

    engine2 = create_engine(
        "postgresql+psycopg2://postgres:admin123@localhost:5435/postgres"
    )

    # Table ma_results
    df_ma_sql = df_ma_full[ma_all_cols].copy()
    df_ma_sql["kpi_name"] = "oee_pct"
    df_ma_sql = df_ma_sql.rename(columns={
        "oee_pct":        "kpi_value",
        "ma_oee_pct":     "ma_value",
        "ma_severity":    "severity"
    })
    df_ma_sql.to_sql("ma_results", engine2, if_exists="replace", index=False)
    print("  ✓ table ma_results chargée en base")

    # Table zscore_results
    df_zs_sql = df_zs_full[zs_cols].copy()
    df_zs_sql["kpi_name"] = "oee_pct"
    df_zs_sql = df_zs_sql.rename(columns={"oee_pct": "kpi_value"})
    df_zs_sql.to_sql("zscore_results", engine2, if_exists="replace", index=False)
    print("  ✓ table zscore_results chargée en base")

    engine2.dispose()

    # ── Rapport texte ─────────────────────────────────────────────────────────
    print("\n→ Génération rapport texte...")

    report_path = "outputs_moving_anomaly/reports/ma_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:

        f.write("=" * 65 + "\n")
        f.write("   RAPPORT MOVING AVERAGE DEVIATION — OEE\n")
        f.write("=" * 65 + "\n\n")

        f.write(f"  Fenêtre MA    : 7 jours\n")
        f.write(f"  Seuil warning : 50%  d'écart vs MA\n")
        f.write(f"  Seuil critical: 100% d'écart vs MA\n")
        f.write(f"  Direction     : low (chutes uniquement)\n")
        f.write(f"  Stations      : {df['station_name'].nunique()}\n")
        f.write(f"  Période       : "
                f"{df['production_day'].min().date()} → "
                f"{df['production_day'].max().date()}\n\n")

        # Résumé MA par station
        f.write("── Résumé Moving Average par station ──\n\n")
        for station in sorted(df_ma_full["station_name"].dropna().unique()):
            grp = df_ma_full[df_ma_full["station_name"] == station]
            n_warn = (grp["ma_severity"] == "warning").sum()
            n_crit = (grp["ma_severity"] == "critical").sum()
            n_total = n_warn + n_crit
            max_dev = grp["ma_deviation_pct"].max()
            f.write(
                f"  {station:<22}  total={n_total:>3}  "
                f"warning={n_warn:>3}  critical={n_crit:>3}  "
                f"max_écart={max_dev*100:.1f}%\n"
            )

        # Résumé Z-Score par station
        f.write("\n── Résumé Z-Score par station ──\n\n")
        for station in sorted(df_zs_full["station_name"].dropna().unique()):
            grp = df_zs_full[df_zs_full["station_name"] == station]
            n_warn = (grp["severity"] == "warning").sum()
            n_crit = (grp["severity"] == "critical").sum()
            min_z  = grp["z_score"].min()
            f.write(
                f"  {station:<22}  warning={n_warn:>3}  "
                f"critical={n_crit:>3}  z_min={min_z:.2f}\n"
            )

        # Détail des anomalies critiques MA
        crit_ma = df_ma_full[df_ma_full["ma_severity"] == "critical"].sort_values(
            "ma_deviation_pct", ascending=False
        )
        if not crit_ma.empty:
            f.write("\n── Anomalies critiques MA (écart ≥ 100%) ──\n\n")
            for _, row in crit_ma.iterrows():
                f.write(
                    f"  {str(row['production_day'].date()):<12}  "
                    f"{str(row.get('station_name','')):<22}  "
                    f"OEE={row['oee_pct']:.1f}%  "
                    f"MA={row.get('ma_oee_pct', float('nan')):.1f}%  "
                    f"écart={row['ma_deviation_pct']*100:.1f}%\n"
                )

        # Détail des anomalies critiques Z-Score
        crit_zs = df_zs_full[df_zs_full["severity"] == "critical"].sort_values(
            "z_score"
        )
        if not crit_zs.empty:
            f.write("\n── Anomalies critiques Z-Score (z < -3) ──\n\n")
            for _, row in crit_zs.iterrows():
                f.write(
                    f"  {str(row['production_day'].date()):<12}  "
                    f"{str(row.get('station_name','')):<22}  "
                    f"OEE={row['oee_pct']:.1f}%  "
                    f"z={row['z_score']:.2f}\n"
                )

        f.write("\n" + "=" * 65 + "\n")

    print(f"  ✓ {report_path}")
    print("\n✅ Terminé.\n")