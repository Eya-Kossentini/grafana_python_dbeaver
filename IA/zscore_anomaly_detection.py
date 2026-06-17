"""
Graphiques corrigés — Détection d'anomalies KPI industriels
==============================================================
Objectif : Appliquer la méthode Z-Score (écart à la moyenne en nombre
           d'écarts-types) sur les 3 KPI principaux : OEE, Downtime,
           Defect Rate — avec seuils warning (|z|>2) et critical (|z|>3).
"""

import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
from sqlalchemy import create_engine

# ─────────────────────────────────────────────────────────────────────────────
# Connexion PostgreSQL (SQLAlchemy — élimine le UserWarning)
# ─────────────────────────────────────────────────────────────────────────────

engine = create_engine("postgresql+psycopg2://postgres:admin123@localhost:5435/postgres")

# ─────────────────────────────────────────────────────────────────────────────
# Palette commune
# ─────────────────────────────────────────────────────────────────────────────

COLOR_LINE    = "#4A90D9"
COLOR_NORMAL  = "#4A90D9"
COLOR_WARNING = "#F5A623"
COLOR_CRITICAL= "#D0021B"
COLOR_MA      = "#7ED321"

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 0 — FONCTION UTILITAIRE Z-SCORE
# Calcul générique du z-score et de la sévérité (normal/warning/critical)
# pour un groupe donné. Utilisée comme référence pour la logique appliquée
# (en boucle) sur chaque KPI ci-dessous.
# ══════════════════════════════════════════════════════════════════════════════

def add_zscore_severity(group, col, direction="low"):
    group = group.copy()
    mean = group[col].mean()
    std  = group[col].std()
    group["z_score"] = (group[col] - mean) / std if std > 0 else 0
    group["severity"] = "normal"
    if direction == "low":
        group.loc[group["z_score"] < -2, "severity"] = "warning"
        group.loc[group["z_score"] < -3, "severity"] = "critical"
    else:
        group.loc[group["z_score"] >  2, "severity"] = "warning"
        group.loc[group["z_score"] >  3, "severity"] = "critical"
    return group

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1 — KPI OEE (Z-SCORE PAR STATION)
# Calcul du z-score par station (direction=low : on cherche les chutes
# d'OEE), génération du graphique multi-stations (bande ±2σ + anomalies)
# et de la heatmap hebdomadaire des anomalies.
# ═════════════════════════════════════════════════════════════════════════════

print("→ Génération des graphiques OEE...")

df_oee = pd.read_sql("SELECT * FROM oee_kpi", engine)
df_oee.columns = df_oee.columns.str.strip()

assert "station_name" in df_oee.columns, f"Colonne manquante ! Colonnes: {df_oee.columns.tolist()}"

df_oee["production_day"] = pd.to_datetime(df_oee["production_day"])
df_oee = df_oee.sort_values("production_day").reset_index(drop=True)

# ── Z-Score par station via boucle (évite la perte de colonnes avec groupby/apply) ──
parts = []
for station, grp in df_oee.groupby("station_name"):
    grp = grp.copy()
    mean = grp["oee_pct"].mean()
    std  = grp["oee_pct"].std()
    grp["z_score"] = (grp["oee_pct"] - mean) / std if std > 0 else 0
    grp["severity"] = "normal"
    grp.loc[grp["z_score"] < -2, "severity"] = "warning"
    grp.loc[grp["z_score"] < -3, "severity"] = "critical"
    parts.append(grp)

df_oee = pd.concat(parts, ignore_index=True)

stations = sorted(df_oee["station_name"].unique())
n = len(stations)

fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(16, 4 * n), sharex=False)
if n == 1:
    axes = [axes]

fig.suptitle("Z-Score Anomaly Detection — OEE par Station", fontsize=16, fontweight="bold", y=1.001)

for ax, station in zip(axes, stations):
    grp       = df_oee[df_oee["station_name"] == station].copy().sort_values("production_day")
    warnings  = grp[grp["severity"] == "warning"]
    criticals = grp[grp["severity"] == "critical"]
    grp["ma7"] = grp["oee_pct"].rolling(7, min_periods=1).mean()

    ax.plot(grp["production_day"], grp["oee_pct"],
            color=COLOR_LINE, linewidth=1.2, alpha=0.7, label="OEE (%)")
    ax.plot(grp["production_day"], grp["ma7"],
            color=COLOR_MA, linewidth=1.5, linestyle="--", label="Moy. mobile 7j")

    if not warnings.empty:
        ax.scatter(warnings["production_day"], warnings["oee_pct"],
                   color=COLOR_WARNING, zorder=5, s=70, label="Warning (z<-2)")
    if not criticals.empty:
        ax.scatter(criticals["production_day"], criticals["oee_pct"],
                   color=COLOR_CRITICAL, zorder=6, s=90, marker="X", label="Critical (z<-3)")

    mean_v = grp["oee_pct"].mean()
    std_v  = grp["oee_pct"].std()
    ax.axhspan(mean_v - 2*std_v, mean_v + 2*std_v,
               alpha=0.07, color=COLOR_LINE, label="Zone normale (±2σ)")
    ax.axhline(mean_v, color=COLOR_LINE, linewidth=0.8, linestyle=":", alpha=0.5)

    ax.set_title(f"Station : {station}  |  Moy={mean_v:.1f}%  σ={std_v:.1f}  "
                 f"Anomalies={len(warnings)+len(criticals)}", fontsize=10, loc="left")
    ax.set_ylabel("OEE (%)")
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
    ax.legend(fontsize=8, loc="lower right", ncol=3)
    ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig("outputs_zscore_anomaly_detection/figures/oee_par_station.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ oee_par_station.png sauvegardé")

# ── Heatmap anomalies OEE (station × semaine) ────────────────────────────────

df_oee["week"] = df_oee["production_day"].dt.to_period("W").astype(str)
df_oee["is_anomaly"] = (df_oee["severity"] != "normal").astype(int)

pivot = df_oee.pivot_table(
    index="station_name", columns="week",
    values="is_anomaly", aggfunc="sum", fill_value=0
)

fig2, ax2 = plt.subplots(figsize=(max(14, len(pivot.columns) * 0.6), len(pivot) * 0.7 + 2))
im = ax2.imshow(pivot.values, aspect="auto", cmap="YlOrRd", interpolation="nearest")
ax2.set_xticks(range(len(pivot.columns)))
ax2.set_xticklabels(pivot.columns, rotation=60, ha="right", fontsize=7)
ax2.set_yticks(range(len(pivot.index)))
ax2.set_yticklabels(pivot.index, fontsize=9)
ax2.set_title("Heatmap — Anomalies OEE par Station × Semaine", fontsize=13, fontweight="bold")
plt.colorbar(im, ax=ax2, label="Nb anomalies / semaine")
plt.tight_layout()
plt.savefig("outputs_zscore_anomaly_detection/figures/oee_heatmap_anomalies.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ oee_heatmap_anomalies.png sauvegardé")

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2 — KPI DOWNTIME (Z-SCORE GLOBAL)
# Calcul du z-score sur l'ensemble des stations (direction=high : on
# cherche les arrêts anormalement longs). Génère la série temporelle
# avec seuil critique et la répartition des anomalies par type de panne.
# ═════════════════════════════════════════════════════════════════════════════

print("→ Génération des graphiques Downtime...")

df_dt = pd.read_sql("SELECT * FROM downtime_by_station_KPI", engine)
df_dt.columns = df_dt.columns.str.strip()
df_dt["production_day"] = pd.to_datetime(df_dt["production_day"])
df_dt = df_dt.sort_values("production_day").reset_index(drop=True)

mean_dt = df_dt["downtime_minutes"].mean()
std_dt  = df_dt["downtime_minutes"].std()
df_dt["z_score"] = (df_dt["downtime_minutes"] - mean_dt) / std_dt
anomalies_dt = df_dt[df_dt["z_score"] > 3]

# ── 2a. Time series ──────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(16, 6))

ax.plot(df_dt["production_day"], df_dt["downtime_minutes"],
        color=COLOR_LINE, linewidth=1, alpha=0.7, label="Downtime (min)")
ax.scatter(anomalies_dt["production_day"], anomalies_dt["downtime_minutes"],
           color=COLOR_CRITICAL, s=70, zorder=5,
           label=f"Anomalies Z-Score (z>3) — n={len(anomalies_dt)}")

seuil = mean_dt + 3 * std_dt
ax.axhline(seuil, color=COLOR_CRITICAL, linewidth=1.2,
           linestyle="--", label=f"Seuil z=3 ({seuil:.0f} min)")
ax.axhline(mean_dt, color=COLOR_MA, linewidth=1,
           linestyle=":", label=f"Moyenne ({mean_dt:.0f} min)")

ax.set_title("Z-Score Anomaly Detection — Downtime", fontsize=13, fontweight="bold")
ax.set_xlabel("Production Day")
ax.set_ylabel("Downtime (minutes)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
ax.legend(fontsize=9)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_zscore_anomaly_detection/figures/zscore_downtime_anomalies.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ zscore_downtime_anomalies.png sauvegardé")

# ── 2b. Histogramme par type ──────────────────────────────────────────────────
type_counts = anomalies_dt["downtime_type"].value_counts()
bar_colors  = [COLOR_CRITICAL, COLOR_WARNING, COLOR_LINE][:len(type_counts)]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(type_counts.index, type_counts.values, color=bar_colors)

for bar, val in zip(bars, type_counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
            str(val), ha="center", va="bottom", fontsize=11, fontweight="bold")

ax.set_title("Downtime Anomalies by Type (Z-Score > 3)", fontsize=13, fontweight="bold")
ax.set_xlabel("Downtime Type")
ax.set_ylabel("Count")
ax.set_xticks(range(len(type_counts.index)))
ax.set_xticklabels(type_counts.index, rotation=20, ha="right", fontsize=10)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_zscore_anomaly_detection/figures/downtime_type_distribution.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ downtime_type_distribution.png sauvegardé")

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3 — KPI DEFECT RATE (Z-SCORE GLOBAL + PAR STATION)
# Deux niveaux d'analyse : vue agrégée journalière (toutes stations,
# direction=high) avec zones warning/critique, puis détail par station
# pour identifier le top 10 des stations les plus impactées.
# ═════════════════════════════════════════════════════════════════════════════

print("→ Génération des graphiques Defect Rate...")

df_def = pd.read_sql("SELECT * FROM defect_rate_kpi", engine)
df_def.columns = df_def.columns.str.strip()

if "production_day" in df_def.columns:
    df_def["production_day"] = pd.to_datetime(df_def["production_day"])
    df_daily = (
        df_def.groupby("production_day")["defect_rate_pct"]
        .mean()
        .reset_index()
        .sort_values("production_day")
    )
else:
    df_daily = df_def[["defect_rate_pct"]].copy()
    df_daily["production_day"] = df_daily.index

mean_def = df_daily["defect_rate_pct"].mean()
std_def  = df_daily["defect_rate_pct"].std()
df_daily["z_score"]  = (df_daily["defect_rate_pct"] - mean_def) / std_def
df_daily["severity"] = "normal"
df_daily.loc[df_daily["z_score"] > 2, "severity"] = "warning"
df_daily.loc[df_daily["z_score"] > 3, "severity"] = "critical"
df_daily["ma7"] = df_daily["defect_rate_pct"].rolling(7, min_periods=1).mean()

warn_d = df_daily[df_daily["severity"] == "warning"]
crit_d = df_daily[df_daily["severity"] == "critical"]

# ── 3a. Time series agrégée ──────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(16, 6))

ax.plot(df_daily["production_day"], df_daily["defect_rate_pct"],
        color=COLOR_LINE, linewidth=1, alpha=0.6, label="Defect Rate (%) — moy. journalière")
ax.plot(df_daily["production_day"], df_daily["ma7"],
        color=COLOR_MA, linewidth=1.8, linestyle="--", label="Moy. mobile 7j")

if not warn_d.empty:
    ax.scatter(warn_d["production_day"], warn_d["defect_rate_pct"],
               color=COLOR_WARNING, s=70, zorder=5, label=f"Warning (z>2) — n={len(warn_d)}")
if not crit_d.empty:
    ax.scatter(crit_d["production_day"], crit_d["defect_rate_pct"],
               color=COLOR_CRITICAL, s=90, marker="X", zorder=6, label=f"Critical (z>3) — n={len(crit_d)}")

ax.axhspan(mean_def + 2*std_def, mean_def + 3*std_def,
           alpha=0.08, color=COLOR_WARNING, label="Zone warning")
ax.axhspan(mean_def + 3*std_def, df_daily["defect_rate_pct"].max() * 1.1,
           alpha=0.08, color=COLOR_CRITICAL, label="Zone critique")
ax.axhline(mean_def, color=COLOR_MA, linewidth=0.8, linestyle=":", alpha=0.6,
           label=f"Moyenne ({mean_def:.2f}%)")

ax.set_title("Z-Score Anomaly Detection — Defect Rate (agrégé par jour)", fontsize=13, fontweight="bold")
ax.set_xlabel("Production Day")
ax.set_ylabel("Defect Rate (%)")
if pd.api.types.is_datetime64_any_dtype(df_daily["production_day"]):
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
ax.legend(fontsize=9, loc="upper right")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_zscore_anomaly_detection/figures/zscore_defect_rate_anomalies.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ zscore_defect_rate_anomalies.png sauvegardé")

# ── 3b. Histogramme par station — TOP 10 ─────────────────────────────────────
# Boucle à la place de groupby/apply pour conserver station_name
parts_def = []
for station, grp in df_def.groupby("station_name"):
    grp = grp.copy()
    mean = grp["defect_rate_pct"].mean()
    std  = grp["defect_rate_pct"].std()
    grp["z_score"] = (grp["defect_rate_pct"] - mean) / std if std > 0 else 0
    grp["severity"] = "normal"
    grp.loc[grp["z_score"] > 2, "severity"] = "warning"
    grp.loc[grp["z_score"] > 3, "severity"] = "critical"
    parts_def.append(grp)

df_def = pd.concat(parts_def, ignore_index=True)

anomalies_def  = df_def[df_def["severity"] != "normal"]
station_counts = anomalies_def["station_name"].value_counts().head(10)

colors_bar = [
    COLOR_CRITICAL if anomalies_def[anomalies_def["station_name"] == s]["severity"].eq("critical").any()
    else COLOR_WARNING
    for s in station_counts.index
]

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.bar(station_counts.index, station_counts.values, color=colors_bar)

for bar, val in zip(bars, station_counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
            str(val), ha="center", va="bottom", fontsize=10, fontweight="bold")

legend_patches = [
    mpatches.Patch(color=COLOR_CRITICAL, label="Contient des Critical (z>3)"),
    mpatches.Patch(color=COLOR_WARNING,  label="Warning uniquement (z>2)"),
]
ax.legend(handles=legend_patches, fontsize=9)
ax.set_title("Top Stations — Anomalies Defect Rate (Z-Score)", fontsize=13, fontweight="bold")
ax.set_xlabel("Station")
ax.set_ylabel("Nombre d'anomalies")
ax.set_xticks(range(len(station_counts.index)))
ax.set_xticklabels(station_counts.index, rotation=25, ha="right", fontsize=9)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_zscore_anomaly_detection/figures/defect_station_distribution.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ defect_station_distribution.png sauvegardé")

# ─────────────────────────────────────────────────────────────────────────────
engine.dispose()
print("\n✅ Tous les graphiques générés et sauvegardés.")

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4 — EXPORT CSV + POSTGRESQL (Z-SCORE RESULTS)
# Export de chaque KPI (OEE, Downtime, Defect Rate) en CSV (complet +
# anomalies), puis consolidation dans la table PostgreSQL unifiée
# "zscore_results" (format long, une ligne par observation/KPI).
# ═════════════════════════════════════════════════════════════════════════════

os.makedirs("outputs_zscore_anomaly_detection/csv", exist_ok=True)

print("\n→ Export CSV et PostgreSQL (Z-Score)...")

# ── OEE ──────────────────────────────────────────────────────────────────────
# df_oee est déjà calculé avec z_score et severity plus haut dans le script
zscore_oee_cols = [
    "production_day", "station_id", "station_name",
    "oee_pct", "z_score", "severity"
]
zscore_oee_cols = [c for c in zscore_oee_cols if c in df_oee.columns]

df_oee[zscore_oee_cols].to_csv(
    "outputs_zscore_anomaly_detection/csv/zscore_oee_all.csv", index=False
)
print("  ✓ zscore_oee_all.csv")

df_oee[df_oee["severity"] != "normal"][zscore_oee_cols].to_csv(
    "outputs_zscore_anomaly_detection/csv/zscore_oee_anomalies.csv", index=False
)
print("  ✓ zscore_oee_anomalies.csv")

# Export PostgreSQL
df_oee_sql = df_oee[zscore_oee_cols].copy()
df_oee_sql["kpi_name"] = "oee_pct"
df_oee_sql = df_oee_sql.rename(columns={"oee_pct": "kpi_value"})

engine2 = create_engine("postgresql+psycopg2://postgres:admin123@localhost:5435/postgres")
df_oee_sql.to_sql("zscore_results", engine2, if_exists="replace", index=False)
print("  ✓ table zscore_results (OEE) chargée en base")
# ── Downtime ─────────────────────────────────────────────────────────────────
zscore_dt_cols_csv = [
    "production_day", "station_id", "station_name",
    "downtime_minutes", "downtime_type", "z_score", "severity"
]
zscore_dt_cols_csv = [c for c in zscore_dt_cols_csv if c in df_dt.columns]

# Ajouter severity si absente
if "severity" not in df_dt.columns:
    df_dt["severity"] = "normal"
    df_dt.loc[df_dt["z_score"] > 2, "severity"] = "warning"
    df_dt.loc[df_dt["z_score"] > 3, "severity"] = "critical"

# CSV — garde downtime_type (utile pour analyse)
df_dt[zscore_dt_cols_csv].to_csv(
    "outputs_zscore_anomaly_detection/csv/zscore_downtime_all.csv", index=False
)
print("  ✓ zscore_downtime_all.csv")

df_dt[df_dt["severity"] != "normal"][zscore_dt_cols_csv].to_csv(
    "outputs_zscore_anomaly_detection/csv/zscore_downtime_anomalies.csv", index=False
)
print("  ✓ zscore_downtime_anomalies.csv")

# PostgreSQL — supprimer downtime_type (colonne hors schéma zscore_results)
zscore_dt_cols_sql = [
    "production_day", "station_id", "station_name",
    "z_score", "severity"
]
zscore_dt_cols_sql = [c for c in zscore_dt_cols_sql if c in df_dt.columns]

df_dt_sql = df_dt[zscore_dt_cols_sql].copy()
df_dt_sql["kpi_name"]  = "downtime_minutes"
df_dt_sql["kpi_value"] = df_dt["downtime_minutes"].values

df_dt_sql.to_sql("zscore_results", engine2, if_exists="append", index=False)
print("  ✓ table zscore_results (Downtime) appendée")

# ── Defect Rate ───────────────────────────────────────────────────────────────
zscore_def_cols = [
    "production_day", "station_id", "station_name",
    "defect_rate_pct", "z_score", "severity"
]
zscore_def_cols = [c for c in zscore_def_cols if c in df_def.columns]

df_def[zscore_def_cols].to_csv(
    "outputs_zscore_anomaly_detection/csv/zscore_defect_all.csv", index=False
)
print("  ✓ zscore_defect_all.csv")

df_def[df_def["severity"] != "normal"][zscore_def_cols].to_csv(
    "outputs_zscore_anomaly_detection/csv/zscore_defect_anomalies.csv", index=False
)
print("  ✓ zscore_defect_anomalies.csv")

df_def_sql = df_def[zscore_def_cols].copy()
df_def_sql["kpi_name"] = "defect_rate_pct"
df_def_sql = df_def_sql.rename(columns={"defect_rate_pct": "kpi_value"})
df_def_sql.to_sql("zscore_results", engine2, if_exists="append", index=False)
print("  ✓ table zscore_results (Defect Rate) appendée")

engine2.dispose()
print("\n✅ Exports Z-Score terminés.")

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5 — RAPPORT TEXTE
# Génération d'un rapport texte consolidé (zscore_report.txt) résumant
# moyenne, écart-type, seuils et détail des anomalies pour les 3 KPI.
# ═════════════════════════════════════════════════════════════════════════════

os.makedirs("outputs_zscore_anomaly_detection/reports", exist_ok=True)

print("\n→ Génération rapport texte (Z-Score)...")

report_path = "outputs_zscore_anomaly_detection/reports/zscore_report.txt"
with open(report_path, "w", encoding="utf-8") as f:

    f.write("=" * 65 + "\n")
    f.write("   RAPPORT Z-SCORE ANOMALY DETECTION\n")
    f.write("=" * 65 + "\n\n")

    f.write(f"  Période OEE      : "
            f"{df_oee['production_day'].min().date()} → "
            f"{df_oee['production_day'].max().date()}\n")
    f.write(f"  Période Downtime : "
            f"{df_dt['production_day'].min().date()} → "
            f"{df_dt['production_day'].max().date()}\n")
    f.write(f"  Période Defect   : "
            f"{df_def['production_day'].min().date()} → "
            f"{df_def['production_day'].max().date()}\n\n")

    # ── OEE ──────────────────────────────────────────────────────────────────
    f.write("── OEE (Z-Score, direction=low) ──\n\n")
    for station in sorted(df_oee["station_name"].unique()):
        grp    = df_oee[df_oee["station_name"] == station]
        n_warn = (grp["severity"] == "warning").sum()
        n_crit = (grp["severity"] == "critical").sum()
        mean_v = grp["oee_pct"].mean()
        std_v  = grp["oee_pct"].std()
        min_z  = grp["z_score"].min() if "z_score" in grp.columns else float("nan")
        f.write(
            f"  {station:<22}  moy={mean_v:.1f}%  σ={std_v:.1f}  "
            f"z_min={min_z:.2f}  warning={n_warn}  critical={n_crit}\n"
        )

    anom_oee = df_oee[df_oee["severity"] != "normal"].sort_values(
        ["station_name", "production_day"]
    )
    if not anom_oee.empty:
        f.write("\n  Détail anomalies OEE :\n")
        for _, row in anom_oee.iterrows():
            z = row["z_score"] if "z_score" in row else float("nan")
            f.write(
                f"    {str(row['production_day'].date()):<12}  "
                f"{str(row.get('station_name','')):<22}  "
                f"OEE={row['oee_pct']:.1f}%  "
                f"z={z:.2f}  "
                f"sévérité={row['severity']}\n"
            )

    # ── Downtime ─────────────────────────────────────────────────────────────
    f.write(f"\n── Downtime (Z-Score > 3) ──\n\n")
    f.write(
        f"  Moyenne={mean_dt:.1f} min  σ={std_dt:.1f}  "
        f"Seuil z=3 → {mean_dt + 3*std_dt:.1f} min  "
        f"Anomalies={len(anomalies_dt)}\n\n"
    )
    if not anomalies_dt.empty:
        f.write("  Par type de downtime :\n")
        for dtype, count in anomalies_dt["downtime_type"].value_counts().items():
            f.write(f"    {dtype:<30}  {count}\n")
        f.write("\n  Détail anomalies Downtime :\n")
        for _, row in anomalies_dt.sort_values(
            "downtime_minutes", ascending=False
        ).iterrows():
            z = row["z_score"] if "z_score" in row else float("nan")
            f.write(
                f"    {str(row['production_day'].date()):<12}  "
                f"{str(row.get('station_name','')):<22}  "
                f"{row['downtime_minutes']:.0f} min  "
                f"z={z:.2f}  "
                f"type={row.get('downtime_type','')}\n"
            )

    # ── Defect Rate ───────────────────────────────────────────────────────────
    f.write(f"\n── Defect Rate (Z-Score, direction=high) ──\n\n")
    f.write(
        f"  Vue agrégée — moy={mean_def:.2f}%  σ={std_def:.2f}  "
        f"Seuil warning z>2 → {mean_def+2*std_def:.2f}%  "
        f"Seuil critical z>3 → {mean_def+3*std_def:.2f}%\n"
        f"  Anomalies agrégées — warning={len(warn_d)}  critical={len(crit_d)}\n\n"
    )
    f.write("  Anomalies par station :\n")
    for station in sorted(df_def["station_name"].unique()):
        grp    = df_def[df_def["station_name"] == station]
        n_warn = (grp["severity"] == "warning").sum()
        n_crit = (grp["severity"] == "critical").sum()
        mean_s = grp["defect_rate_pct"].mean()
        std_s  = grp["defect_rate_pct"].std()
        f.write(
            f"    {station:<22}  moy={mean_s:.2f}%  σ={std_s:.3f}  "
            f"warning={n_warn}  critical={n_crit}\n"
        )

    anom_def_detail = df_def[df_def["severity"] != "normal"].sort_values(
        ["station_name", "production_day"]
    )
    if not anom_def_detail.empty:
        f.write("\n  Détail anomalies Defect Rate :\n")
        for _, row in anom_def_detail.iterrows():
            z = row["z_score"] if "z_score" in row else float("nan")
            f.write(
                f"    {str(row['production_day'].date()):<12}  "
                f"{str(row.get('station_name','')):<22}  "
                f"defect={row['defect_rate_pct']:.2f}%  "
                f"z={z:.2f}  "
                f"sévérité={row['severity']}\n"
            )

    f.write("\n" + "=" * 65 + "\n")

print(f"  ✓ {report_path}")
print("\n✅ Exports Z-Score terminés.")