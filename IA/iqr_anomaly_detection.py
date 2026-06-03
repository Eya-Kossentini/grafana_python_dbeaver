"""
Détection d'anomalies KPI industriels — Méthode IQR
=====================================================
IQR (Interquartile Range) : détection robuste sans hypothèse de normalité
KPIs : OEE, Downtime, Defect Rate
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
from sqlalchemy import create_engine

# ─────────────────────────────────────────────────────────────────────────────
# Connexion
# ─────────────────────────────────────────────────────────────────────────────

engine = create_engine("postgresql+psycopg2://postgres:admin123@localhost:5435/postgres")

# ─────────────────────────────────────────────────────────────────────────────
# Palette
# ─────────────────────────────────────────────────────────────────────────────

COLOR_LINE    = "#4A90D9"
COLOR_WARNING = "#F5A623"
COLOR_CRITICAL= "#D0021B"
COLOR_MA      = "#7ED321"
COLOR_IQR     = "#9B59B6"  # violet pour les bornes IQR

# ─────────────────────────────────────────────────────────────────────────────
# Fonction IQR générique
# ─────────────────────────────────────────────────────────────────────────────

def add_iqr_severity(df, col, direction="both", multiplier=1.5):
    """
    Ajoute les colonnes Q1, Q3, IQR, lower_bound, upper_bound, severity.
    direction :
        'low'  → anomalie uniquement si valeur < lower_bound  (OEE)
        'high' → anomalie uniquement si valeur > upper_bound  (Defect, Downtime)
        'both' → les deux côtés
    """
    df = df.copy()
    Q1  = df[col].quantile(0.25)
    Q3  = df[col].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - multiplier * IQR
    upper = Q3 + multiplier * IQR

    df["Q1"]          = Q1
    df["Q3"]          = Q3
    df["IQR"]         = IQR
    df["lower_bound"] = lower
    df["upper_bound"] = upper
    df["severity"]    = "normal"

    if direction in ("low", "both"):
        df.loc[df[col] < lower, "severity"] = "anomaly_low"
    if direction in ("high", "both"):
        df.loc[df[col] > upper, "severity"] = "anomaly_high"

    return df, Q1, Q3, IQR, lower, upper

def add_iqr_severity_group(df, col, group_col, direction="both", multiplier=1.5):
    """Applique IQR groupe par groupe (par station)."""
    parts = []
    for group_val, grp in df.groupby(group_col):
        grp = grp.copy()
        Q1  = grp[col].quantile(0.25)
        Q3  = grp[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - multiplier * IQR
        upper = Q3 + multiplier * IQR

        grp["Q1"]          = Q1
        grp["Q3"]          = Q3
        grp["IQR"]         = IQR
        grp["lower_bound"] = lower
        grp["upper_bound"] = upper
        grp["severity"]    = "normal"

        if direction in ("low", "both"):
            grp.loc[grp[col] < lower, "severity"] = "anomaly_low"
        if direction in ("high", "both"):
            grp.loc[grp[col] > upper, "severity"] = "anomaly_high"

        parts.append(grp)
    return pd.concat(parts, ignore_index=True)

# ═════════════════════════════════════════════════════════════════════════════
# 1.  OEE — IQR par station (on cherche les BAISSES → direction="low")
# ═════════════════════════════════════════════════════════════════════════════

print("→ Génération OEE (IQR)...")

df_oee = pd.read_sql("SELECT * FROM oee_kpi", engine)
df_oee.columns = df_oee.columns.str.strip()
df_oee["production_day"] = pd.to_datetime(df_oee["production_day"])
df_oee = df_oee.sort_values("production_day").reset_index(drop=True)

# APRÈS — 1.0×IQR + plancher absolu à 50%
df_oee = add_iqr_severity_group(df_oee, "oee_pct", "station_name",
                                 direction="low", multiplier=1.0)

# Appliquer le seuil absolu : toute valeur < 50% est aussi une anomalie
for station in df_oee["station_name"].unique():
    mask = df_oee["station_name"] == station
    iqr_val = df_oee.loc[mask, "IQR"].iloc[0]
    lb      = df_oee.loc[mask, "lower_bound"].iloc[0]

    if iqr_val < 10:
        # IQR trop compressé → appliquer le plancher absolu
        effective_lb = max(lb, 50.0)
        df_oee.loc[mask, "lower_bound"] = effective_lb
        df_oee.loc[mask & (df_oee["oee_pct"] < effective_lb), "severity"] = "anomaly_low"
    # sinon : garder le seuil IQR naturel, ne rien modifier

stations = sorted(df_oee["station_name"].unique())
n = len(stations)

fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(16, 4 * n), sharex=False)
if n == 1:
    axes = [axes]

fig.suptitle("IQR Anomaly Detection — OEE par Station", fontsize=16, fontweight="bold", y=1.001)

for ax, station in zip(axes, stations):
    grp      = df_oee[df_oee["station_name"] == station].copy().sort_values("production_day")
    anomalies = grp[grp["severity"] == "anomaly_low"]
    grp["ma7"] = grp["oee_pct"].rolling(7, min_periods=1).mean()

    lower_b = grp["lower_bound"].iloc[0]
    
    # Adapter le label pour indiquer la source du seuil
    iqr_val = grp["IQR"].iloc[0]
    label_seuil = (f"Seuil abs. 50% ({lower_b:.1f}%)" if iqr_val < 10
                else f"Seuil IQR 1.0× ({lower_b:.1f}%)")

    ax.axhline(lower_b, color=COLOR_CRITICAL, linewidth=1.2, linestyle="--",
            label=label_seuil)

    Q1_v    = grp["Q1"].iloc[0]
    Q3_v    = grp["Q3"].iloc[0]
    upper_b = grp["upper_bound"].iloc[0]

    ax.plot(grp["production_day"], grp["oee_pct"],
            color=COLOR_LINE, linewidth=1.2, alpha=0.7, label="OEE (%)")
    ax.plot(grp["production_day"], grp["ma7"],
            color=COLOR_MA, linewidth=1.5, linestyle="--", label="Moy. mobile 7j")

    # Bande IQR normale [Q1, Q3]
    ax.axhspan(Q1_v, Q3_v, alpha=0.10, color=COLOR_IQR, label=f"Zone IQR [Q1={Q1_v:.1f}, Q3={Q3_v:.1f}]")
    # Borne basse
    ax.axhline(lower_b, color=COLOR_CRITICAL, linewidth=1.2, linestyle="--",
               label=f"Seuil bas IQR ({lower_b:.1f}%)")

    if not anomalies.empty:
        ax.scatter(anomalies["production_day"], anomalies["oee_pct"],
                   color=COLOR_CRITICAL, zorder=6, s=80, marker="X",
                   label=f"Anomalie IQR — n={len(anomalies)}")

    mean_v = grp["oee_pct"].mean()
    ax.set_title(
        f"Station : {station}  |  Moy={mean_v:.1f}%  "
        f"Q1={Q1_v:.1f}  Q3={Q3_v:.1f}  IQR={grp['IQR'].iloc[0]:.1f}  "
        f"Seuil bas={lower_b:.1f}%  Anomalies={len(anomalies)}",
        fontsize=9, loc="left"
    )
    ax.set_ylabel("OEE (%)")
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
    ax.legend(fontsize=8, loc="lower right", ncol=3)
    ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig("outputs_iqr_anomaly_detection/figures/iqr_oee_par_station.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ iqr_oee_par_station.png sauvegardé")

# ── Heatmap OEE anomalies ────────────────────────────────────────────────────

df_oee["week"]       = df_oee["production_day"].dt.to_period("W").astype(str)
df_oee["is_anomaly"] = (df_oee["severity"] != "normal").astype(int)

pivot = df_oee.pivot_table(
    index="station_name", columns="week",
    values="is_anomaly", aggfunc="sum", fill_value=0
)

fig2, ax2 = plt.subplots(figsize=(max(14, len(pivot.columns) * 0.6), len(pivot) * 0.7 + 2))
im = ax2.imshow(pivot.values, aspect="auto", cmap="YlOrRd",
                interpolation="nearest", vmin=0, vmax=max(pivot.values.max(), 1))
ax2.set_xticks(range(len(pivot.columns)))
ax2.set_xticklabels(pivot.columns, rotation=60, ha="right", fontsize=7)
ax2.set_yticks(range(len(pivot.index)))
ax2.set_yticklabels(pivot.index, fontsize=9)
ax2.set_title("Heatmap — Anomalies OEE IQR par Station × Semaine", fontsize=13, fontweight="bold")
plt.colorbar(im, ax=ax2, label="Nb anomalies / semaine")
plt.tight_layout()
plt.savefig("outputs_iqr_anomaly_detection/figures/iqr_oee_heatmap.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ iqr_oee_heatmap.png sauvegardé")


# ═════════════════════════════════════════════════════════════════════════════
# 2.  DOWNTIME — IQR global (on cherche les PICS → direction="high")
# ═════════════════════════════════════════════════════════════════════════════

print("→ Génération Downtime (IQR)...")

df_dt = pd.read_sql("SELECT * FROM downtime_by_station_KPI", engine)
df_dt.columns = df_dt.columns.str.strip()
df_dt["production_day"] = pd.to_datetime(df_dt["production_day"])
df_dt = df_dt.sort_values("production_day").reset_index(drop=True)

df_dt, Q1_dt, Q3_dt, IQR_dt, lower_dt, upper_dt = add_iqr_severity(
    df_dt, "downtime_minutes", direction="high", multiplier=3.0
)
anomalies_dt = df_dt[df_dt["severity"] == "anomaly_high"]

print(f"  Downtime — Q1={Q1_dt:.1f}  Q3={Q3_dt:.1f}  IQR={IQR_dt:.1f}  "
      f"Seuil 3×IQR={upper_dt:.1f} min  Anomalies={len(anomalies_dt)}")


# ── 2a. Time series ──────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(16, 6))

ax.plot(df_dt["production_day"], df_dt["downtime_minutes"],
        color=COLOR_LINE, linewidth=1, alpha=0.7, label="Downtime (min)")
ax.scatter(anomalies_dt["production_day"], anomalies_dt["downtime_minutes"],
           color=COLOR_CRITICAL, s=70, zorder=5,
           label=f"Anomalies IQR — n={len(anomalies_dt)}")

ax.axhline(upper_dt, color=COLOR_CRITICAL, linewidth=1.5, linestyle="--",
           label=f"Seuil IQR Q3+3×IQR ({upper_dt:.0f} min)")
ax.axhline(Q3_dt, color=COLOR_IQR, linewidth=1, linestyle=":",
           label=f"Q3 ({Q3_dt:.0f} min)")
ax.axhline(Q1_dt, color=COLOR_MA, linewidth=1, linestyle=":",
           label=f"Q1 ({Q1_dt:.0f} min)")

ax.fill_between(df_dt["production_day"], Q1_dt, Q3_dt,
                alpha=0.08, color=COLOR_IQR, label="Zone IQR normale")

ax.set_title("IQR Anomaly Detection — Downtime", fontsize=13, fontweight="bold")
ax.set_xlabel("Production Day")
ax.set_ylabel("Downtime (minutes)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
ax.legend(fontsize=9)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_iqr_anomaly_detection/figures/iqr_downtime_anomalies.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ iqr_downtime_anomalies.png sauvegardé")

# ── 2b. Bar chart par type ────────────────────────────────────────────────────
type_counts = anomalies_dt["downtime_type"].value_counts()
bar_colors  = [COLOR_CRITICAL, COLOR_WARNING][:len(type_counts)]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(type_counts.index, type_counts.values, color=bar_colors)
for bar, val in zip(bars, type_counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
            str(val), ha="center", va="bottom", fontsize=11, fontweight="bold")

ax.set_title("Downtime Anomalies by Type (IQR)", fontsize=13, fontweight="bold")
ax.set_xlabel("Downtime Type")
ax.set_ylabel("Count")
ax.set_xticks(range(len(type_counts.index)))
ax.set_xticklabels(type_counts.index, rotation=20, ha="right", fontsize=10)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_iqr_anomaly_detection/figures/iqr_downtime_type.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ iqr_downtime_type.png sauvegardé")


# ═════════════════════════════════════════════════════════════════════════════
# 3.  DEFECT RATE — IQR par station (on cherche les HAUSSES → direction="high")
# ═════════════════════════════════════════════════════════════════════════════

print("→ Génération Defect Rate (IQR)...")

df_def = pd.read_sql("SELECT * FROM defect_rate_kpi", engine)
df_def.columns = df_def.columns.str.strip()
df_def["production_day"] = pd.to_datetime(df_def["production_day"])
df_def = df_def.sort_values("production_day").reset_index(drop=True)

# ── 3a. Vue agrégée journalière ───────────────────────────────────────────────
df_daily = (
    df_def.groupby("production_day")["defect_rate_pct"]
    .mean().reset_index().sort_values("production_day")
)

df_daily, Q1_d, Q3_d, IQR_d, lower_d, upper_d = add_iqr_severity(
    df_daily, "defect_rate_pct", direction="high"
)
df_daily["ma7"] = df_daily["defect_rate_pct"].rolling(7, min_periods=1).mean()

anom_daily = df_daily[df_daily["severity"] == "anomaly_high"]

fig, ax = plt.subplots(figsize=(16, 6))
ax.plot(df_daily["production_day"], df_daily["defect_rate_pct"],
        color=COLOR_LINE, linewidth=1, alpha=0.6, label="Defect Rate (%) — moy. journalière")
ax.plot(df_daily["production_day"], df_daily["ma7"],
        color=COLOR_MA, linewidth=1.8, linestyle="--", label="Moy. mobile 7j")

if not anom_daily.empty:
    ax.scatter(anom_daily["production_day"], anom_daily["defect_rate_pct"],
               color=COLOR_CRITICAL, s=90, marker="X", zorder=6,
               label=f"Anomalie IQR — n={len(anom_daily)}")

ax.axhline(upper_d, color=COLOR_CRITICAL, linewidth=1.5, linestyle="--",
           label=f"Seuil IQR ({upper_d:.2f}%)")
ax.fill_between(df_daily["production_day"], Q1_d, Q3_d,
                alpha=0.10, color=COLOR_IQR, label=f"Zone IQR [Q1={Q1_d:.2f}, Q3={Q3_d:.2f}]")
ax.axhline(df_daily["defect_rate_pct"].mean(), color=COLOR_MA,
           linewidth=0.8, linestyle=":", alpha=0.6,
           label=f"Moyenne ({df_daily['defect_rate_pct'].mean():.2f}%)")

ax.set_title("IQR Anomaly Detection — Defect Rate (agrégé par jour)", fontsize=13, fontweight="bold")
ax.set_xlabel("Production Day")
ax.set_ylabel("Defect Rate (%)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
ax.legend(fontsize=9, loc="upper right")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_iqr_anomaly_detection/figures/iqr_defect_rate_daily.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ iqr_defect_rate_daily.png sauvegardé")

# ── 3b. IQR par station + bar chart TOP 10 ───────────────────────────────────
df_def = add_iqr_severity_group(df_def, "defect_rate_pct", "station_name", direction="high")
anomalies_def  = df_def[df_def["severity"] == "anomaly_high"]
station_counts = anomalies_def["station_name"].value_counts().head(10)

# Couleur : rouge si la station a des valeurs très hautes (> Q3 + 3*IQR), orange sinon
def is_extreme(station):
    grp = df_def[df_def["station_name"] == station]
    extreme_bound = grp["Q3"].iloc[0] + 3 * grp["IQR"].iloc[0]
    return grp["defect_rate_pct"].max() > extreme_bound

colors_bar = [COLOR_CRITICAL if is_extreme(s) else COLOR_WARNING for s in station_counts.index]

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.bar(station_counts.index, station_counts.values, color=colors_bar)
for bar, val in zip(bars, station_counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
            str(val), ha="center", va="bottom", fontsize=10, fontweight="bold")

legend_patches = [
    mpatches.Patch(color=COLOR_CRITICAL, label="Extrême (> Q3 + 3×IQR)"),
    mpatches.Patch(color=COLOR_WARNING,  label="Anomalie IQR standard (> Q3 + 1.5×IQR)"),
]
ax.legend(handles=legend_patches, fontsize=9)
ax.set_title("Top Stations — Anomalies Defect Rate (IQR)", fontsize=13, fontweight="bold")
ax.set_xlabel("Station")
ax.set_ylabel("Nombre d'anomalies")
ax.set_xticks(range(len(station_counts.index)))
ax.set_xticklabels(station_counts.index, rotation=25, ha="right", fontsize=9)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("outputs_iqr_anomaly_detection/figures/iqr_defect_station.png", dpi=150, bbox_inches="tight")
plt.show()
print("  ✓ iqr_defect_station.png sauvegardé")

# ─────────────────────────────────────────────────────────────────────────────
engine.dispose()
print("\n✅ Tous les graphiques IQR générés et sauvegardés.")