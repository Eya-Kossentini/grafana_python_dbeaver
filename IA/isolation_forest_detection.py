import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches

from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA


# =========================
# FOLDERS
# =========================

os.makedirs("outputs_isolation_forest/figures", exist_ok=True)
os.makedirs("outputs_isolation_forest/csv", exist_ok=True)


# =========================
# DATABASE CONNECTION
# =========================

engine = create_engine(
    "postgresql+psycopg2://postgres:admin123@localhost:5435/postgres"
)


# =========================
# COLORS
# =========================

COLOR_NORMAL = "#4A90D9"
COLOR_WARNING = "#F5A623"
COLOR_CRITICAL = "#D0021B"


# =========================
# LOAD DATA
# =========================

print("→ Loading KPI data from PostgreSQL...")

query = """
SELECT
    o.production_day,
    o.station_id,
    o.station_name,

    o.oee_pct,
    o.availability_pct,
    o.performance_pct,
    o.quality_pct,

    d.defect_rate_pct,
    dt.downtime_minutes,

    m.mtbf_hours,
    t.mttr_hours

FROM oee_kpi o

LEFT JOIN defect_rate_kpi d
    ON o.production_day = d.production_day
   AND o.station_id = d.station_id

LEFT JOIN downtime_by_station_kPI dt
    ON o.production_day = dt.production_day
   AND o.station_id = dt.station_id

LEFT JOIN mtbf_kpi m
    ON o.production_day = m.production_day
   AND o.station_id = m.station_id

LEFT JOIN mttr_kpi t
    ON o.production_day = t.production_day
   AND o.station_id = t.station_id
"""

df = pd.read_sql(query, engine)
df.columns = df.columns.str.strip()
df["production_day"] = pd.to_datetime(df["production_day"])

print("Rows loaded:", len(df))
print("Columns:", list(df.columns))


# =========================
# FEATURE SELECTION
# =========================

features = [
    "oee_pct",
    "availability_pct",
    "performance_pct",
    "quality_pct",
    "defect_rate_pct",
    "downtime_minutes",
    "mtbf_hours",
    "mttr_hours",
]


features = [col for col in features if col in df.columns]

for col in features:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df_model = df.copy()

print("\nValeurs manquantes par feature :")
print(df_model[features].isna().sum())

df_model[features] = df_model[features].fillna(df_model[features].median())
df_model[features] = df_model[features].fillna(0)

print("Rows after cleaning:", len(df_model))

# =========================
# NORMALIZATION
# =========================

X = df_model[features]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


# =========================
# ISOLATION FOREST MODEL
# =========================

model = IsolationForest(
    n_estimators=200,
    contamination=0.03,
    random_state=42
)

df_model["iforest_prediction"] = model.fit_predict(X_scaled)

# -1 = anomaly, 1 = normal
df_model["is_anomaly"] = df_model["iforest_prediction"] == -1

# Anomaly score: lower = more abnormal
df_model["anomaly_score"] = model.decision_function(X_scaled)

anomalies = df_model[df_model["is_anomaly"] == True].copy()

print("\n===== ISOLATION FOREST RESULTS =====")
print("Total observations:", len(df_model))
print("Total anomalies:", len(anomalies))
print("Anomaly rate:", round(len(anomalies) / len(df_model) * 100, 2), "%")


# =========================
# EXPORT CSV
# =========================

output_cols = [
    "production_day",
    "station_id",
    "station_name",
    "oee_pct",
    "availability_pct",
    "performance_pct",
    "quality_pct",
    "defect_rate_pct",
    "downtime_minutes",
    "mtbf_hours",
    "mttr_hours",
    "anomaly_score",
    "is_anomaly",
]

output_cols = [col for col in output_cols if col in df_model.columns]

df_model[output_cols].to_csv(
    "outputs_isolation_forest/csv/isolation_forest_results.csv",
    index=False
)

anomalies[output_cols].to_csv(
    "outputs_isolation_forest/csv/isolation_forest_anomalies.csv",
    index=False
)

print("✓ CSV files saved.")


# =========================
# GRAPH 1: ANOMALIES BY STATION
# =========================

station_counts = anomalies["station_name"].value_counts().sort_values(ascending=False)

plt.figure(figsize=(12, 5))
bars = plt.bar(station_counts.index, station_counts.values, color=COLOR_CRITICAL)

for bar, val in zip(bars, station_counts.values):
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 0.1,
        str(val),
        ha="center",
        va="bottom",
        fontweight="bold"
    )

plt.title("Isolation Forest — Anomalies by Station")
plt.xlabel("Station")
plt.ylabel("Number of anomalies")
plt.xticks(rotation=25, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(
    "outputs_isolation_forest/figures/iforest_anomalies_by_station.png",
    dpi=300,
    bbox_inches="tight"
)
plt.show()


# =========================
# GRAPH 2: HEATMAP STATION × WEEK
# =========================

df_model["week"] = df_model["production_day"].dt.to_period("W").astype(str)

pivot = df_model.pivot_table(
    index="station_name",
    columns="week",
    values="is_anomaly",
    aggfunc="sum",
    fill_value=0
)

fig, ax = plt.subplots(
    figsize=(max(14, len(pivot.columns) * 0.6), len(pivot) * 0.7 + 2)
)

im = ax.imshow(
    pivot.values,
    aspect="auto",
    cmap="YlOrRd",
    interpolation="nearest"
)

ax.set_xticks(range(len(pivot.columns)))
ax.set_xticklabels(pivot.columns, rotation=60, ha="right", fontsize=7)
ax.set_yticks(range(len(pivot.index)))
ax.set_yticklabels(pivot.index, fontsize=9)
ax.set_title("Isolation Forest — Anomaly Heatmap (Station × Week)")

plt.colorbar(im, ax=ax, label="Number of anomalies")
plt.tight_layout()
plt.savefig(
    "outputs_isolation_forest/figures/iforest_heatmap_station_week.png",
    dpi=300,
    bbox_inches="tight"
)
plt.show()


# =========================
# GRAPH 3: PCA 2D VISUALIZATION
# =========================

pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)

df_model["pca_1"] = X_pca[:, 0]
df_model["pca_2"] = X_pca[:, 1]

normal = df_model[df_model["is_anomaly"] == False]
anom = df_model[df_model["is_anomaly"] == True]

plt.figure(figsize=(10, 6))

plt.scatter(
    normal["pca_1"],
    normal["pca_2"],
    s=20,
    alpha=0.5,
    label="Normal"
)

plt.scatter(
    anom["pca_1"],
    anom["pca_2"],
    s=60,
    marker="X",
    label="Anomaly"
)

plt.title("Isolation Forest — Multivariate Anomaly Detection (PCA View)")
plt.xlabel("PCA Component 1")
plt.ylabel("PCA Component 2")
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(
    "outputs_isolation_forest/figures/iforest_pca_visualization.png",
    dpi=300,
    bbox_inches="tight"
)
plt.show()


# =========================
# PRINT TOP ANOMALIES
# =========================

print("\n===== TOP 20 MOST ABNORMAL OBSERVATIONS =====")

top_anomalies = anomalies.sort_values("anomaly_score").head(20)

print(
    top_anomalies[
        [
            "production_day",
            "station_name",
            "oee_pct",
            "availability_pct",
            "performance_pct",
            "quality_pct",
            "defect_rate_pct",
            "downtime_minutes",
            "mtbf_hours",
            "mttr_hours",
            "anomaly_score"
        ]
    ].to_string(index=False)
)

engine.dispose()

print("\n✅ Isolation Forest completed successfully.")

# Ajouter à la fin de chaque script Python, exemple pour isolation_forest
from sqlalchemy import text

df_model[output_cols].to_sql(
    "iforest_results",
    engine,
    if_exists="replace",
    index=False
)
print("✓ iforest_results chargé en base")

# ═════════════════════════════════════════════════════════════════════════════
# RAPPORT TEXTE — Isolation Forest
# ═════════════════════════════════════════════════════════════════════════════

import os
os.makedirs("outputs_isolation_forest/reports", exist_ok=True)

print("\n→ Génération rapport texte (Isolation Forest)...")

report_path = "outputs_isolation_forest/reports/iforest_report.txt"
with open(report_path, "w", encoding="utf-8") as f:

    f.write("=" * 65 + "\n")
    f.write("   RAPPORT ISOLATION FOREST ANOMALY DETECTION\n")
    f.write("=" * 65 + "\n\n")

    f.write(f"  Modèle            : IsolationForest\n")
    f.write(f"  n_estimators      : 200\n")
    f.write(f"  contamination     : 0.03 (3%)\n")
    f.write(f"  Features utilisées: {features}\n")
    f.write(f"  Total observations: {len(df_model)}\n")
    f.write(f"  Total anomalies   : {len(anomalies)}\n")
    f.write(
        f"  Taux anomalies    : "
        f"{round(len(anomalies)/len(df_model)*100, 2)}%\n"
    )
    f.write(f"  Période           : "
            f"{df_model['production_day'].min().date()} → "
            f"{df_model['production_day'].max().date()}\n\n")

    # ── Résumé par station ────────────────────────────────────────────────────
    f.write("── Anomalies par station ──\n\n")
    for station in sorted(df_model["station_name"].unique()):
        grp        = df_model[df_model["station_name"] == station]
        n_total    = len(grp)
        n_anom     = grp["is_anomaly"].sum()
        rate       = n_anom / n_total * 100 if n_total > 0 else 0
        score_min  = grp.loc[grp["is_anomaly"], "anomaly_score"].min() \
                     if n_anom > 0 else float("nan")
        f.write(
            f"  {station:<22}  total={n_total:>4}  "
            f"anomalies={n_anom:>3}  "
            f"taux={rate:.1f}%  "
            f"score_min={score_min:.4f}\n"
        )

    # ── Top 20 anomalies les plus sévères ────────────────────────────────────
    f.write("\n── Top 20 anomalies (score le plus bas = plus anormal) ──\n\n")
    top20 = anomalies.sort_values("anomaly_score").head(20)
    for _, row in top20.iterrows():
        f.write(
            f"  {str(row['production_day'].date()):<12}  "
            f"{str(row.get('station_name','')):<22}  "
            f"score={row['anomaly_score']:.4f}  "
            f"OEE={row.get('oee_pct', float('nan')):.1f}%  "
            f"defect={row.get('defect_rate_pct', float('nan')):.2f}%  "
            f"downtime={row.get('downtime_minutes', float('nan')):.0f}min\n"
        )

    # ── Résumé par feature (valeurs moyennes anomalies vs normal) ────────────
    f.write("\n── Profil moyen : anomalies vs normal ──\n\n")
    normal_df = df_model[df_model["is_anomaly"] == False]
    anom_df   = df_model[df_model["is_anomaly"] == True]

    f.write(f"  {'Feature':<25}  {'Normal (moy)':<15}  {'Anomalie (moy)':<15}  Écart\n")
    f.write("  " + "-" * 60 + "\n")
    for feat in features:
        if feat in df_model.columns:
            mean_normal = normal_df[feat].mean()
            mean_anom   = anom_df[feat].mean()
            ecart       = mean_anom - mean_normal
            f.write(
                f"  {feat:<25}  {mean_normal:<15.3f}  "
                f"{mean_anom:<15.3f}  "
                f"{'↑' if ecart > 0 else '↓'}{abs(ecart):.3f}\n"
            )

    # ── Distribution temporelle ───────────────────────────────────────────────
    f.write("\n── Anomalies par mois ──\n\n")
    anomalies_copy = anomalies.copy()
    anomalies_copy["month"] = anomalies_copy["production_day"].dt.to_period("M").astype(str)
    monthly = anomalies_copy.groupby("month").size()
    for month, count in monthly.items():
        bar = "█" * count
        f.write(f"  {month}  {bar}  ({count})\n")

    f.write("\n" + "=" * 65 + "\n")

print(f"  ✓ {report_path}")
print("\n✅ Isolation Forest completed successfully.")