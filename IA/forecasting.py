"""
Prévision court terme — Defect Rate J+1
=========================================
Objectif : Prévoir le taux de rebuts du lendemain (J+1) par station
           afin d'anticiper les dérives qualité.

Méthodes :
  1. Moving Average Forecast    → moyenne des 7 derniers jours
  2. Exponential Smoothing (Holt) → modèle pondéré, plus réactif

Source de données :
  PostgreSQL → table defect_rate_kpi
  Colonnes attendues : production_day, station_id, station_name, defect_rate_pct

Usage :
  python forecasting_defect_rate.py
  python forecasting_defect_rate.py --station 2
  python forecasting_defect_rate.py --window 14
"""

import argparse
import sys
import os
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

os.makedirs("outputs_forecasting_prevision/figures", exist_ok=True)
os.makedirs("outputs_forecasting_prevision/csv",     exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Palette
# ─────────────────────────────────────────────────────────────────────────────
C_ACTUAL  = "#4A90D9"
C_MA      = "#F5A623"
C_ES      = "#7ED321"
C_ERROR   = "#D0021B"
C_FEATURE = "#9B59B6"
C_GRID    = "#E8E8E8"


# ══════════════════════════════════════════════════════════════════════════════
# 1. CHARGEMENT DES DONNÉES
# ══════════════════════════════════════════════════════════════════════════════

def load_data(station_id: int = None) -> pd.DataFrame:
    """
    Charge la table defect_rate_kpi depuis PostgreSQL.
    Si station_id est fourni, filtre sur cette station uniquement.
    """
    print("→ Connexion PostgreSQL...")
    engine = create_engine(
        "postgresql+psycopg2://postgres:admin123@localhost:5435/postgres"
    )

    df = pd.read_sql("SELECT * FROM defect_rate_kpi", engine)
    engine.dispose()

    df.columns          = df.columns.str.strip()
    df["production_day"] = pd.to_datetime(df["production_day"])
    df = df.sort_values(["station_name", "production_day"]).reset_index(drop=True)

    if station_id is not None:
        df = df[df["station_id"] == station_id].reset_index(drop=True)
        print(f"  Filtre station_id={station_id} → {len(df)} lignes")

    print(f"  {len(df)} observations · {df['station_id'].nunique()} station(s)")
    print(f"  Colonnes : {list(df.columns)}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2. FEATURE ENGINEERING  (méthodologie groupby + transform)
# ══════════════════════════════════════════════════════════════════════════════

def build_features(df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    """
    Construit les features par station via groupby + transform.

    Features :
    ┌──────────────────────┬──────────────────────────────────────────────────┐
    │ Feature              │ Rôle                                             │
    ├──────────────────────┼──────────────────────────────────────────────────┤
    │ rolling_mean_7d      │ Moyenne mobile 7j (tendance récente)             │
    │ rolling_std_7d       │ Volatilité 7j (instabilité du processus)         │
    │ previous_day_scrap   │ Taux rebuts J-1 (inertie)                        │
    │ defect_trend         │ Écart entre réel et moyenne 7j (dérive)          │
    │ rolling_mean_3d      │ Réactivité court terme                           │
    │ day_of_week          │ Effet jour (0=lundi)                             │
    └──────────────────────┴──────────────────────────────────────────────────┘
    """
    g = df.groupby("station_name")["defect_rate_pct"]

    # Moyenne mobile — shift(1) pour éviter la fuite de données (data leakage)
    df["rolling_mean_7d"] = g.transform(
        lambda x: x.shift(1).rolling(window, min_periods=3).mean()
    )
    df["rolling_mean_3d"] = g.transform(
        lambda x: x.shift(1).rolling(3, min_periods=2).mean()
    )

    # Volatilité
    df["rolling_std_7d"] = g.transform(
        lambda x: x.shift(1).rolling(window, min_periods=3).std()
    )

    # Valeur de la veille
    df["previous_day_scrap"] = g.transform(lambda x: x.shift(1))

    # Tendance = écart entre réel et moyenne 7j (positif = dérive à la hausse)
    df["defect_trend"] = df["defect_rate_pct"] - df["rolling_mean_7d"]

    # Effet jour de semaine
    df["day_of_week"] = df["production_day"].dt.dayofweek  # 0 = lundi

    new_cols = ["rolling_mean_7d", "rolling_mean_3d", "rolling_std_7d",
                "previous_day_scrap", "defect_trend", "day_of_week"]
    print(f"  ✓ Features créées : {new_cols}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3. MODÈLES DE PRÉVISION
# ══════════════════════════════════════════════════════════════════════════════

def forecast_moving_average(series: pd.Series, window: int = 7) -> pd.Series:
    """
    Prévision J+1 = moyenne des `window` derniers jours.
        Ŷ_{t+1} = (1/n) × Σ Y_{t-n+1..t}
    """
    return series.shift(1).rolling(window=window, min_periods=3).mean()


def forecast_exponential_smoothing(series: pd.Series) -> pd.Series:
    """
    Holt Exponential Smoothing (capture tendance).
        Level  : L_t = α·Y_t + (1-α)·(L_{t-1} + T_{t-1})
        Trend  : T_t = β·(L_t - L_{t-1}) + (1-β)·T_{t-1}
        Ŷ_{t+h} = L_t + h·T_t
    """
    try:
        from statsmodels.tsa.holtwinters import Holt
    except ImportError:
        print("  ⚠ statsmodels manquant → pip install statsmodels")
        return pd.Series(np.nan, index=series.index)

    valid = series.dropna()
    if len(valid) < 5:
        return pd.Series(np.nan, index=series.index)

    forecasts = pd.Series(np.nan, index=series.index)
    try:
        fit     = Holt(valid, initialization_method="estimated").fit(optimized=True)
        fitted  = fit.fittedvalues
        for idx in fitted.index:
            if idx in forecasts.index:
                forecasts[idx] = fitted[idx]
        forecasts = forecasts.shift(1)   # simuler prévision J+1
    except Exception as e:
        print(f"  ⚠ ES fitting error: {e}")

    return forecasts


def predict_next_day(series: pd.Series, window: int = 7) -> dict:
    """
    Calcule la prévision pour le vrai J+1 (demain) en utilisant
    toute la série disponible.
    Fonctionne avec un index datetime ou entier.
    """
    valid = series.dropna()
    if len(valid) < 3:
        return {"ma": np.nan, "es": np.nan, "date": None}

    # Calcul de la date J+1 — robuste quel que soit le type d'index
    last_idx = series.index[-1]
    try:
        next_date = last_idx + pd.Timedelta(days=1)
    except TypeError:
        next_date = None

    # MA : moyenne des `window` derniers jours réels
    ma_pred = float(valid.tail(window).mean())

    # ES : index entier pour éviter le ValueWarning statsmodels (no freq)
    try:
        from statsmodels.tsa.holtwinters import Holt
        valid_int = valid.copy()
        valid_int.index = pd.RangeIndex(len(valid_int))
        fit     = Holt(valid_int, initialization_method="estimated").fit(optimized=True)
        es_pred = float(fit.forecast(1).iloc[0])
    except Exception:
        alpha = 0.3
        level = float(valid.iloc[0])
        for v in valid:
            level = alpha * float(v) + (1 - alpha) * level
        es_pred = level

    return {"ma": round(ma_pred, 3), "es": round(es_pred, 3), "date": next_date}


def evaluate_model(actual: pd.Series, predicted: pd.Series, name: str) -> dict:
    mask = actual.notna() & predicted.notna()
    a, p = actual[mask], predicted[mask]
    if len(a) < 3:
        return {"model": name, "n": 0, "MAE": np.nan, "RMSE": np.nan, "MAPE": np.nan}
    mae  = mean_absolute_error(a, p)
    rmse = np.sqrt(mean_squared_error(a, p))
    mape = np.mean(np.abs((a - p) / a.replace(0, np.nan))) * 100
    return {"model": name, "n": len(a),
            "MAE": round(mae, 4), "RMSE": round(rmse, 4), "MAPE": round(mape, 2)}


# ══════════════════════════════════════════════════════════════════════════════
# 4. GRAPHIQUES
# ══════════════════════════════════════════════════════════════════════════════

def plot_forecast_per_station(df_feat: pd.DataFrame, window: int = 7):
    stations   = sorted(df_feat["station_id"].unique())
    n          = len(stations)
    fig, axes  = plt.subplots(
        nrows=n, ncols=2, figsize=(20, 5 * n),
        gridspec_kw={"width_ratios": [3, 1]}
    )
    if n == 1:
        axes = [axes]   # garantit toujours une liste de paires

    fig.suptitle(
        f"Prévision J+1 — Defect Rate  (MA{window}j · Exponential Smoothing Holt)",
        fontsize=15, fontweight="bold", y=1.001
    )

    all_metrics, next_day_preds = [], []

    for (ax_main, ax_err), sid in zip(axes, stations):
        grp = (df_feat[df_feat["station_id"] == sid]
               .copy()
               .sort_values("production_day")
               .set_index("production_day"))
        col = "defect_rate_pct"

        grp["ma_forecast"] = forecast_moving_average(grp[col], window=window)
        grp["es_forecast"] = forecast_exponential_smoothing(grp[col])

        # Intervalle de confiance ±1.5σ
        rolling_std     = grp[col].shift(1).rolling(window, min_periods=3).std()
        grp["ma_upper"] = grp["ma_forecast"] + 1.5 * rolling_std
        grp["ma_lower"] = grp["ma_forecast"] - 1.5 * rolling_std

        m_ma = evaluate_model(grp[col], grp["ma_forecast"], f"MA{window}")
        m_es = evaluate_model(grp[col], grp["es_forecast"], "ES Holt")
        m_ma["station_id"] = sid
        m_es["station_id"] = sid
        all_metrics += [m_ma, m_es]

        nxt = predict_next_day(grp[col], window=window)
        next_day_preds.append({
            "station_id":  sid,
            "date_J1":     (grp.index[-1] + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            "ma_pred":     nxt["ma"],
            "es_pred":     nxt["es"],
            "last_actual": round(float(grp[col].iloc[-1]), 3),
        })

        # ── Série + prévisions ───────────────────────────────────────────────
        ax_main.plot(grp.index, grp[col],
                     color=C_ACTUAL, linewidth=1.3, alpha=0.8, label="Réel")
        ax_main.plot(grp.index, grp["ma_forecast"],
                     color=C_MA, linewidth=1.8, linestyle="--",
                     label=f"MA{window} (MAE={m_ma['MAE']:.3f})")
        ax_main.plot(grp.index, grp["es_forecast"],
                     color=C_ES, linewidth=1.8, linestyle="-.",
                     label=f"ES Holt (MAE={m_es['MAE']:.3f})")
        ax_main.fill_between(grp.index, grp["ma_lower"], grp["ma_upper"],
                             alpha=0.10, color=C_MA, label="IC ±1.5σ")

        # Point J+1
        j1_date = grp.index[-1] + pd.Timedelta(days=1)
        ax_main.scatter([j1_date], [nxt["ma"]], color=C_MA, s=120,
                        zorder=8, marker="D", label=f"J+1 MA={nxt['ma']}%")
        ax_main.scatter([j1_date], [nxt["es"]], color=C_ES, s=120,
                        zorder=8, marker="*", label=f"J+1 ES={nxt['es']}%")
        ax_main.axvline(grp.index[-1], color="gray", linewidth=0.8,
                        linestyle=":", alpha=0.6, label="Aujourd'hui")

        ax_main.set_title(
            f"Station {sid}  |  MAE MA={m_ma['MAE']:.3f}  MAE ES={m_es['MAE']:.3f}  "
            f"MAPE MA={m_ma['MAPE']:.1f}%  MAPE ES={m_es['MAPE']:.1f}%",
            fontsize=10, loc="left"
        )
        ax_main.set_ylabel("Defect Rate (%)")
        ax_main.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax_main.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax_main.xaxis.get_majorticklabels(), rotation=30, ha="right")
        ax_main.legend(fontsize=8, loc="upper left", ncol=3)
        ax_main.grid(axis="y", alpha=0.3, color=C_GRID)

        # ── Résidus ──────────────────────────────────────────────────────────
        res_ma = grp[col] - grp["ma_forecast"]
        res_es = grp[col] - grp["es_forecast"]

        ax_err.bar(grp.index, res_ma, color=C_MA, alpha=0.5, width=0.8, label=f"MA{window}")
        ax_err.plot(grp.index, res_es, color=C_ES, linewidth=1.2, label="ES Holt")
        ax_err.axhline(0, color="black", linewidth=0.8)
        ax_err.axhline( res_ma.std() * 2, color=C_ERROR, linewidth=0.7, linestyle="--", alpha=0.6)
        ax_err.axhline(-res_ma.std() * 2, color=C_ERROR, linewidth=0.7, linestyle="--", alpha=0.6)
        ax_err.set_title("Résidus", fontsize=9)
        ax_err.set_ylabel("Erreur (pts %)")
        ax_err.xaxis.set_major_formatter(mdates.DateFormatter("%m"))
        ax_err.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax_err.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=7)
        ax_err.legend(fontsize=7)
        ax_err.grid(alpha=0.3, color=C_GRID)

    plt.tight_layout()
    plt.savefig(
        "outputs_forecasting_prevision/figures/forecast_defect_rate_par_station.png",
        dpi=150, bbox_inches="tight"
    )
    plt.show()
    print("  ✓ forecast_defect_rate_par_station.png")
    return all_metrics, next_day_preds


def plot_features(df_feat: pd.DataFrame) -> None:
    """Visualise les features pour la première station."""
    sid = sorted(df_feat["station_id"].unique())[0]
    grp = (df_feat[df_feat["station_id"] == sid]
           .copy()
           .sort_values("production_day")
           .set_index("production_day"))

    features = [f for f in ["rolling_mean_7d", "rolling_std_7d",
                             "previous_day_scrap", "defect_trend"]
                if f in grp.columns and grp[f].notna().sum() > 5]

    n    = len(features)
    fig, axes = plt.subplots(n + 1, 1, figsize=(16, 3 * (n + 1)), sharex=True)

    axes[0].plot(grp.index, grp["defect_rate_pct"], color=C_ACTUAL, linewidth=1.3)
    axes[0].set_title(f"Station {sid} — Defect Rate réel (%)", fontsize=10, loc="left")
    axes[0].set_ylabel("Defect Rate")
    axes[0].grid(alpha=0.3, color=C_GRID)

    colors = [C_MA, C_ES, C_FEATURE, C_ERROR]
    labels_map = {
        "rolling_mean_7d":    "Moy. mobile 7j",
        "rolling_std_7d":     "Volatilité (std 7j)",
        "previous_day_scrap": "Rebuts J-1",
        "defect_trend":       "Tendance (écart vs moy. 7j)",
    }

    for ax, feat, color in zip(axes[1:], features, colors):
        ax.plot(grp.index, grp[feat], color=color, linewidth=1.4)
        ax.axhline(grp[feat].mean(), color="gray", linewidth=0.8, linestyle=":", alpha=0.5)
        ax.set_title(labels_map.get(feat, feat), fontsize=10, loc="left")
        ax.set_ylabel(feat.replace("_", " "))
        ax.grid(alpha=0.3, color=C_GRID)

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axes[-1].xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=30, ha="right")

    fig.suptitle(f"Feature Engineering — Station {sid}", fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(
        "outputs_forecasting_prevision/figures/forecast_features_station.png",
        dpi=150, bbox_inches="tight"
    )
    plt.show()
    print("  ✓ forecast_features_station.png")


def plot_metrics_comparison(metrics: list) -> None:
    df_m     = pd.DataFrame(metrics)
    df_m     = df_m[df_m["n"] > 0]
    stations = sorted(df_m["station_id"].unique())
    x, width = np.arange(len(stations)), 0.35

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    for ax, metric, ylabel in [(ax1, "MAE", "MAE (pts %)"), (ax2, "MAPE", "MAPE (%)")]:
        ma_vals = [
            df_m[(df_m["station_id"] == s) & df_m["model"].str.startswith("MA")][metric].values[0]
            if len(df_m[(df_m["station_id"] == s) & df_m["model"].str.startswith("MA")]) > 0 else 0
            for s in stations
        ]
        es_vals = [
            df_m[(df_m["station_id"] == s) & df_m["model"].str.startswith("ES")][metric].values[0]
            if len(df_m[(df_m["station_id"] == s) & df_m["model"].str.startswith("ES")]) > 0 else 0
            for s in stations
        ]

        b1 = ax.bar(x - width/2, ma_vals, width, color=C_MA,  label="MA7",     alpha=0.85)
        b2 = ax.bar(x + width/2, es_vals, width, color=C_ES,  label="ES Holt", alpha=0.85)

        for bar in list(b1) + list(b2):
            if bar.get_height() > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                        f"{bar.get_height():.3f}", ha="center", va="bottom", fontsize=8)

        ax.set_xticks(x)
        ax.set_xticklabels([f"Station {s}" for s in stations], rotation=15, ha="right")
        ax.set_title(f"{ylabel} — MA7 vs ES Holt", fontsize=12, fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.legend()
        ax.grid(axis="y", alpha=0.3, color=C_GRID)

    plt.tight_layout()
    plt.savefig(
        "outputs_forecasting_prevision/figures/forecast_metrics_comparison.png",
        dpi=150, bbox_inches="tight"
    )
    plt.show()
    print("  ✓ forecast_metrics_comparison.png")


def plot_next_day_summary(next_day_preds: list) -> None:
    df_n = pd.DataFrame(next_day_preds)
    n    = len(df_n)

    fig, ax = plt.subplots(figsize=(12, n * 1.5 + 1.5))  
    ax.axis("off")

    col_labels = ["Station", "Date J+1", "Réel J (actuel)",
                  "Prévision MA7 (%)", "Prévision ES (%)", "Écart MA vs Réel"]
    cell_text, cell_colors = [], []

    for _, row in df_n.iterrows():
        ecart     = abs(row["ma_pred"] - row["last_actual"])
        ecart_pct = ecart / row["last_actual"] * 100 if row["last_actual"] > 0 else 0
        cell_text.append([
            f"Station {int(row['station_id'])}",
            str(row["date_J1"]),
            f"{row['last_actual']:.3f}%",
            f"{row['ma_pred']:.3f}%",
            f"{row['es_pred']:.3f}%",
            f"{ecart:.3f} pts ({ecart_pct:.1f}%)",
        ])
        cell_colors.append(
            ["#FFFFFF"] * 5 + ["#FFCCCC" if ecart_pct > 20 else "#CCFFCC"]
        )

    tbl = ax.table(cellText=cell_text, colLabels=col_labels,
                   cellLoc="center", loc="center", cellColours=cell_colors)
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1.2, 2.0)

    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor("#2E75B6")
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")

    ax.set_title(
        f"Prévisions J+1 — Defect Rate  ({df_n['date_J1'].iloc[0]})",
        fontsize=13, fontweight="bold", pad=20
    )
    plt.tight_layout()
    plt.savefig(
        "outputs_forecasting_prevision/figures/forecast_next_day_summary.png",
        dpi=150, bbox_inches="tight"
    )
    plt.show()
    print("  ✓ forecast_next_day_summary.png")


# ══════════════════════════════════════════════════════════════════════════════
# 5. RAPPORT CONSOLE + EXPORT CSV
# ══════════════════════════════════════════════════════════════════════════════

def print_forecast_report(metrics: list, next_day_preds: list) -> None:
    print("\n" + "=" * 65)
    print("   RAPPORT PRÉVISION DEFECT RATE J+1")
    print("=" * 65)

    df_m = pd.DataFrame(metrics)
    if not df_m.empty:
        print("\n  ── Métriques de performance ──")
        for sid in sorted(df_m["station_id"].unique()):
            print(f"\n  Station {sid} :")
            for _, row in df_m[df_m["station_id"] == sid].iterrows():
                print(f"    {row['model']:<12}  MAE={row['MAE']:.4f}  "
                      f"RMSE={row['RMSE']:.4f}  MAPE={row['MAPE']:.2f}%  (n={row['n']})")

    print("\n  ── Prévisions J+1 ──")
    for p in next_day_preds:
        better = "MA7" if abs(p["ma_pred"] - p["last_actual"]) <= \
                          abs(p["es_pred"] - p["last_actual"]) else "ES Holt"
        print(f"  Station {p['station_id']}  [{p['date_J1']}]"
              f"  Réel={p['last_actual']}%"
              f"  MA7={p['ma_pred']}%"
              f"  ES={p['es_pred']}%"
              f"  → meilleur modèle : {better}")

    print("=" * 65 + "\n")


def export_results(df_feat: pd.DataFrame, next_day_preds: list,
                   metrics: list, window: int = 7) -> None:
    parts = []
    for sid in df_feat["station_id"].unique():
        g = (df_feat[df_feat["station_id"] == sid]
             .copy()
             .sort_values("production_day")
             .set_index("production_day"))
        g["ma_forecast"] = forecast_moving_average(g["defect_rate_pct"], window=window)
        g["es_forecast"] = forecast_exponential_smoothing(g["defect_rate_pct"])
        g["ma_error"]    = g["defect_rate_pct"] - g["ma_forecast"]
        g["es_error"]    = g["defect_rate_pct"] - g["es_forecast"]
        parts.append(g.reset_index())

    pd.concat(parts).to_csv(
        "outputs_forecasting_prevision/csv/forecast_defect_rate_full.csv", index=False
    )
    print("  ✓ forecast_defect_rate_full.csv")

    pd.DataFrame(next_day_preds).to_csv(
        "outputs_forecasting_prevision/csv/forecast_next_day.csv", index=False
    )
    print("  ✓ forecast_next_day.csv")

    pd.DataFrame(metrics).to_csv(
        "outputs_forecasting_prevision/csv/forecast_metrics.csv", index=False
    )
    print("  ✓ forecast_metrics.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 6. MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # ── Arguments CLI ────────────────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="Prévision Defect Rate J+1")
    parser.add_argument(
        "--station", type=int, default=None,
        help="Filtrer sur une station_id spécifique (ex: --station 2)"
    )
    parser.add_argument(
        "--window", type=int, default=7,
        help="Fenêtre Moving Average en jours (défaut: 7)"
    )
    args = parser.parse_args()

    # ── Chargement ───────────────────────────────────────────────────────────
    df_raw = load_data(station_id=args.station)

    # ── Feature engineering ──────────────────────────────────────────────────
    print("\n→ Feature engineering...")
    df_feat = build_features(df_raw, window=args.window)

    # ── Graphiques ───────────────────────────────────────────────────────────
    print("\n→ Génération des graphiques...")

    print("  [1/4] Features...")
    plot_features(df_feat)

    print("  [2/4] Prévisions par station...")
    metrics, next_day_preds = plot_forecast_per_station(df_feat, window=args.window)

    print("  [3/4] Comparaison des métriques...")
    plot_metrics_comparison(metrics)

    print("  [4/4] Tableau prévisions J+1...")
    plot_next_day_summary(next_day_preds)

    # ── Rapport + export ─────────────────────────────────────────────────────
    print_forecast_report(metrics, next_day_preds)
    print("→ Export CSV...")
    export_results(df_feat, next_day_preds, metrics, window=args.window)

    print("\n✅ Prévision terminée.\n")