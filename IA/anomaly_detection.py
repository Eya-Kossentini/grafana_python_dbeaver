import requests
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import os

os.makedirs("outputs_anomaly_detection/csv", exist_ok=True)
os.makedirs("outputs_anomaly_detection/figures", exist_ok=True)


# Connexion PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port="5435",
    dbname="postgres",
    user="postgres",
    password="admin123"
)

# **************************** TRS/OEE anormal (oee_pct) ************************** #
#lecture kpi

query = """
SELECT *
FROM oee_kpi
"""

df = pd.read_sql(query, conn)


# Calcul Z-Score Pour détecter les valeurs très éloignées de la moyenne : TRS anormal, defect rate anormal, downtime anormal 
mean_oee = df["oee_pct"].mean()
std_oee = df["oee_pct"].std()

df["z_score"] = (df["oee_pct"] - mean_oee) / std_oee

# Détection anomalies ZSCORE
# Principe : si valeur > moyenne + 3 écarts-types ⇒ anomalie on commence par zscore

df["severity"] = "normal"
df.loc[df["z_score"] < -2, "severity"] = "warning"
df.loc[df["z_score"] < -3, "severity"] = "critical"
anomalies = df[df["severity"] != "normal"]


print("\n===== ANOMALIES OEE =====")
print(anomalies[[
   "production_day",
    "station_name",
    "oee_pct",
    "z_score",
    "severity"
]])

print(df.columns)

# import csv
anomalies.to_csv(
    "outputs_anomaly_detection/csv/zscore_oee_anomalies.csv",
    index=False
)
print("Résultats sauvegardés dans zscore_oee_anomalies.csv")

# graphiquement
fig, axes = plt.subplots(
    nrows=len(df["station_name"].unique()),
    figsize=(15, 4 * len(df["station_name"].unique()))
)

for ax, (station, group) in zip(
    axes, df.groupby("station_name")
):
    anom = group[group["severity"] != "normal"]
    ax.plot(group["production_day"], group["oee_pct"])
    ax.scatter(anom["production_day"], anom["oee_pct"], color="red", s=80)
    ax.set_title(f"OEE — {station}")
    ax.set_ylabel("OEE (%)")

plt.tight_layout()
plt.savefig("outputs_anomaly_detection/figures/zscore_oee_par_station.png", dpi=300, bbox_inches="tight")



#histogramme

oee_station = (
    anomalies["station_name"]
    .value_counts()
)

plt.figure(figsize=(10,5))

oee_station.plot(kind="bar")

plt.title("Number of OEE Anomalies per Station")
plt.xlabel("Station")
plt.ylabel("Anomalies")

plt.tight_layout()
plt.savefig("outputs_anomaly_detection_anomaly_detection/figures/Number of OEE Anomalies per Station.png", dpi=300, bbox_inches="tight")
plt.show()


# **************************** Downtime anormal (downtime) ************************** #
query = """
SELECT *
FROM downtime_by_station_KPI
"""

df = pd.read_sql(query, conn)


# Calcul Z-Score Pour détecter les valeurs très éloignées de la moyenne : TRS anormal, defect rate anormal, downtime anormal 
mean_dt = df["downtime_minutes"].mean()
std_dt = df["downtime_minutes"].std()

df["z_score"] = (df["downtime_minutes"] - mean_dt) / std_dt

# Détection anomalies ZSCORE
# Principe : si valeur > moyenne + 3 écarts-types ⇒ anomalie on commence par zscore

anomalies = df[df["z_score"] > 3]


print("\n===== ANOMALIES DOWNTIME =====")
print(anomalies[
        [
            "production_day",
            "station_id",
            "downtime_type",
            "downtime_minutes",
            "z_score"
        ]
    ])

print(df.columns)

print("\nNombre d'anomalies :", len(anomalies))
print("\nRépartition par type :")
print(anomalies["downtime_type"].value_counts())

#import csv

anomalies.to_csv("outputs_anomaly_detection/csv/zscore_downtime_anomalies.csv", index=False)
print("Résultats sauvegardés dans zscore_downtime_anomalies.csv")

#graphiquement 

anomalies["downtime_type"].value_counts().plot(
    kind="bar"
)
df["production_day"] = pd.to_datetime(df["production_day"])
df = df.sort_values("production_day")

plt.figure(figsize=(14, 6))

plt.plot(df["production_day"],
         df["downtime_minutes"], 
         label="Downtime minutes")

plt.scatter(
    anomalies["production_day"],
    anomalies["downtime_minutes"],
    label="Anomalies Z-Score",
    s=60
)

plt.title("Z-Score Anomaly Detection - Downtime")
plt.xlabel("Production Day")
plt.ylabel("Downtime (minutes)")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("outputs_anomaly_detection/figures/zscore_downtime_anomalies.png", dpi=300, bbox_inches="tight")
plt.show()

#histogramme
plt.figure(figsize=(8,5))

anomalies["downtime_type"].value_counts().plot(
    kind="bar"
)

plt.title("Downtime Anomalies by Type")
plt.xlabel("Downtime Type")
plt.ylabel("Count")

plt.tight_layout()
plt.savefig("outputs_anomaly_detection/figures/Downtime Anomalies by Type.png", dpi=300, bbox_inches="tight")
plt.show()

# **************************** Defect Rate anormal (defect_rate) ************************** #

query = """
SELECT *
FROM defect_rate_kpi
"""

df = pd.read_sql(query, conn)


mean_defect = df["defect_rate_pct"].mean()
std_defect = df["defect_rate_pct"].std()

df["z_score"] = (
    df["defect_rate_pct"] - mean_defect
) / std_defect


df["severity"] = "normal"
df.loc[df["z_score"] > 2, "severity"] = "warning"
df.loc[df["z_score"] > 3, "severity"] = "critical"
anomalies = df[df["severity"] != "normal"]


print("\n===== ANOMALIES DEFECT RATE =====")

print(
    anomalies[
        [
            "station_name",
            "defect_rate_pct",
            "scrap_count",
            "defect_count",
            "z_score",
            "severity"
        ]
    ]
)

print("\nNombre d'anomalies :", len(anomalies))

#import csv

anomalies.to_csv("outputs_anomaly_detection/csv/zscore_defect_rate_anomalies.csv", index=False)
print("Résultats sauvegardés dans zscore_defect_rate_anomalies.csv")

#graphiquement

plt.figure(figsize=(10,5))

(
    anomalies["station_name"]
    .value_counts()
    .head(10)
).plot(kind="bar")


plt.title("Top Stations with Defect Rate Anomalies")
plt.xlabel("Station")
plt.ylabel("Anomalies")

plt.tight_layout()
plt.savefig("outputs_anomaly_detection/figures/Top Stations with Defect Rate Anomalies.png", dpi=300, bbox_inches="tight")
plt.show()
plt.figure(figsize=(14,6))

plt.plot(
    df.index,
    df["defect_rate_pct"],
    label="Defect Rate (%)"
)

plt.scatter(
    anomalies.index,
    anomalies["defect_rate_pct"],
    label="Anomalies Z-Score",
    s=60
)

plt.title("Z-Score Anomaly Detection - Defect Rate")
plt.xlabel("Observations")
plt.ylabel("Defect Rate (%)")
plt.legend()
plt.tight_layout()

plt.savefig("outputs_anomaly_detection/figures/zscore_defect_rate_anomalies.png", dpi=300, bbox_inches="tight")
plt.show()

conn.close()