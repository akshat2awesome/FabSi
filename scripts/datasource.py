import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# CONFIG
LAT = 33.3062  # Chandler, Arizona
LON = -111.8413
STATION_ID = "CHANDLER_AZ_01"



end_date = datetime.utcnow().date() - timedelta(days=3)
start_date = end_date

# Open-Meteo Historical API
url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={LAT}&longitude={LON}"
    f"&start_date={start_date}&end_date={end_date}"
    f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,"
    f"wind_direction_10m,precipitation,pressure_msl&timezone=UTC"
)


# Request data
response = requests.get(url)
data = response.json()

hourly = data.get("hourly", {})
timestamps = hourly.get("time", [])

# Build row-wise dicts
records = []
for i, ts in enumerate(timestamps):
    record = {
        "timestamp": ts,
        "temperature_c": hourly.get("temperature_2m", [None]*len(timestamps))[i],
        "humidity_pct": hourly.get("relative_humidity_2m", [None]*len(timestamps))[i],
        "wind_speed_mps": hourly.get("wind_speed_10m", [None]*len(timestamps))[i],
        "wind_direction_deg": hourly.get("wind_direction_10m", [None]*len(timestamps))[i],
        "rain_mm": hourly.get("precipitation", [None]*len(timestamps))[i],
        "pressure_hpa": hourly.get("pressure_msl", [None]*len(timestamps))[i]
    }
    records.append(record)
# print(records)
# Create DataFrame
metero_df = pd.DataFrame(records)

# Add metadata
metero_df["station_id"] = STATION_ID
metero_df["aqi"] = [random.randint(30, 180) for _ in range(len(metero_df))]
metero_df["event_flag"] = metero_df["aqi"].apply(lambda x: "smog" if x > 120 else "normal")

np.random.seed(42)  # reproducibility

# --- Configurable ---
log_date = (datetime.utcnow().date() - timedelta(days=3))  # Yesterday, or specify (e.g. datetime(2025,7,10).date())

# Schemas and IDs
facility_id = "CHANDLER_FAB1"
tool_ids = [f"TOOL_{str(i).zfill(2)}" for i in range(1, 11)]
cleanroom_classes = ['ISO1', 'ISO5', 'ISO7']
power_status_choices = ['stable', 'dip', 'outage']
hvac_statuses = ['nominal', 'alert', 'fail']
operators = [f"OP_{str(i).zfill(3)}" for i in range(1, 21)]
shifts = ['morning', 'evening', 'night']
action_types = ['recipe_change', 'tool_reset', 'calibration']
wafer_lots = [f"LOT_{str(i).zfill(4)}" for i in range(1, 21)]
wafer_ids = [f"WAFER_{str(lot)}_{w}" for lot in wafer_lots for w in range(1, 26)]
process_steps = ['Coat', 'Litho', 'Etch', 'Deposition', 'CMP', 'Inspection']

# Create a 24-hour hourly timestamp index for log_date
hourly_range = pd.date_range(start=datetime.combine(log_date, datetime.min.time()),
                             periods=96, freq='15min')

# 1. FacilityLogs: one log per hour
fac_logs = []
for timestamp in hourly_range:
    entry = {
        "facility_id": facility_id,
        "timestamp": timestamp,
        "cleanroom_class": np.random.choice(cleanroom_classes, p=[0.1, 0.7, 0.2]),
        "power_status": np.random.choice(power_status_choices, p=[0.9, 0.07, 0.03]),
        "water_quality_ppb": np.round(np.random.uniform(5, 20), 2),
        "gas_pressure_kpa": np.round(np.random.uniform(380, 420), 1),
        "tool_id": np.random.choice(tool_ids + [None]),
        "hvac_status": np.random.choice(hvac_statuses, p=[0.85, 0.13, 0.02]),
    }
    fac_logs.append(entry)
FacilityLogs = pd.DataFrame(fac_logs)

# 2. ProcessMetrics: unchanged (static, unrelated to log window)


pm_rows = []
for wafer_id in np.random.choice(wafer_ids, size=100, replace=False):  # reduce for demo
    for step in process_steps:
        cd_target_nm = 5.0  # fixed for 5nm node
        cd_variation_nm = np.abs(np.random.normal(loc=0.45, scale=0.15))  # deviation from target
        cd_actual_nm = cd_target_nm + cd_variation_nm

        # Yield drops only if variation exceeds 0.6nm
        cd_penalty = max(0, (cd_variation_nm - 0.6) * 100)
        yield_pct = np.round(np.clip(99 - cd_penalty, 85, 99), 2)

        row = {
            "process_id": f"{wafer_id}_{step}",
            "wafer_id": wafer_id,
            "step_name": step,
            "tool_id": np.random.choice(tool_ids),
            "cd_target_nm": cd_target_nm,
            "cd_variation_nm": np.round(cd_variation_nm, 3),
            "cd_actual_nm": np.round(cd_actual_nm, 3),
            "etch_depth_nm": np.round(np.random.normal(loc=60, scale=2), 2),
            "defect_density_cm2": np.round(np.abs(np.random.normal(loc=0.06, scale=0.03)), 4),
            "yield_pct": yield_pct
        }
        pm_rows.append(row)

ProcessMetrics = pd.DataFrame(pm_rows)




# 3. HumanOps: 1–3 logs per hour
hm_rows = []
for timestamp in hourly_range:
    hour = timestamp.hour
    shift = (
        'morning' if 6 <= hour < 14 else
        'evening' if 14 <= hour < 22 else
        'night'
    )
    actions_per_hour = np.random.randint(1, 4)
    for i in range(actions_per_hour):
        op_id = np.random.choice(operators)
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        time = timestamp.replace(minute=minute, second=second)
        row = {
            "log_id": f"LOG_{op_id}_{time.strftime('%Y%m%d%H%M%S')}_{i}",
            "operator_id": op_id,
            "timestamp": time,
            "action_type": np.random.choice(action_types),
            "error_flag": np.random.rand() < 0.04,
            "shift": shift
        }
        hm_rows.append(row)
HumanOps = pd.DataFrame(hm_rows)

# 4. WaferLotTracking: full lot coverage
wlt_rows = []
for lot in wafer_lots:
    lot_start = datetime.combine(log_date, datetime.min.time()) + timedelta(hours=np.random.randint(0, 16))
    lot_size = np.random.randint(15, 25)
    duration = np.random.uniform(8, 20)
    anomaly = np.random.choice(['none', 'yield_drop', 'delay', 'contamination'], p=[0.87, 0.05, 0.06, 0.02])
    row = {
        'lot_id': lot,
        'wafer_count': lot_size,
        'start_time': lot_start,
        'end_time': lot_start + timedelta(hours=duration),
        'avg_process_time_hr': np.round(duration / lot_size, 2),
        'anomaly': anomaly,
        'final_status': 'accepted' if anomaly == 'none' else 'review'
    }
    wlt_rows.append(row)
WaferLotTracking = pd.DataFrame(wlt_rows)

# Concatenate all DataFrames into one
combined_df = pd.concat([metero_df, FacilityLogs, ProcessMetrics, HumanOps, WaferLotTracking], ignore_index=True)

# Get the directory of the current script file
script_dir = os.path.dirname(os.path.abspath(__file__))

# Base 'csvs' folder path
csvs_dir = os.path.join(script_dir, 'csvs')

# Folder name based on date (3 days ago)
folder_name = str(datetime.utcnow().date() - timedelta(days=3))

# Full path for the dated folder
dated_folder_path = os.path.join(csvs_dir, folder_name)

# Create the folder if it doesn't exist
os.makedirs(dated_folder_path, exist_ok=True)

# Filename
filename = f"phase1_{folder_name}.csv"

# Full file path inside the dated folder
file_path = os.path.join(dated_folder_path, filename)

# Save the CSV
combined_df.to_csv(file_path, index=False)

print(f"✅ Saved combined CSV to: {file_path}")


