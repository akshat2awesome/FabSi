import pandas as pd
import os
from datetime import datetime, timedelta

script_dir = os.path.dirname(os.path.abspath(__file__))
csvs_dir = os.path.join(script_dir, 'csvs')

# Folder name is date 3 days ago
folder_name = str(datetime.utcnow().date() - timedelta(days=3))

# Full path to that dated folder
dated_folder_path = os.path.join(csvs_dir, folder_name)

# Combined CSV filename inside dated folder
combined_csv_name = f"phase1_{folder_name}.csv"
csv_path = os.path.join(dated_folder_path, combined_csv_name)

# Read the combined CSV
df = pd.read_csv(csv_path)

# Output prefix for saving split CSVs (same dated folder)
output_prefix = dated_folder_path + os.sep

schemas = {
    "Meteorology": {
        "required_cols": {
            "timestamp",
            "temperature_c",
            "humidity_pct",
            "wind_speed_mps",
            "wind_direction_deg",
            "rain_mm",
            "pressure_hpa",
            "station_id",
            "aqi",
            "event_flag"
        },
        "extra_cols": set()  # no extras for Meteorology
    },
    "FacilityLogs": {
        "required_cols": {
            "facility_id",
            "timestamp",
            "cleanroom_class",
            "power_status",
            "water_quality_ppb",
            "gas_pressure_kpa",
            "tool_id",
            "hvac_status"
        },
        "extra_cols": set()  # all columns accounted for in required_cols
    },
    "ProcessMetrics": {
        "required_cols": {
            "process_id",
            "wafer_id",
            "step_name",
            "tool_id",
            "cd_target_nm",
            "cd_variation_nm",
            "cd_actual_nm",
            "etch_depth_nm",
            "defect_density_cm2",
            "yield_pct"
        },
        "extra_cols": set()  # all columns accounted for
    },
    "HumanOps": {
        "required_cols": {
            "log_id",
            "operator_id",
            "timestamp",
            "action_type",
            "error_flag",
            "shift"
        },
        "extra_cols": set()
    },
    "WaferLotTracking": {
        "required_cols": {
            "lot_id",
            "wafer_count",
            "start_time",
            "end_time",
            "avg_process_time_hr",
            "anomaly",
            "final_status"
        },
        "extra_cols": set()
    }
}


for table_name, meta in schemas.items():
    required_cols = meta["required_cols"]
    extra_cols = meta.get("extra_cols", set())
    
    if required_cols.issubset(df.columns):
        filtered_df = df.dropna(subset=list(required_cols))
        
        cols_to_keep = set(filtered_df.columns).intersection(required_cols.union({"timestamp"}))
        cols_to_keep = cols_to_keep.union(extra_cols).intersection(filtered_df.columns)
        
        filtered_df = filtered_df[list(cols_to_keep)]
        
        out_path = os.path.join(output_prefix, f"{table_name}.csv")
        filtered_df.to_csv(out_path, index=False)
        
        print(f"✅ Extracted {len(filtered_df)} rows for {table_name}, saved to {out_path}")
    else:
        missing = required_cols.difference(df.columns)
        print(f"⚠️ Skipping {table_name}: missing required columns: {missing}")
