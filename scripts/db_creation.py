import pymysql
from dotenv import load_dotenv
import os

# Load .env from parent folder relative to this script
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

sql_commands = """
DROP DATABASE IF EXISTS Phase1;
CREATE DATABASE Phase1;
USE Phase1;

DROP TABLE IF EXISTS Meteorology;
CREATE TABLE Meteorology (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp VARCHAR(255),
    temperature_c VARCHAR(255),
    humidity_pct VARCHAR(255),
    wind_speed_mps VARCHAR(255),
    wind_direction_deg VARCHAR(255),
    rain_mm VARCHAR(255),
    pressure_hpa VARCHAR(255),
    station_id VARCHAR(255),
    aqi VARCHAR(255),
    event_flag VARCHAR(255)
);

DROP TABLE IF EXISTS FacilityLogs;
CREATE TABLE FacilityLogs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    facility_id VARCHAR(255),
    timestamp VARCHAR(255),
    cleanroom_class VARCHAR(255),
    power_status VARCHAR(255),
    water_quality_ppb VARCHAR(255),
    gas_pressure_kpa VARCHAR(255),
    tool_id VARCHAR(255),
    hvac_status VARCHAR(255)
);

DROP TABLE IF EXISTS ProcessMetrics;
CREATE TABLE ProcessMetrics (
    process_id VARCHAR(255) PRIMARY KEY,
    wafer_id VARCHAR(255),
    step_name VARCHAR(255),
    tool_id VARCHAR(255),
    cd_target_nm VARCHAR(255),
    cd_variation_nm VARCHAR(255),
    cd_actual_nm VARCHAR(255),
    etch_depth_nm VARCHAR(255),
    defect_density_cm2 VARCHAR(255),
    yield_pct VARCHAR(255)
);

DROP TABLE IF EXISTS HumanOps;
CREATE TABLE HumanOps (
    log_id VARCHAR(255) PRIMARY KEY,
    operator_id VARCHAR(255),
    timestamp VARCHAR(255),
    action_type VARCHAR(255),
    error_flag VARCHAR(255),
    shift VARCHAR(255)
);

DROP TABLE IF EXISTS WaferLotTracking;
CREATE TABLE WaferLotTracking (
    lot_id VARCHAR(255) PRIMARY KEY,
    wafer_count VARCHAR(255),
    start_time VARCHAR(255),
    end_time VARCHAR(255),
    avg_process_time_hr VARCHAR(255),
    anomaly VARCHAR(255),
    final_status VARCHAR(255)
);
"""

def setup_database():
    timeout = 10
    connection = pymysql.connect(
        charset="utf8mb4",
        connect_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor,
        db=os.getenv("MYSQL_DB", "defaultdb"),
        host=os.getenv("MYSQL_HOST", "localhost"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        read_timeout=timeout,
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", ""),
        write_timeout=timeout,
        autocommit=True
    )
    cursor = None
    try:
        cursor = connection.cursor()
        statements = [s.strip() for s in sql_commands.split(';') if s.strip()]
        for statement in statements:
            cursor.execute(statement)
        print("✅ Database and tables created successfully.")
    except pymysql.MySQLError as err:
        print(f"❌ MySQL error: {err}")
    finally:
        if cursor:
            cursor.close()
        connection.close()

if __name__ == "__main__":
    setup_database()