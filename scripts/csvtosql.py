import os
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Print current DB target (without password)
print("🔧 Connecting to MySQL with config:")
print({
    'user': os.getenv("MYSQL_USER"),
    'host': os.getenv("MYSQL_HOST"),
    'port': os.getenv("MYSQL_PORT"),
    'db': os.getenv("MYSQL_DB"),
})

# MySQL connection config
config = {
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'host': os.getenv("MYSQL_HOST"),
    'port': int(os.getenv("MYSQL_PORT")),
    'db': os.getenv("MYSQL_DB"),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor
}

# Date folder path (3 days ago)
date_folder = str(datetime.utcnow().date() - timedelta(days=3))
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_folder = os.path.join(script_dir, 'csvs', date_folder)

# Table to CSV mapping
table_csv_map = {
    "Meteorology": "Meteorology.csv",
    "FacilityLogs": "FacilityLogs.csv",
    "ProcessMetrics": "ProcessMetrics.csv",
    "HumanOps": "HumanOps.csv",
    "WaferLotTracking": "WaferLotTracking.csv"
}

def insert_dataframe_to_mysql(cursor, table_name, df):
    # Get MySQL table columns
    cursor.execute(
        "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (config['db'], table_name)
    )
    mysql_columns = [row[0] for row in cursor.fetchall()]

    # Filter DataFrame to valid columns
    df_filtered = df.loc[:, df.columns.intersection(mysql_columns)]

    # Log if any columns are skipped
    dropped = set(df.columns) - set(mysql_columns)
    if dropped:
        print(f"⚠️ Dropping unmatched columns for {table_name}: {dropped}")

    if df_filtered.empty:
        print(f"⚠️ No matching columns to insert for {table_name}. Skipping.")
        return

    # Build insert SQL
    cols = list(df_filtered.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    column_str = ", ".join(cols)
    insert_sql = f"INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})"

    # Clean and insert data
    data = [tuple(None if pd.isna(x) else str(x) for x in row) for row in df_filtered.values]
    cursor.executemany(insert_sql, data)

def main():
    cnx = None
    cursor = None

    try:
        cnx = pymysql.connect(**config)
        cursor = cnx.cursor()
        print("✅ Connected to MySQL")

        for table, csv_file in table_csv_map.items():
            csv_path = os.path.join(csv_folder, csv_file)

            if not os.path.exists(csv_path):
                print(f"⚠️ CSV file for {table} not found: {csv_path}")
                continue

            df = pd.read_csv(csv_path)
            if df.empty:
                print(f"⚠️ {csv_file} is empty. Skipping.")
                continue

            insert_dataframe_to_mysql(cursor, table, df)
            cnx.commit()
            print(f"✅ Inserted {len(df)} rows into {table}")

    except pymysql.MySQLError as err:
        print(f"❌ MySQL Error: {err}")
    except Exception as e:
        print(f"❌ General Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

if __name__ == "__main__":
    main()
