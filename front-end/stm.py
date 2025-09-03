import streamlit as st
import pymysql
import pandas as pd
import os
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# --- Page Configuration ---
st.set_page_config(
    page_title="Semiconductor Fab Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Database Connection ---
def get_connection():
    try:
        timeout = 10
        connection = pymysql.connect(
            charset="utf8mb4",
            connect_timeout=timeout,
            cursorclass=pymysql.cursors.DictCursor,
            db=os.getenv("MYSQL_DB", "defaultdb"),
            host=os.getenv("MYSQL_HOST", "localhost"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER", ""),
            read_timeout=timeout,
            write_timeout=timeout,
            autocommit=True
        )
        print("✅ Database connection successful.")
        return connection
    except (pymysql.MySQLError, ValueError) as e:
        st.error(f"❌ Error connecting to MySQL database: {e}")
        return None

# --- Data Fetching ---
def fetch_data(connection, table_name, limit):
    if connection is None:
        return pd.DataFrame()

    cursor = None
    try:
        # Switch to Phase1 schema
        connection.select_db("Phase1")
        cursor = connection.cursor()
        query = f"SELECT * FROM `{table_name}` LIMIT %s"
        cursor.execute(query, (limit,))
        data = cursor.fetchall()
        return pd.DataFrame(data)
    except pymysql.MySQLError as e:
        st.warning(f"⚠️ Could not fetch data from `{table_name}`: {e}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()

# --- Streamlit App Layout ---
def main():
    st.title("Semiconductor Fab Database Viewer")
    st.markdown(
        "View live data from the facility's operational database. "
        "Use the controls in the sidebar to adjust the number of records displayed."
    )

    with st.sidebar:
        st.header("Controls")
        record_limit = st.number_input(
            label="Number of records to display",
            min_value=1,
            max_value=1000,
            value=10,
            step=5,
        )

    table_names = [
        "Meteorology",
        "FacilityLogs",
        "ProcessMetrics",
        "HumanOps",
        "WaferLotTracking"
    ]

    connection = get_connection()

    if connection:
        for table in table_names:
            st.subheader(f"Table: `{table}`")
            df = fetch_data(connection, table, record_limit)
            if not df.empty:
                st.dataframe(df, use_container_width=True)
            else:
                st.info(f"No data to display for `{table}`.")
    else:
        st.error("Could not connect to the database. Check environment variables or connectivity.")

if __name__ == "__main__":
    main()
