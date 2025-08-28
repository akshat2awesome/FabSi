import streamlit as st
import pymysql
import os
import pandas as pd
from dotenv import load_dotenv

# --- Page Configuration ---
# Set the page configuration for the Streamlit app.
# This should be the first Streamlit command in your script.
st.set_page_config(
    page_title="Semiconductor Fab Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Database Connection ---
# Use Streamlit's caching to store the database connection object.
# This prevents re-establishing the connection on every user interaction, improving performance.
@st.cache_resource
def get_connection():
    """
    Establishes a connection to the MySQL database using credentials
    from environment variables.
    Returns:
        pymysql.connections.Connection or None: The connection object or None if connection fails.
    """
    # Load environment variables from a .env file.
    load_dotenv()

    try:
        # MySQL connection config dictionary
        config = {
            'user': os.getenv("MYSQL_USER"),
            'password': os.getenv("MYSQL_PASSWORD"),
            'host': os.getenv("MYSQL_HOST"),
            'port': int(os.getenv("MYSQL_PORT", 3306)),
            'db': os.getenv("MYSQL_DB", "defaultdb"),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor, # Use DictCursor for pandas compatibility
            'connect_timeout': 10
        }

        # Print current DB target for debugging (without password)
        print("Connecting to MySQL with config:")
        print({k: v for k, v in config.items() if k != 'password'})

        # Establish connection using the config dictionary
        connection = pymysql.connect(**config)
        
        print("Database connection successful.")
        return connection
    except (pymysql.MySQLError, ValueError) as e:
        # Display an error message in the Streamlit app if the connection fails
        st.error(f"Error connecting to MySQL database: {e}")
        st.error("Please ensure your environment variables (.env file) are correctly configured.")
        return None

# --- Data Fetching ---
def fetch_data(connection, table_name, limit):
    """
    Fetches a specified number of rows from a given table.
    Args:
        connection (pymysql.connections.Connection): The database connection object.
        table_name (str): The name of the table to fetch data from.
        limit (int): The maximum number of rows to return.
    Returns:
        pandas.DataFrame: A DataFrame containing the fetched data, or an empty DataFrame on error.
    """
    if connection is None:
        return pd.DataFrame()  # Return an empty DataFrame if there's no connection

    cursor = None
    try:
        # Ensure the correct database is selected for the query.
        # This is important if the initial connection was to a different DB.
        connection.select_db("Phase1")
        cursor = connection.cursor()
        # Securely format the SQL query with the table name and a parameter for the limit
        query = f"SELECT * FROM `{table_name}` LIMIT %s"
        cursor.execute(query, (limit,))
        data = cursor.fetchall()
        # Convert the list of dictionaries to a pandas DataFrame for easy display
        return pd.DataFrame(data)
    except pymysql.MySQLError as e:
        # Show a warning if a specific table can't be queried
        st.warning(f"Could not fetch data from table '{table_name}': {e}")
        return pd.DataFrame()
    finally:
        # Ensure the cursor is closed after the query
        if cursor:
            cursor.close()

# --- Main Application UI ---
def main():
    """
    The main function that lays out the Streamlit user interface.
    """
    st.title("Semiconductor Fab Database Viewer")
    st.markdown(
        "View live data from the facility's operational database. "
        "Use the controls in the sidebar to adjust the number of records displayed for each table."
    )

    # --- Sidebar Controls ---
    with st.sidebar:
        st.header("Controls")
        # A number input widget to allow users to specify the record limit
        record_limit = st.number_input(
            label="Number of records to display",
            min_value=1,
            max_value=1000,
            value=10, # Default value
            step=5,
            help="Select the number of rows you want to view from each table."
        )

    # --- Main Content ---
    # List of all tables you want to display on the dashboard
    table_names = [
        "Meteorology",
        "FacilityLogs",
        "ProcessMetrics",
        "HumanOps",
        "WaferLotTracking"
    ]

    # Get the database connection
    connection = get_connection()

    if connection:
        # Iterate through the list of table names and display each one
        for table in table_names:
            st.subheader(f"Table: `{table}`")
            df = fetch_data(connection, table, record_limit)

            if not df.empty:
                # Display the DataFrame using Streamlit's built-in function
                st.dataframe(df, use_container_width=True)
            else:
                # Show a message if the table is empty or doesn't exist
                st.info(f"No data to display for table `{table}`. It might be empty or not yet created.")
    else:
        # A prominent error if the initial database connection failed
        st.error("Could not connect to the database. Please check your environment variables and ensure the database server is running.")

# --- Script Entry Point ---
if __name__ == "__main__":
    main()
