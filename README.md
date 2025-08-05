FabSi Data Pipeline - Project Overview
--------------------------------------

This project automates the collection, processing, and storage of semiconductor fab data.

📁 Project Structure:
- csvs/: Contains daily generated CSV files (5 categories).
- scripts/: Python scripts to generate data, split CSVs, and load to SQL.
- .github/workflows/: GitHub Actions file to automate the pipeline.
- db_create.sql: SQL file to define the schema in Neon PostgreSQL.

🔁 Workflow:
1. A script generates synthetic or collected data.
2. The raw data is split into 5 category-specific CSVs.
3. These CSVs are uploaded to a cloud MySQL database.
4. The process is automated daily using GitHub Actions.

🧪 Technologies Used:
- Python (pandas, SQLAlchemy)
- GitHub Actions for automation
- Neon PostgreSQL for cloud database hosting

🎯 Goal:
Create a reproducible and scalable data pipeline for fab monitoring.

📌 Future Add-ons:
- Streamlit dashboard for visualization
- Live data ingestion
