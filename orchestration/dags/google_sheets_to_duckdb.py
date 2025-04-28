"""
# 📊 Google Sheets → DuckDB ELT Pipeline

## 📌 Overview  
Dynamically generated DAGs that:  
- Extract data from Google Sheets  
- Transform headers/JSON fields  
- Load into DuckDB tables  

## ⚙️ Configuration  
**Airflow Variables Required** (set as JSON):  

```json
{
    "CRYPTO_INVEST": {
        "id": "YOUR_SHEET_ID_1",
        "range": "invest_range!A1:Z1000"
    },
    "EXPENSE": {
        "id": "YOUR_SHEET_ID_2",
        "range": "expense_range!A:D"
    }
}
```

## 🔄 Data Flow  
1. **Extract**  
   - Google Sheets API authentication  
   - Data fetch from specified range  
2. **Transform**  
   - Duplicate header handling (e.g., `col_1`, `col_2`)  
   - JSON auto-parsing (`{"key":value}` → dict)  
3. **Load**  
   - DuckDB table creation/replacement  


---

## 🚨 TROUBLESHOOTING

```
| Error                | Fix Steps                              | Prevention                          |
|----------------------|----------------------------------------|-------------------------------------|
| `No Data`            | Check range syntax - Verify Sheet ID   | ✅ Test range in Sheets UI          |
| `Permission Denied`  | Re-share Sheet - Rotate credentials    | 🔑 Pre-authorize service account    |
```
   
---
**Technical Specs** 
- **DuckDB Path**: `/data/database.duckdb`

"""

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import pandas as pd
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pathlib import Path
import json

logger = LoggingMixin().log

# Path configurations
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_PATH = BASE_DIR / "data" / "duck-quant.json"
DUCKDB_PATH = BASE_DIR / "data" / "database.duckdb"

# List of Airflow Variable keys containing Google Sheets configurations
SHEET_VARS = [
    "CRYPTO_INVEST",
    "EXPENSE",
]


def save_to_duckdb(df: pd.DataFrame, table_name: str) -> None:
    """Save a DataFrame to a DuckDB table with JSON column handling.

    Args:
        df: Pandas DataFrame containing the data to be saved
        table_name: Name of the target table in DuckDB

    Note:
        Automatically detects and converts JSON strings to structured data.
        Replaces the table if it already exists.
    """
    con = duckdb.connect(str(DUCKDB_PATH))

    # Handle potential JSON columns
    for col in df.columns:
        if df[col].astype(str).str.startswith('{').any():
            try:
                df[col] = df[col].apply(lambda x: json.loads(x) if pd.notnull(x) and isinstance(x, str) else None)
            except (ValueError, TypeError):
                pass

    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

    logger.info(f"💾 Saved {len(df)} rows to DuckDB table '{table_name}'")
    con.close()


def create_dag(dag_id: str, var_key: str):
    """Factory function to create a Sheet-to-DuckDB ETL DAG.

    Args:
        dag_id: Unique identifier for the DAG
        var_key: Airflow Variable key containing sheet configuration

    Returns:
        A dynamically generated Airflow DAG

    Example:
        The generated DAG will:
        1. Extract data from configured Google Sheet
        2. Transform headers and JSON data
        3. Load results into DuckDB
    """
    @dag(
        dag_id=dag_id,
        schedule_interval="@daily",
        start_date=datetime(2025, 4, 9),
        catchup=False,
        max_active_runs=1,
        tags=["ELT", "google_sheets", "duckdb"], 
        doc_md=__doc__  # Inherits module docstring
    )
    def _inner_dag():
        """Google Sheet to DuckDB ETL Pipeline.
        
        This DAG extracts data from a configured Google Sheet, handles duplicate
        column headers, processes JSON-formatted columns, and loads the data
        into DuckDB.
        """

        @task()
        def extract_sheet_data():
            """Extract data from Google Sheets and load into DuckDB.
            
            Performs the following operations:
            1. Retrieves sheet configuration from Airflow Variables
            2. Authenticates with Google Sheets API
            3. Extracts data from specified range
            4. Processes headers to ensure uniqueness
            5. Saves data to DuckDB
            
            Raises:
                Exception: If sheet returns no data or extraction fails
            """
            try:
                sheet_config = Variable.get(var_key, deserialize_json=True)
                sheet_id = sheet_config["id"]
                sheet_range = sheet_config["range"]

                logger.info(
                    f"⛏️ Extracting data from Sheet var: {var_key},"
                    f" Range: {sheet_range}"
                )

                # Google Sheets API setup
                creds = service_account.Credentials.from_service_account_file(
                    str(CREDENTIALS_PATH),
                    scopes=[
                        "https://www.googleapis.com/auth/spreadsheets.readonly"
                    ]
                )
                service = build("sheets", "v4", credentials=creds)
                
                # API request
                result = service.spreadsheets().values().get(
                    spreadsheetId=sheet_id, range=sheet_range
                ).execute()
                values = result.get("values", [])
                
                if not values:
                    raise Exception("🕳️ Sheet returned no data.")

                # Process headers to handle duplicates
                headers = values[0]
                data = values[1:]
                
                headers = [
                    f"col_{i}" if headers[:i].count(col) > 0 else col
                    for i, col in enumerate(headers)
                ]
                
                df = pd.DataFrame(data, columns=headers)

                logger.info("✅ Data extracted successfully.")
                table_name = dag_id.replace("-", "_").replace("sheet_to_duckdb_", "")
                
                save_to_duckdb(df, table_name)

            except Exception as e:
                logger.error(
                    f"❌ [{dag_id}] Error while extracting data: {e}",
                    exc_info=True
                )
                raise

        extract_sheet_data()

    return _inner_dag()


# Dynamically generate DAGs for each sheet configuration
for var_key in SHEET_VARS:
    dag_suffix = var_key.lower()
    dag_id = f"sheet_to_duckdb_{dag_suffix}"
    globals()[dag_id] = create_dag(dag_id, var_key)