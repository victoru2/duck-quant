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
   - Google Sheets API authentication.
   [(Setup)](https://github.com/victoru2/duck-quant/blob/main/data/README.md#-google-sheets-api-credentials-setup)
   - Data fetch from specified range
2. **Transform**
   - Duplicate header handling (e.g., col_1, col_2)
   - JSON auto-parsing ({"key":value} → dict)
3. **Load**
   - DuckDB table creation/replacement


---

## 🚨 TROUBLESHOOTING

Errors and recommended actions:

- No Data:
    - Fix: Check range syntax and verify the Sheet ID.
    - Prevention: Test the range directly in the Google Sheets UI.

- Permission Denied:
    - Fix: Re-share the Sheet or rotate the credentials.
    - Prevention: Pre-authorize the service account key.

---
**Technical Specs**
- **DuckDB Path**: `/data/database.duckdb`

"""

from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from resources import RESOURCE_LIST
from sensors import GoogleSheetsChangeSensor

from datetime import datetime, timedelta
import pandas as pd
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

# Path configurations
BASE_DIR = Path(__file__).parent
CREDENTIALS_PATH = BASE_DIR / "data" / "duck-quant.json"
DUCKDB_PATH = BASE_DIR / "data" / "database.duckdb"


default_args = {
    "owner": "victor",
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}


def get_sheet_config(var_key: str) -> tuple[str, str]:
    """
    Retrieve and validate a Google Sheet config from an Airflow Variable.

    The variable must contain a JSON object with 'id' and 'range' keys.

    Args:
        var_key (str): Key of the Airflow Variable containing the sheet config.

    Returns:
        tuple[str, str]: A tuple with (spreadsheet_id, sheet_range).

    Raises:
        ValueError: If the content is not valid JSON or lacks required keys.
    """
    raw = Variable.get(var_key, deserialize_json=True)

    # Handle the case where the value is still a JSON string
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse sheet config for {var_key}: {e}")
            raise ValueError(f"Invalid JSON in Variable '{var_key}'")

    if not isinstance(raw, dict) or not all(k in raw for k in ("id", "range")):
        raise ValueError(
            f"Invalid sheet config structure for '{var_key}': must contain "
            "'id' and 'range'"
        )

    return raw["id"], raw["range"]


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
                df[col] = df[col].apply(
                    lambda x: json.loads(x)
                    if pd.notnull(x) and isinstance(x, str)
                    else None
                )
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
        schedule="@hourly",
        start_date=datetime(2025, 4, 9),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        tags=["ELT", "google_sheets", "duckdb"],
        doc_md=__doc__  # Inherits module docstring
    )
    def _inner_dag():
        """Google Sheet to DuckDB ETL Pipeline.

        This DAG extracts data from a configured Google Sheet, handles
        duplicate column headers, processes JSON-formatted columns, and
        loads the data into DuckDB.
        """

        sheet_id, sheet_range = get_sheet_config(var_key)

        wait_for_change = GoogleSheetsChangeSensor(
            task_id=f"wait_for_{var_key}",
            spreadsheet_id=sheet_id,
            sheet_range=sheet_range,
            credentials_path=str(CREDENTIALS_PATH),
            variable_key=f"LAST_HASH_{var_key}",
            poke_interval=60,
            timeout=60 * 60 * 24
        )

        @task(pool="duckdb_pool")
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
            table_name = dag_id.replace("-", "_")
            table_name = table_name.replace("sheet_to_duckdb__", "")

            save_to_duckdb(df, table_name)

        extract_task = extract_sheet_data()

        tag_suffix = var_key
        target_dag_id = f"dbt_by_tag__{tag_suffix}"

        trigger_dbt_dag = TriggerDagRunOperator(
            task_id=f"trigger_dbt_{tag_suffix}",
            trigger_dag_id=target_dag_id,
            wait_for_completion=False,
            reset_dag_run=True,
            conf={"triggered_by": dag_id}
        )

        wait_for_change >> extract_task >> trigger_dbt_dag

    return _inner_dag()


# Dynamically generate DAGs for each sheet configuration
for var_key in RESOURCE_LIST:
    dag_suffix = var_key
    dag_id = f"sheet_to_duckdb__{dag_suffix}"
    globals()[dag_id] = create_dag(dag_id, var_key)
