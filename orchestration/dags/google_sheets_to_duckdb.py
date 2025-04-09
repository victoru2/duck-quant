from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import pandas as pd
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pathlib import Path

logger = LoggingMixin().log

BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_PATH = BASE_DIR / "data" / "duck-quant.json"
DUCKDB_PATH = BASE_DIR / "data" / "database.duckdb"

SHEET_VARS = [
    "CRYPTO_INVEST",
]


def save_to_duckdb(data: list[dict], table_name: str):
    df = pd.DataFrame(data)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    logger.info(f"💾 Saved {len(df)} rows to DuckDB table '{table_name}'")


def create_dag(dag_id, var_key):
    @dag(
        dag_id=dag_id,
        schedule_interval="@daily",
        start_date=datetime(2025, 4, 9),
        catchup=False,
        max_active_runs=1,
        tags=["ELT", "google_sheets", "duckdb"],
    )
    def _inner_dag():

        @task()
        def extract_sheet_data():
            try:
                sheet_config = Variable.get(var_key, deserialize_json=True)
                sheet_id = sheet_config["id"]
                sheet_range = sheet_config["range"]

                logger.info(
                    f"⛏️ Extracting data from Sheet var: {var_key},"
                    f" Range: {sheet_range}"
                )

                creds = service_account.Credentials.from_service_account_file(
                    str(CREDENTIALS_PATH),
                    scopes=[
                        "https://www.googleapis.com/auth/spreadsheets.readonly"
                    ]
                )
                service = build("sheets", "v4", credentials=creds)
                result = service.spreadsheets().values().get(
                    spreadsheetId=sheet_id, range=sheet_range
                ).execute()
                values = result.get("values", [])
                if not values:
                    raise Exception("🕳️ Sheet returned no data.")

                df = pd.DataFrame(values[1:], columns=values[0])
                logger.info("✅ Data extracted successfully.")
                table_name = dag_id.replace("-", "_")
                table_name = table_name.replace("sheet_to_duckdb_", "")
                save_to_duckdb(df.to_dict(orient="records"), table_name)

            except Exception as e:
                logger.error(
                    f"❌ [{dag_id}] Error while extracting data: {e}",
                    exc_info=True
                )
                raise

        extract_sheet_data()

    return _inner_dag()


for var_key in SHEET_VARS:
    dag_suffix = var_key.lower()
    dag_id = f"sheet_to_duckdb_{dag_suffix}"
    globals()[dag_id] = create_dag(dag_id, var_key)
