from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from pathlib import Path
import logging
import sys

sys.path.append(str(Path(__file__).parent.parent))


@dag(
    dag_id="gsheets_to_duckdb_elt",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    tags=["gsheets", "duckdb"],
)
def gsheets_to_duckdb_pipeline():

    @task
    def run_elt():
        """Load data from Google Sheets to DuckDB"""
        try:
            # 1. Load config from Airflow environment variable
            config = Variable.get("SHEET_CONFIG", deserialize_json=True)
            sheet_id = config.get("id")
            range_name = config.get("range")

            # 2. Import and execute extraction
            from extract.gsheets import load_gsheets_data, pipeline
            data = load_gsheets_data(
                sheet_id=sheet_id,
                range_name=range_name
            )
            pipeline.run(data)
            logging.info("✅ Data successfully loaded to DuckDB")

        except Exception as e:
            logging.error(f"❌ ELT pipeline error: {str(e)}")
            raise

    run_elt()


dag = gsheets_to_duckdb_pipeline()
