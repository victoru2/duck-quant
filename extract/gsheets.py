import dlt
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pathlib import Path

# Credentials configuration
DEFAULT_CREDENTIALS_PATH = Path(__file__).parent / "duck-quant.json"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/database.duckdb")


@dlt.resource
def load_gsheets_data(sheet_id: str, range_name: str):
    """
    Extracts data from Google Sheets.

    Args:
        sheet_id (str): The ID of the Google Sheet to extract data from
        range_name (str): The range of cells to extract (e.g., 'range!A1:Z100')

    Yields:
        list: A list of lists containing the sheet data, where each sublist 
        represents a row

    Raises:
        Exception: If there are issues with Google API authentication or data 
        retrieval
    """
    creds = service_account.Credentials.from_service_account_file(
        str(DEFAULT_CREDENTIALS_PATH),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=range_name
    ).execute()
    values = result.get("values", [])

    yield values


# Global pipeline configuration
pipeline = dlt.pipeline(
    pipeline_name="gsheets_database",
    destination=dlt.destinations.duckdb(DUCKDB_PATH),
    dataset_name="raw_data"
)

# Prueba local
if __name__ == "__main__":
    """
    Local execution entry point for testing the Google Sheets data loader.

    Usage:
        python script_name.py '{"id":"your_sheet_id","range":"your_range"}'

    The script expects a JSON string argument with 'id' and 'range' parameters.
    """
    import json
    import sys

    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
            sheet_id = config["id"]
            range_name = config["range"]
        except Exception as e:
            raise ValueError(f"Error reading config from CLI: {e}")

        data = load_gsheets_data(sheet_id, range_name)
        pipeline.run(data)
