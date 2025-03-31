# extract/gsheets_to_duckdb.py
import dlt
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pathlib import Path

credentials_path = Path(__file__).parent.parent.parent / "duck-quant.json"


@dlt.resource
def load_gsheets_data(sheet_id: str, range_name: str):
    # Carga credenciales desde archivo
    creds = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )

    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=range_name
    ).execute()

    yield result.get("values", [])


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="gsheets_pipeline",
        destination="duckdb",
        dataset_name="raw_data"
    )
    data = load_gsheets_data(
        sheet_id="123abc-123456abcd",
        range_name="range!A:O"
    )
    pipeline.run(data)
