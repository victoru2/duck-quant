import hashlib
import logging

from airflow.sensors.base import BaseSensorOperator
from airflow.sdk import Variable
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class GoogleSheetsChangeSensor(BaseSensorOperator):
    """
    Sensor that waits until the content of a specified range in a Google Sheets
    document changes. It authenticates using a service account and compares
    the MD5 hash of the current sheet content with the previously stored value
    in Airflow Variables.

    :param task_id: Task identifier
    :param spreadsheet_id: ID of the target Google Spreadsheet
    :param sheet_range: Range to monitor (e.g. "Sheet1!A1:D10")
    :param credentials_path: Path to the service account credentials JSON file
    :param variable_key: Optional key for storing the hash in Airflow Variable
    :param poke_interval: Interval between checks in seconds
    :param timeout: Time in seconds before giving up
    :param mode: Sensor mode ("poke" or "reschedule")
    """

    def __init__(
        self,
        *,
        task_id: str,
        spreadsheet_id: str,
        sheet_range: str,
        credentials_path: str,
        variable_key: str = None,
        poke_interval: int = 300,
        timeout: int = 60 * 60 * 24,
        mode: str = 'poke',
        **kwargs
    ):
        super().__init__(
            task_id=task_id,
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            **kwargs
        )
        self.spreadsheet_id = spreadsheet_id
        self.sheet_range = sheet_range
        self.credentials_path = credentials_path
        self.variable_key = variable_key or (
            f"last_hash_{spreadsheet_id}_{sheet_range.replace(':', '_')}"
        )
        self._service = None

    def poke(self, context):
        """
        Checks whether the sheet content has changed since the last check.

        :param context: Airflow context dictionary
        :return: True if sheet content has changed, False otherwise
        """
        # Initialize Sheets API service if not already done
        if not self._service:
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets.readonly"
                ]
            )
            self._service = build("sheets", "v4", credentials=creds)

        # Fetch values from the sheet
        result = self._service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.sheet_range
        ).execute()
        values = result.get("values", [])

        # Flatten values and calculate MD5 hash
        flat = "".join([",".join(row) for row in values])
        current_hash = hashlib.md5(flat.encode("utf-8")).hexdigest()

        # Compare with previous hash stored in Variable
        previous_hash = Variable.get(self.variable_key, default=None)
        if previous_hash != current_hash:
            Variable.set(self.variable_key, current_hash)
            logger.info(
                f"[{self.task_id}] Change detected in sheet, "
                f"new hash: {current_hash}"
            )
            return True

        return False
