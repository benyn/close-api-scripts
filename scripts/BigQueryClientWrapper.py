from typing import Any
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account


class BigQueryClientWrapper:
    def __init__(self, credentials_path, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = self._create_bigquery_client(credentials_path, project_id)

    def _create_bigquery_client(self, credentials_path, project_id):
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        return bigquery.Client(credentials=credentials, project=project_id)

    def get_table_id(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def get_max_unix_time(self, table_name: str):
        table_id = self.get_table_id(table_name)
        query = f"SELECT MAX(unix_time) as max_unix_time FROM `{table_id}`"
        try:
            rows = self.client.query_and_wait(query)  # Make an API request.
            return next((row.max_unix_time for row in rows), None)
        except NotFound:
            return None
        except Exception as e:
            print(f"An error occurred during querying: {e}")
            return None

    def create_table_if_not_exists(self, table_name: str, schema_path: str):
        table_id = self.get_table_id(table_name)

        try:
            self.client.get_table(table_id)
            print(f"Table {table_id} already exists.")
        except NotFound:
            schema = self.client.schema_from_json(schema_path)
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            print(f"Created table {table_id}")

    def load_json_data(
        self, table_name: str, schema_path: str, data: list[dict[str, Any]]
    ):
        if not isinstance(data, list) or not all(
            isinstance(item, dict) for item in data
        ):
            raise ValueError("Data must be a list of dictionaries")

        table_id = self.get_table_id(table_name)
        schema = self.client.schema_from_json(schema_path)
        job_config = bigquery.LoadJobConfig(
            schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        load_job = self.client.load_table_from_json(
            data, table_id, job_config=job_config
        )

        try:
            # Wait for the job to complete
            load_job.result()

        except BadRequest as e:
            print(e)
            if load_job.errors:
                print(f"{len(load_job.errors)} errors")
                for err in load_job.errors:
                    print(err)
