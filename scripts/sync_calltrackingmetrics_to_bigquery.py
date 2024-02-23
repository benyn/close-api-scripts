import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import sys
from BigQueryClientWrapper import BigQueryClientWrapper
from CallTrackingMetricsAPIClient import CallTrackingMetricsAPIClient
from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Sync CallTrackingMetrics records to BigQuery"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument(
    "--bigquery-credentials-path",
    "-c",
    required=True,
    help="Path to BigQuery credentials",
)
arg_parser.add_argument(
    "--project-id", "-p", required=True, help="Google Cloud project ID"
)
arg_parser.add_argument("--dataset-id", "-d", required=True, help="BigQuery dataset ID")
arg_parser.add_argument("--table-name", "-t", required=True, help="BigQuery table name")
arg_parser.add_argument("--schema-path", "-s", help="Path to BigQuery schema file")
arg_parser.add_argument("--data-path", "-f", help="Path to CTM data file")
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()


if args.schema_path:
    if not os.path.exists(args.schema_path):
        print(f"The schema file {args.schema_path} does not exist.")
        sys.exit(0)

if args.data_path:
    if not os.path.exists(args.data_path):
        print(f"The data file {args.data_path} does not exist.")
        sys.exit(0)


auth_token = get_api_key("api.calltrackingmetrics.com", args.env)
debug = args.env == "dev"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


async def get_ctm_activities(start_date: str | None = None):
    ctm = CallTrackingMetricsAPIClient(auth_token, debug=debug)
    params = {"start_date": start_date} if start_date else None

    try:
        calls = await ctm.get_calls(params=params)
        calls.reverse()

        if calls:
            if args.verbose:
                logging.info(f"Fetched {len(calls)} calls")
            file_path = f"output/calltrackingmetrics-calls-{args.env}.json"
            with open(file_path, "w") as f:
                json.dump(calls, f)
            # return file_path
            return calls
        else:
            logging.info("No calls were fetched.")
            return None

    finally:
        await ctm.close()


def read_json_file(path: str):
    with open(path, "r") as f:
        return json.load(f)


def unix_time_to_date(unix_time):
    dt = datetime.utcfromtimestamp(unix_time)
    dt_minus_one_day = dt - timedelta(days=1)
    return dt_minus_one_day.strftime("%Y-%m-%d")


def convert_datetime_to_iso8601(date_string: str) -> str:
    dt = datetime.strptime(date_string, "%Y-%m-%d %I:%M %p %z")
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat()


async def main():
    bigquery = BigQueryClientWrapper(
        args.bigquery_credentials_path, args.project_id, args.dataset_id
    )

    if args.data_path:
        activities = read_json_file(args.data_path)
        if not activities:
            logging.info(f"No data found in {args.data_path}")
            return
    else:
        max_unix_time = bigquery.get_max_unix_time(args.table_name)
        start_date = unix_time_to_date(max_unix_time) if max_unix_time else None
        fetched_activities = await get_ctm_activities(start_date)
        if not fetched_activities:
            logging.info(f"No new calls since {start_date}")
            return

        if max_unix_time:
            activities = [
                item for item in fetched_activities if item["unix_time"] > max_unix_time
            ]
        else:
            activities = fetched_activities
        print(f"{len(fetched_activities)} fetched, {len(activities)} filtered")

    for activity in activities:
        if "called_at" in activity:
            activity["called_at"] = convert_datetime_to_iso8601(activity["called_at"])

    bigquery.create_table_if_not_exists(args.table_name, args.schema_path)
    bigquery.load_json_data(args.table_name, args.schema_path, activities)
    logging.info(f"Loaded {len(activities)} CallTrackingMetrics activities")


if __name__ == "__main__":
    asyncio.run(main())
