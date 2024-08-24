import argparse
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from CloseApiWrapper import CloseApiWrapper
from utils.csv import write_csv
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(description="Check Healthie appointment sync status")
parser.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
    required=True,
)
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

healthie_api_key = get_api_key("api.gethealthie.com", args.env)
close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)

healthie_user_id_custom_field_id = close.get_custom_field_id("lead", "healthie_user_id")
procedure_custom_activity_type_id = close.get_custom_activity_type_id("Procedure")
if not procedure_custom_activity_type_id:
    logging.error("Custom activity type ID not found.")


async def fetch_graphql(url, query, variables=None, headers=None):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        response = await loop.run_in_executor(
            pool,
            lambda: requests.post(
                url, json={"query": query, "variables": variables}, headers=headers
            ),
        )
        return response


async def fetch_appointments(appointment_type_id: int):
    # GraphQL query
    query = """
    query getAppointments($appointment_type_id: ID) {
        appointments(
            filter: "all"
            filter_by_appointment_type_id: $appointment_type_id
            is_org: true
            should_paginate: false
        ) {
            id
            date
            created_at
            attendees {
                id
                name
            }
            pm_status
        }
    }
    """

    variables = {"appointment_type_id": appointment_type_id}

    # API endpoint
    url = "https://api.gethealthie.com/graphql"

    # Headers
    headers = {
        "Authorization": f"Basic {healthie_api_key}",
        "AuthorizationSource": "API",
    }

    # Make the API call
    response = await fetch_graphql(url, query, variables, headers)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        if "errors" in data:
            print("Errors:", data["errors"])

        elif "data" in data:
            appointments = data.get("data", {}).get("appointments", [])
            print(f"Retrieved {len(appointments)} appointments.")
            return appointments

    else:
        print(f"Error: {response.status_code}")
        print(response.text)


def get_healthie_user_id_search_query(user_ids):
    return {
        "type": "or",
        "queries": [
            {
                "type": "field_condition",
                "field": {
                    "type": "custom_field",
                    "custom_field_id": healthie_user_id_custom_field_id,
                },
                "condition": {"type": "text", "mode": "exact_value", "value": user_id},
            }
            for user_id in user_ids
        ],
    }


def get_attendee_to_lead_mapping(appointments):
    # Extract attendee IDs from appointments
    attendee_ids = set()
    for appointment in appointments:
        for attendee in appointment.get("attendees", []):
            attendee_ids.add(attendee["id"])

    # Fetch leads associated with the attendee_ids
    query = get_healthie_user_id_search_query(list(attendee_ids))
    leads = close.search(
        query, fields=["id", f"custom.{healthie_user_id_custom_field_id}"]
    )
    if args.verbose:
        print(f"Fetched {len(leads)} leads from {len(attendee_ids)} attendee IDs")

    # Create a mapping between healthie_user_id and lead IDs
    attendee_to_lead_mapping = {}
    for lead in leads:
        healthie_user_id = lead.get(f"custom.{healthie_user_id_custom_field_id}")
        if healthie_user_id:
            attendee_to_lead_mapping[healthie_user_id] = lead["id"]

    if args.verbose:
        print(f"{len(attendee_to_lead_mapping)} mappings")

    return attendee_to_lead_mapping


def diff_appointments_and_custom_activity_instances(
    appointments, custom_activity_instances, attendee_to_lead_mapping
):
    custom_field_id_name_mapping = close.get_custom_field_name_prefixed_id_mapping(
        f"activity/{procedure_custom_activity_type_id}"
    )
    healthie_appointment_id_custom_field_id = custom_field_id_name_mapping[
        "healthie_appointment_id"
    ]

    discrepancies = []

    # Create a set of tuples (appointment_id, lead_id) from custom_activity_instances for quick lookup
    activity_lookup = set(
        (activity[healthie_appointment_id_custom_field_id], activity["lead_id"])
        for activity in custom_activity_instances
    )

    # Iterate through each appointment
    for appointment in appointments:
        appointment_id = appointment["id"]
        attendees = appointment["attendees"]

        # Check each attendee in the appointment
        for attendee in attendees:
            lead_id = attendee_to_lead_mapping.get(attendee["id"])
            if lead_id is None:
                discrepancies.append(
                    {
                        "appointment_id": appointment_id,
                        "appointment_date": appointment["date"],
                        "appointment_created_at": appointment["created_at"],
                        "attendee_id": attendee["id"],
                        "attendee_name": attendee["name"],
                        "status": "Lead ID not found in mapping",
                    }
                )
            elif (appointment_id, lead_id) not in activity_lookup:
                discrepancies.append(
                    {
                        "appointment_id": appointment_id,
                        "appointment_date": appointment["date"],
                        "appointment_created_at": datetime.strptime(
                            appointment["created_at"], "%Y-%m-%d %H:%M:%S %z"
                        )
                        .astimezone(timezone.utc)
                        .isoformat(),
                        "attendee_id": attendee["id"],
                        "attendee_name": attendee["name"],
                        "lead_id": lead_id,
                        "appointment_status": appointment["pm_status"],
                        "status": "Not in custom_activity_instances",
                    }
                )

    # TODO: FIND CUSTOM ACTIVITY INSTANCES THAT DO NOT EXIST IN HEALTHIE.

    return discrepancies


async def main():
    appointments, procedure_custom_activity_instances = await asyncio.gather(
        fetch_appointments(325747),
        close.get_custom_activity_instances(procedure_custom_activity_type_id),
    )

    if not appointments:
        print("No appointments found.")
        return

    if not procedure_custom_activity_instances:
        print("No custom activity instances found.")
        return

    attendee_to_lead_mapping = get_attendee_to_lead_mapping(appointments)

    discrepancies = diff_appointments_and_custom_activity_instances(
        appointments, procedure_custom_activity_instances, attendee_to_lead_mapping
    )

    # Write discrepancies to CSV file
    filename = "output/appointment_discrepancies.csv"
    write_csv(
        filename,
        [
            "appointment_id",
            "appointment_date",
            "appointment_created_at",
            "attendee_id",
            "attendee_name",
            "lead_id",
            "appointment_status",
            "status",
        ],
        discrepancies,
    )
    print(f"{len(discrepancies)} discrepancies written to {filename}")


if __name__ == "__main__":
    asyncio.run(main())
