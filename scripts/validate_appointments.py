import argparse
import asyncio
import csv
from concurrent.futures import ThreadPoolExecutor

import requests
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


async def fetch_appointments():
    # GraphQL query
    query = """
    query {
        appointments(
            filter: "all"
            is_org: true
            should_paginate: false
        ) {
            id
            date
            created_at
            is_blocker
            contact_type
            location
            locationResource
            appointment_type_id
            appointment_type {
                name
            }
            is_group
            resourceId
            user_id
            # attendees {
            #     name
            # }
            # provider {
            #     name
            # }
        }
    }
    """

    # API endpoint
    url = "https://api.gethealthie.com/graphql"

    # Headers
    headers = {
        "Authorization": f"Basic {healthie_api_key}",
        "AuthorizationSource": "API",
    }

    # Make the API call
    response = await fetch_graphql(url, query, headers=headers)

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


def is_valid(appointment):
    issues = {}

    contact_type = appointment.get("contact_type")
    valid_contact_types = [
        "Healthie Video Call",
        "In Person",
        "Phone Call",
        "Secure Videochat",
    ]
    if contact_type not in valid_contact_types:
        if contact_type == "":
            issues["unexpected_contact_type"] = "(empty)"
        elif contact_type is None:
            issues["unexpected_contact_type"] = "None"
        else:
            issues["unexpected_contact_type"] = contact_type

    location = appointment.get("location")
    location_resource = appointment.get("locationResource")

    if contact_type == "In Person":
        valid_locations = [
            "Greater Miami",
            "Greater Tampa",
            "Greater Jacksonville",
            "IV Therapy",
        ]
        if location not in valid_locations:
            if location == "":
                issues["unexpected_location"] = "(empty)"
            elif location is None:
                issues["unexpected_location"] = "None"
            else:
                issues["unexpected_location"] = location
        if not location_resource:
            issues["missing_location_resource"] = True

        appointment_type = appointment.get("appointment_type")
        if appointment_type:
            appointment_type_name = appointment_type.get("name")
            if appointment_type_name not in ["Procedure Day!", "Balloon Removal"]:
                issues["unexpected_appointment_type"] = appointment_type_name
        elif not appointment.get("is_blocker"):
            issues["unexpected_appointment_type"] = "None"
    else:
        if not appointment.get("is_blocker") and location != contact_type:
            issues["location_mismatch"] = f"{contact_type} <> {location}"
        if location_resource:
            issues["unexpected_location_resource"] = f"{location} / {location_resource}"

    return issues


async def main():
    appointments = await fetch_appointments()
    if not appointments:
        print("No appointments found.")
        return

    issues_found = 0
    fieldnames = [
        "id",
        "date",
        "created_at",
        "is_blocker",
        "contact_type",
        "location",
        "locationResource",
        "appointment_type_id",
        "appointment_type_name",
        "is_group",
        "resourceId",
        "user_id",
        # "attendees",
        # "provider_name",
        "unexpected_contact_type",
        "unexpected_location",
        "missing_location_resource",
        "unexpected_appointment_type",
        "location_mismatch",
        "unexpected_location_resource",
    ]

    with open("output/appointment_issues.csv", "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for appointment in appointments:
            issues = is_valid(appointment)
            if issues:
                issues_found += 1
                row = {
                    "id": appointment.get("id"),
                    "date": appointment.get("date"),
                    "created_at": appointment.get("created_at"),
                    "is_blocker": appointment.get("is_blocker"),
                    "contact_type": appointment.get("contact_type"),
                    "location": appointment.get("location"),
                    "locationResource": appointment.get("locationResource"),
                    "appointment_type_id": appointment.get("appointment_type_id"),
                    "appointment_type_name": appointment.get("appointment_type")
                    and appointment["appointment_type"].get("name"),
                    "is_group": appointment.get("is_group"),
                    "resourceId": appointment.get("resourceId"),
                    "user_id": appointment.get("user_id"),
                    # "attendees": ", ".join(
                    #     [
                    #         attendee.get("name", "")
                    #         for attendee in appointment.get("attendees", [])
                    #     ]
                    # ),
                    # "provider_name": appointment.get("provider", {}).get("name"),
                }
                row.update(issues)
                writer.writerow(row)

    print(f"Number of appointments with issues: {issues_found}")


if __name__ == "__main__":
    asyncio.run(main())
