import argparse
import asyncio
import json
import logging
import sys
from CloseApiWrapper import CloseApiWrapper
from utils.formatters import get_full_name

from utils.get_api_key import get_api_key


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

parser = argparse.ArgumentParser(description="Change workflow call assignee")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument(
    "--from-assignee",
    "-f",
    required=True,
    help="Current call assignee",
)
parser.add_argument(
    "--to-assignee",
    "-t",
    help="New call assignee",
)
parser.add_argument("--payload", "-p")
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

if args.env:
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

api = CloseApiWrapper(api_key)


patient_navigator_field_id = api.get_custom_field_id("lead", "Patient Navigator")
if not patient_navigator_field_id:
    logging.error("Patient Navigator custom field ID not found")
    sys.exit(1)

patient_navigator_field_id_with_prefix = f"custom.{patient_navigator_field_id}"


def get_user_ids(
    from_assignee_name: str, to_assignee_name: str
) -> tuple[str | None, str | None]:
    users = api.get_all("user", params={"_fields": "id,first_name,last_name"})
    from_user_id = None
    to_user_id = None

    for user in users:
        full_name = get_full_name(user["first_name"], user["last_name"])
        if full_name == from_assignee_name:
            from_user_id = user["id"]
            if to_user_id is not None:
                return from_user_id, to_user_id
        elif full_name == to_assignee_name:
            to_user_id = user["id"]
            if from_user_id is not None:
                return from_user_id, to_user_id

    return from_user_id, to_user_id


def get_subscriptions(call_assignee_id: str):
    from_subs = []

    workflows = api.get_all("sequence", params={"_fields": "id"})
    for workflow in workflows:
        if args.verbose:
            logging.info(f"Getting subscriptions for {workflow['name']}...")

        workflow_subscriptions = api.get_all(
            "sequence_subscription",
            params={"sequence_id": workflow["id"]},  # "_fields" doesn't work
        )
        from_subs += [
            sub
            for sub in workflow_subscriptions
            if call_assignee_id in sub.get("calls_assigned_to")
            and sub["status"] == "active"
        ]

    if args.verbose:
        logging.info(f"Found {len(from_subs)} subscriptions.")

    return from_subs


def get_patient_navigator_id(lead_id: str) -> str | None:
    lead = api.get(
        f"lead/{lead_id}", params={"_fields": patient_navigator_field_id_with_prefix}
    )
    if patient_navigator_field_id_with_prefix in lead:
        return lead[patient_navigator_field_id_with_prefix]
    return None


def get_delete_endpoint_and_reassign_payload(
    subscriptions, to_assignee_id: str | None
) -> tuple[list[str], list[dict[str, str | list[str]]]]:
    delete_endpoints = []
    reassign_payloads = []

    for subscription in subscriptions:
        new_assignee_id = (
            to_assignee_id
            if to_assignee_id
            else get_patient_navigator_id(subscription["lead_id"])
        )

        if not new_assignee_id:
            logging.error(f"New assignee not found for {subscription['lead_id']}")
            continue

        endpoint = f"sequence_subscription/{subscription['id']}"
        delete_endpoints.append(endpoint)

        del subscription["id"]
        del subscription["calls_assigned_to"]
        del subscription["created_by_id"]
        del subscription["updated_by_id"]
        del subscription["date_created"]
        del subscription["date_updated"]
        del subscription["from_phone_number_id"]
        del subscription["initial_email_id"]
        del subscription["sender_account_id"]
        del subscription["sender_email"]
        del subscription["sender_name"]
        subscription["calls_assigned_to"] = [new_assignee_id]
        reassign_payloads.append(("sequence_subscription", subscription))

    return delete_endpoints, reassign_payloads


async def main():
    from_assignee_id, to_assignee_id = get_user_ids(
        args.from_assignee, args.to_assignee
    )
    if not from_assignee_id:
        logging.error(f"User IDs not found for {args.from_assignee}")
        return

    subscriptions = get_subscriptions(from_assignee_id)
    delete_endpoints, reassign_payloads = get_delete_endpoint_and_reassign_payload(
        subscriptions, to_assignee_id
    )

    if args.verbose:
        logging.info(
            f"Reassigning {len(reassign_payloads)} of {len(subscriptions)} subscriptions"
        )

    if delete_endpoints:
        with open(
            f"output/workflow_subscriptions_deleted-{args.env}.json",
            "w",
        ) as f:
            json.dump(delete_endpoints, f)

        results = await api.delete_all(delete_endpoints)
        print(results)

    if reassign_payloads:
        with open(
            f"output/workflow_subscriptions_payload-{args.env}.json",
            "w",
        ) as f:
            json.dump(reassign_payloads, f)

        created_subscriptions, failed_creations = await api.post_all(
            reassign_payloads, verbose=args.verbose
        )
        if created_subscriptions:
            logging.info(f"Reassigned {len(created_subscriptions)} subscriptions.")
            with open(
                f"output/workflow_subscriptions_created-{args.env}.json",
                "w",
            ) as f:
                json.dump(created_subscriptions, f)

        if failed_creations:
            logging.info(f"{len(failed_creations)} subscriptions could not be created.")
            with open(
                f"output/workflow_subscriptions-failed-{args.env}.json",
                "w",
            ) as f:
                json.dump(failed_creations, f)


if __name__ == "__main__":
    asyncio.run(main())
