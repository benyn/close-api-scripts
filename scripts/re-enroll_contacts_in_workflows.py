import argparse
import asyncio
import json
import sys
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Unenroll & re-enroll untouched Contacts in Workflows"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument("--created-before", "-b", help="Created before")
arg_parser.add_argument("--workflow-name-prefix", "-w", help="Workflow name prefix")
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()


close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


def get_workflow_ids(prefix: str | None) -> list[str]:
    workflows = close.get_name_id_mapping("sequence")
    if prefix is None:
        return list(workflows.values())

    workflow_ids = [
        workflow_id
        for name, workflow_id in workflows.items()
        if name.startswith(prefix)
    ]
    return workflow_ids


def get_untouched_contact_ids(workflow_ids: list[str]) -> set[str]:
    query = {
        "type": "and",
        "queries": [
            {
                "type": "has_related",
                "this_object_type": "contact",
                "related_object_type": "sequence_subscription",
                "related_query": {
                    "type": "and",
                    "queries": [
                        {
                            "type": "field_condition",
                            "field": {
                                "type": "regular_field",
                                "object_type": "sequence_subscription",
                                "field_name": "sequence_id",
                            },
                            "condition": {
                                "type": "reference",
                                "reference_type": "sequence",
                                "object_ids": workflow_ids,
                            },
                        },
                        {
                            "type": "field_condition",
                            "field": {
                                "type": "regular_field",
                                "object_type": "sequence_subscription",
                                "field_name": "status",
                            },
                            "condition": {"type": "term", "values": ["active"]},
                        },
                    ],
                },
            },
            {
                "type": "has_related",
                "this_object_type": "contact",
                "related_object_type": "lead",
                "related_query": {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "lead",
                        "field_name": "times_communicated",
                    },
                    "condition": {"type": "number_range", "gte": 0, "lte": 0},
                },
            },
        ],
    }

    contacts = close.search(query, object_type="contact")
    contact_ids = {contact["id"] for contact in contacts}
    return contact_ids


def get_active_subscriptions(workflow_ids: list[str], contact_ids: set[str]):
    subscriptions = []

    for workflow_id in workflow_ids:
        if args.verbose:
            print(f"Getting subscriptions for {workflow_id}...")

        workflow_subscriptions = close.get_all(
            "sequence_subscription", params={"sequence_id": workflow_id}
        )
        subscriptions += [
            subscription
            for subscription in workflow_subscriptions
            if subscription["contact_id"] in contact_ids
            and subscription["status"] == "active"
            and (
                args.created_before is None
                or subscription["date_created"] < args.created_before
            )
        ]

    if args.verbose:
        print(f"Found {len(subscriptions)} subscriptions.")

    return subscriptions


async def main():
    workflow_ids = get_workflow_ids(args.workflow_name_prefix)
    if not workflow_ids:
        print(f"No workflows starting with '{args.workflow_prefix}' found.")
        return

    contact_ids = get_untouched_contact_ids(workflow_ids)
    if not contact_ids:
        print(f"No untouched contacts enrolled in the specified Workflows found.")
        return

    subscriptions = get_active_subscriptions(workflow_ids, contact_ids)
    if not subscriptions:
        print("No active subscriptions found.")
        return

    with open(
        f"output/workflow_subscriptions_unenrolled-{args.env}.json",
        "w",
    ) as f:
        json.dump(subscriptions, f)

    delete_endpoints = [f"sequence_subscription/{sub['id']}" for sub in subscriptions]
    post_endpoint_and_payloads = [
        (
            "sequence_subscription",
            {
                k: v
                for k, v in sub.items()
                if k not in ["id", "created_by_id", "updated_by_id", "date_updated"]
            },
        )
        for sub in subscriptions
    ]

    await close.delete_all(delete_endpoints)
    post_subscriptions, errors = await close.post_all(
        post_endpoint_and_payloads, verbose=args.verbose
    )
    if post_subscriptions:
        print(f"Recreated {len(post_subscriptions)} subscriptions.")
        with open(
            f"output/workflow_subscriptions_recreated-{args.env}.json",
            "w",
        ) as f:
            json.dump(post_subscriptions, f)

    if errors:
        print(f"{len(errors)} subscriptions could not be recreated.")
        with open(
            f"output/workflow_subscriptions_recreation_failed-{args.env}.json",
            "w",
        ) as f:
            json.dump(errors, f)


if __name__ == "__main__":
    asyncio.run(main())
