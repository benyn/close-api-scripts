import argparse
import asyncio
import json
import sys

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Replace Lost opportunity with Lead Qualification form"
)
parser.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
parser.add_argument(
    "--custom-activity-type", "-t", help="Custom Activity type", required=True
)
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

api_key = get_api_key("api.close.com", f"admin_{args.env}")
api = CloseApiWrapper(api_key)

opportunity_id_field_id = api.get_prefixed_custom_field_id("shared", "opportunity_id")
if not opportunity_id_field_id:
    print("Opportunity ID custom field not found")
    sys.exit(1)


def find_opportunity(custom_activity_instance, opportunities):
    if len(opportunities) == 1:
        return opportunities[0]

    filtered_opportunities = [
        opportunity
        for opportunity in opportunities
        if opportunity["date_created"] <= custom_activity_instance["activity_at"]
        and (
            not opportunity["date_won"]
            or opportunity["date_won"] >= custom_activity_instance["activity_at"]
        )
    ]

    if not filtered_opportunities:
        print(
            f"No opportunities found for {custom_activity_instance['id']} of {custom_activity_instance['lead_id']}"
        )
        return None

    # If there's only one opportunity left after filtering, return it
    if len(filtered_opportunities) == 1:
        return filtered_opportunities[0]

    # If multiple opportunities are still viable, prompt for choice
    print(
        f"Multiple opportunities found for {custom_activity_instance['id']} of {custom_activity_instance['lead_id']}"
    )
    return None


def get_custom_activity_instance_update_payloads(lead):
    if args.verbose and len(lead["custom_activity_instances"]) > 1:
        print(
            f"{len(lead['custom_activity_instances'])} Custom Activity instances for {lead['id']}"
        )

    endpoints_and_payloads = []
    for instance in lead["custom_activity_instances"]:
        if opportunity_id_field_id in instance:
            if args.verbose:
                print(f"{instance['id']} already linked to opportunity")
            continue

        opportunity = find_opportunity(instance, lead["opportunities"])
        if opportunity:
            endpoint = f"activity/custom/{instance['id']}"
            payload = {opportunity_id_field_id: opportunity["id"]}
            endpoints_and_payloads.append((endpoint, payload))

    return endpoints_and_payloads


async def main():
    custom_activity_type_id = api.get_custom_activity_type_id(args.custom_activity_type)
    if not custom_activity_type_id:
        print(f"Custom Activity '{args.custom_activity_type}' not found")
        return

    leads = await api.get_leads_with_custom_activity_instances_and_opportunities(
        custom_activity_type_id
    )
    print(f"{len(leads)} leads")
    all_endpoints_and_payloads = []
    unmatched_leads = []
    for lead in leads:
        endpoints_and_payloads = get_custom_activity_instance_update_payloads(lead)
        if endpoints_and_payloads:
            all_endpoints_and_payloads.extend(endpoints_and_payloads)
        elif any(
            opportunity_id_field_id not in instance
            for instance in lead["custom_activity_instances"]
        ):
            unmatched_leads.append(lead)

    if all_endpoints_and_payloads:
        (updated_instances, failed_updates) = await api.put_all(
            all_endpoints_and_payloads
        )
        if updated_instances:
            print(f"Updated {len(updated_instances)} Custom Activity instances.")
            with open(
                f"output/custom_activity_instances_updated_with_opportunity_ids-{args.env}.json",
                "w",
            ) as f:
                json.dump(updated_instances, f)

        if failed_updates:
            print(f"{len(failed_updates)} update attempts failed.")
            with open(
                f"output/custom_activity_instances_updated_with_opportunity_ids-errors-{args.env}.json",
                "w",
            ) as f:
                json.dump(failed_updates, f)
    else:
        print("No Custom Activity instances were matched with opportunities")

    if unmatched_leads:
        print(f"{len(unmatched_leads)} leads were not updated.")
        with open(
            f"output/custom_activity_instances_not_updated-{args.env}.json",
            "w",
        ) as f:
            json.dump(unmatched_leads, f)


if __name__ == "__main__":
    asyncio.run(main())
