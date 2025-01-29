import argparse
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from CloseApiWrapper import CloseApiWrapper
from closeio_api import APIError
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Replace Lost opportunity with Lead Qualification form"
)
parser.add_argument("--prod", "-p", action="store_true", help="production environment")
parser.add_argument(
    "--custom-activity-type", "-t", help="Custom Activity type", required=True
)
parser.add_argument("--dry-run", "-d", action="store_true", help="dry run")
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

env = "prod" if args.prod else "dev"
api_key = get_api_key("api.close.com", f"admin_{env}")
api = CloseApiWrapper(api_key)


def find_opportunity(
    custom_activity_instance: dict, opportunities: list[dict], provider_field_id: str
) -> dict | None:
    # Temporarily allow only existing opportunities to be linked since some opportunities are created after the activity
    if len(opportunities) == 1:
        return opportunities[0]

    eastern_date = (
        datetime.fromisoformat(custom_activity_instance["activity_at"])
        .astimezone(ZoneInfo("America/New_York"))
        .date()
        .isoformat()
    )

    filtered_opportunities = [
        opp
        for opp in opportunities
        if opp["date_created"] <= custom_activity_instance["activity_at"]
        and (
            opp["status_type"] == "active"
            or (
                opp["status_type"] == "lost"
                and opp["date_lost"] >= custom_activity_instance["activity_at"]
            )
            or (opp["status_type"] == "won" and opp["date_won"] >= eastern_date)
        )
    ]

    if not filtered_opportunities:
        if args.verbose:

            def reason(opp) -> str:
                if opp["date_created"] > custom_activity_instance["activity_at"]:
                    return f"  vs. \033[34mcreated\033[0m: {opp['date_created']} ({opp['id']})"
                if (
                    opp["status_type"] == "lost"
                    and opp["date_lost"] < custom_activity_instance["activity_at"]
                ):
                    return f"  vs. \033[31mlost\033[0m:    {opp['date_lost']} ({opp['id']})"
                if opp["status_type"] == "won" and opp["date_won"] < eastern_date:
                    return f"  vs. \033[32mwon\033[0m:     {opp.get('date_won')} ({opp['id']})"
                return f"  vs. \033[31mlost\033[0m:    {opp['date_lost']}+\033[32mwon\033[0m:   {opp.get('date_won')} ({opp['id']})"

            print(
                f"{custom_activity_instance['lead_id']}: "
                f"No opportunities found out of {len(opportunities)} "
                f"for {custom_activity_instance['id']} of {custom_activity_instance['lead_id']} "
            )
            if opportunities:
                print(
                    f"  activity at: {custom_activity_instance['activity_at']} ({custom_activity_instance.get(provider_field_id)})"
                )
                print(
                    "\n".join(
                        reason(o)
                        for o in opportunities
                        if o not in filtered_opportunities
                    )
                )

        return None

    # If there's only one opportunity left after filtering, return it
    if len(filtered_opportunities) == 1:
        return filtered_opportunities[0]

    # If multiple opportunities are still viable, prompt for choice
    print(
        f"{custom_activity_instance['lead_id']}: Multiple opportunities found for {custom_activity_instance['id']}"
    )
    return None


def get_custom_activity_instance_update_payloads(
    lead: dict, opportunity_id_field_id: str, provider_field_id: str
) -> list[tuple[str, dict]]:
    # if args.verbose and len(lead["custom_activity_instances"]) > 1:
    #     print(
    #         f"{lead['id']} has {len(lead['custom_activity_instances'])} Custom Activity instances"
    #     )

    endpoints_and_payloads = []
    for instance in lead["custom_activity_instances"]:
        opportunity = find_opportunity(
            instance, lead["opportunities"], provider_field_id
        )

        if opportunity_id_field_id in instance:
            existing_opp_id = instance[opportunity_id_field_id]
            if not opportunity:
                if lead["primary_email"]["email"].lower().endswith("@bariendo.com"):
                    if args.verbose:
                        print(
                            f"ℹ️ {lead['id']}: Skipping {instance['id']} because this is a test lead"
                        )
                    continue

                print(
                    f"👻 {lead['id']}: {instance['id']} has opportunity ID {existing_opp_id} but no matching opportunity was found ({lead['display_name']})"
                )
                continue

            if existing_opp_id != opportunity["id"]:
                if existing_opp_id == "deleted_opportunity":
                    print(
                        f"⚠️ {lead['id']}: {instance['id']} linked to deleted opportunity, but should be {opportunity['id']} ({opportunity['lead_name']})"
                    )
                    continue

                try:
                    existing_opp = api.get(f"opportunity/{existing_opp_id}")
                except APIError as e:
                    existing_opp = None
                    if e.response.text == '{"error": "Not found"}\n':
                        existing_opp = None
                    else:
                        raise
                print(
                    f"⚠️ {lead['id']}: {instance['id']} linked to {'wrong' if existing_opp else 'missing'} opportunity {existing_opp_id} ({existing_opp}), but should be {opportunity['id']} ({opportunity['lead_name']})"
                )
                endpoint = f"activity/custom/{instance['id']}"
                payload = {opportunity_id_field_id: opportunity["id"]}
                endpoints_and_payloads.append((endpoint, payload))

            # if args.verbose:
            #     print(
            #         f"✅ {instance['id']} correctly linked to opportunity {existing_opp_id}"
            #     )
            continue

        if opportunity:
            endpoint = f"activity/custom/{instance['id']}"
            payload = {opportunity_id_field_id: opportunity["id"]}
            endpoints_and_payloads.append((endpoint, payload))

    return endpoints_and_payloads


async def main():
    custom_activity_type_id = api.get_custom_activity_type_id(args.custom_activity_type)
    if not custom_activity_type_id:
        raise ValueError(
            f"Custom Activity type '{args.custom_activity_type}' not found"
        )

    opportunity_id_field_id = api.get_prefixed_custom_field_id(
        f"activity/{custom_activity_type_id}", "opportunity_id"
    )
    if not opportunity_id_field_id:
        if args.verbose:
            print(f"Custom Activity type ID: {custom_activity_type_id}")
        raise ValueError(
            f"Opportunity ID custom field not found for Custom Activity type '{args.custom_activity_type}'"
        )

    provider_field_id = api.get_prefixed_custom_field_id(
        f"activity/{custom_activity_type_id}",
        "Provider",
    )

    # TODO: Validate Healthie appointment IDs (exists? correct appointment type?)
    leads = await api.get_leads_with_custom_activity_instances(
        custom_activity_type_id,
        lead_fields=["id", "opportunities", "primary_email", "display_name"],
        custom_activity_fields=[
            "id",
            "lead_id",
            "lead_name",  # Include for debugging
            "activity_at",
            "date_created",
            opportunity_id_field_id,
            provider_field_id,
            # "custom",  # Include for debugging
        ],
        verbose=args.verbose,
    )
    print(
        f"Found {len(leads)} leads with total {sum(len(lead['custom_activity_instances']) for lead in leads)} {args.custom_activity_type} Custom Activity instances"
    )

    all_endpoints_and_payloads = []
    unmatched_leads = []
    for lead in leads:
        endpoints_and_payloads = get_custom_activity_instance_update_payloads(
            lead, opportunity_id_field_id, provider_field_id
        )
        if endpoints_and_payloads:
            all_endpoints_and_payloads.extend(endpoints_and_payloads)
        elif any(
            opportunity_id_field_id not in instance
            for instance in lead["custom_activity_instances"]
        ):
            unmatched_leads.append(lead)

    if args.dry_run:
        print(f"Dry run, not updating {len(all_endpoints_and_payloads)} instances.")
        return

    if all_endpoints_and_payloads:
        (updated_instances, failed_updates) = await api.put_all(
            all_endpoints_and_payloads
        )
        if updated_instances:
            print(f"Updated {len(updated_instances)} Custom Activity instances.")
            with open(
                f"output/custom_activity_instances_updated_with_opportunity_ids-{env}.json",
                "w",
            ) as f:
                json.dump(updated_instances, f)

        if failed_updates:
            print(f"{len(failed_updates)} update attempts failed.")
            with open(
                f"output/custom_activity_instances_updated_with_opportunity_ids-errors-{env}.json",
                "w",
            ) as f:
                json.dump(failed_updates, f)
    else:
        print("No Custom Activity instances were matched with opportunities")

    if unmatched_leads:
        print(f"{len(unmatched_leads)} leads were not updated.")
        with open(
            f"output/custom_activity_instances_not_updated-{env}.json",
            "w",
        ) as f:
            json.dump(unmatched_leads, f)


if __name__ == "__main__":
    asyncio.run(main())
