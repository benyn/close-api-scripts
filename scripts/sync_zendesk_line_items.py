import argparse
import asyncio
import json
import logging
import sys

from basecrm.errors import RateLimitError
from CloseApiWrapper import CloseApiWrapper
from ZendeskApiWrapper import ZendeskApiWrapper

from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Copy Healthie user IDs from Zendesk to Close"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument(
    "--field", "-f", required=True, help="Opportunity custom field name"
)
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Zendesk API client
zendesk_access_token = get_api_key("api.getbase.com", args.env)
zendesk = ZendeskApiWrapper(access_token=zendesk_access_token)

# Close API client
close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


services_field_id = close.get_custom_field_id("shared", args.field)
if not services_field_id:
    logging.error(f"Shared field '{args.field}' not found")
    sys.exit(1)

services_field_id_with_prefix = f"custom.{services_field_id}"


service_mapping = {
    "ESG": "ESG Stomach Tightening",
    "ESG-Stomach Tightening": "ESG Stomach Tightening",
    "Gastric Balloon": "Gastric Balloon",
    "Removal of Gastric Balloon": "Gastric Balloon Removal",
    "Revision of Bariatic Surgeries": "Revision of Sleeve Gastrectomy",
    "3 Month Medication Package ": "Medication Package (6 months)",
    "Monthly Medication Management Package ": "Medication Package (6 months)",
}


async def sync_order(order):
    deal_id = order["deal_id"]
    try:
        (deal, contact), line_items = await asyncio.gather(
            zendesk.get_deal_and_associated_primary_contact(deal_id),
            zendesk.get_line_items(order["id"]),
        )

        if not contact:
            logging.warning(f"Deal #{deal_id}: No associated contacts")
            return None

        if not contact["email"]:
            logging.warning(f"Deal #{deal_id}: No email for associated primary contact")
            return None

        close_lead = await close.find_lead_by_email(
            contact["email"], ["id", "opportunities"]
        )
        if not close_lead:
            logging.warning(
                f"Deal #{deal_id}: No matching Close Lead with {contact['email']}"
            )
            return None

        if not close_lead["opportunities"]:
            logging.warning(
                f"Deal #{deal_id}: No Close Opportunities for {close_lead['id']} (Zendesk Stage {deal['stage_id']})"
            )
            return None

        opp = next(
            (
                opp
                for opp in close_lead["opportunities"]
                if services_field_id_with_prefix not in opp
            ),
            None,
        )
        if not opp:
            logging.warning(f"Deal #{deal_id}: all Close opportunities have line items")
            return None

        services = [
            service_mapping.get(item["name"], item["name"]) for item in line_items
        ]
        payload = {services_field_id_with_prefix: services}
        if any(item["name"] == "3 Month Medication Package " for item in line_items):
            payload["note"] = "Medication Package (3 months) in Zendesk"

        updated_opp = await close.update_opportunity(opp["id"], data=payload)
        if args.verbose:
            logging.info(
                f"Deal #{deal_id} -> Lead {close_lead['id']} ({','.join(services)})"
            )
        return updated_opp

    except RateLimitError as e:
        logging.error(f"Deal #{deal_id}: Could not be synced due to {e}")


async def sync_order_group(orders):
    tasks = [sync_order(order) for order in orders]
    return await asyncio.gather(*tasks)


async def main():
    orders = zendesk.get_all("orders")
    if args.verbose:
        logging.info(f"Syncing {len(orders)} orders from Zendesk to Close...")

    group_size = 2
    updated_opps = []

    for i in range(0, len(orders), group_size):
        group_orders = orders[i : i + group_size]
        updated_opps_group = await sync_order_group(group_orders)
        updated_opps.extend(opp for opp in updated_opps_group if opp)

    if updated_opps:
        logging.info(f"Updated {len(updated_opps)} opportunities.")
        with open(
            f"output/opportunities_updated_with_services-{args.env}.json",
            "w",
        ) as f:
            json.dump(updated_opps, f)
    else:
        logging.info("No opportunities were updated.")


asyncio.run(main())
