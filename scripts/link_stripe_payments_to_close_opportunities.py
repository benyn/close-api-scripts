import argparse
import stripe
import sys
from typing import cast

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key
from utils.get_lead_id import get_lead_id
from utils.prompt_user_for_choice import prompt_user_for_choice


parser = argparse.ArgumentParser(
    description="Update Stripe charges with linked Close Opportunity IDs"
)
parser.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

api_key = get_api_key("api.close.com", f"admin_{args.env}")
stripe.api_key = get_api_key("api.stripe.com", args.env)

close = CloseApiWrapper(api_key)


def get_all_leads_with_won_opportunities():
    return close.search(
        {
            "type": "and",
            "queries": [
                {"type": "object_type", "object_type": "lead"},
                {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "lead",
                        "field_name": "num_won_opportunities",
                    },
                    "condition": {"type": "number_range", "gt": 0},
                },
            ],
        },
        fields=["name", "contacts", "opportunities"],
    )


def get_unlinked_charges():
    unlinked_charges = []
    next_page = None

    while True:
        charges = stripe.Charge.search(
            limit=100,
            page=next_page,
            query="metadata['close_opportunity_id']:null",
        )
        unlinked_charges.extend(charges.data)

        if not charges.has_more:
            break

        next_page = charges.next_page

    return unlinked_charges


def find_won_opportunity(opportunities):
    for opportunity in opportunities:
        if opportunity["status_type"] == "won":
            return opportunity
    return None


def find_matching_opportunity(charge, leads):
    charge_customer_email = (
        charge.billing_details.email.lower() if charge.billing_details.email else None
    )
    charge_customer_phone = (
        "+1" + charge.billing_details.phone if charge.billing_details.phone else None
    )
    charge_customer_name = (
        charge.billing_details.name.lower().strip()
        if charge.billing_details.name
        else None
    )

    for lead in leads:
        lead_emails = [
            email["email"]
            for contact in lead["contacts"]
            for email in contact["emails"]
        ]
        lead_phones = [
            phone["phone"]
            for contact in lead["contacts"]
            for phone in contact["phones"]
        ]
        lead_name = lead["name"].lower().strip()

        if charge_customer_email and charge_customer_email in lead_emails:
            return find_won_opportunity(lead["opportunities"])
        if charge_customer_phone and charge_customer_phone in lead_phones:
            return find_won_opportunity(lead["opportunities"])
        elif charge_customer_name == lead_name:
            return find_won_opportunity(lead["opportunities"])
    return None


leads = get_all_leads_with_won_opportunities()
charges = get_unlinked_charges()
print(f"{len(leads)} leads, {len(charges)} charges")

update_count = 0
charges_without_opportunity = []

for charge in charges:
    opportunity = find_matching_opportunity(charge, leads)
    if opportunity:
        resp = stripe.Charge.modify(
            charge.id, metadata={"close_opportunity_id": opportunity["id"]}
        )
        update_count += 1
    else:
        charges_without_opportunity.append(charge)
        if charge.amount > 5000:
            print(
                f"Unlinked: {charge.amount} {charge.billing_details.name} {charge.billing_details.email} {charge.billing_details.phone} {charge.receipt_email}"
            )

print(
    f"Linked {update_count} charges with opportunity, {len(charges_without_opportunity)} charges without opportunity"
)
