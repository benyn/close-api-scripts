import argparse
import asyncio
import json
import logging
from CloseApiWrapper import CloseApiWrapper
from ZendeskApiWrapper import ZendeskApiWrapper
from utils.formatters import format_phone_number, get_full_name
from utils.get_api_key import get_api_key


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


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
arg_parser.add_argument("--since", "-s", help="Starting time.")
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()

# Zendesk API client
zendesk_access_token = get_api_key("api.getbase.com", args.env)
zendesk = ZendeskApiWrapper(access_token=zendesk_access_token)

# Close API client
close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


# Get Close Leads and Contacts
def get_close_contacts():
    close_contacts = close.get_all(
        "contact", params={"_fields": "id,name,emails,phones"}
    )

    # Transform the data
    contacts_dict = {}
    email_addresses = set()
    phone_numbers = set()

    for contact in close_contacts:
        standardized_name = contact["name"].strip().lower()
        contacts_dict[standardized_name] = {
            "id": contact["id"],
            "emails": contact["emails"],
            "phones": contact["phones"],
        }
        email_addresses.update([email["email"] for email in contact["emails"]])
        phone_numbers.update([phone["phone"] for phone in contact["phones"]])

    return contacts_dict, email_addresses, phone_numbers


def get_update_payload(lead, close_contacts, email_addresses, phone_numbers):
    update_payload = {}

    full_name = get_full_name(lead.first_name, lead.last_name)
    standardized_name = full_name.lower() if full_name else None
    # if not standardized_name in close_contacts:
    #     logging.warning(f"[{lead.id}] Name not found in Close: {full_name}")
    existing_contact = close_contacts.get(standardized_name)

    if lead.email:
        standardized_email = lead.email.lower()
        if standardized_email not in email_addresses:
            if existing_contact:
                new_email = {"email": standardized_email, "type": "direct"}
                update_payload["emails"] = existing_contact["emails"] + [new_email]
                logging.warning(
                    f"[{lead.id}] Email not found in Close: {standardized_email}"
                )
            else:
                logging.error(
                    f"[{lead.id}] Email & contact not found in Close: {standardized_email} ({full_name})"
                )

    if lead.mobile:
        standardized_mobile = format_phone_number(lead.mobile)
        if standardized_mobile not in phone_numbers:
            if existing_contact:
                new_mobile = {"phone": standardized_mobile, "type": "mobile"}
                if "phones" in update_payload:
                    update_payload["phones"].append(new_mobile)
                else:
                    update_payload["phones"] = existing_contact["phones"] + [new_mobile]
                logging.warning(
                    f"[{lead.id}] Mobile not found in Close: {standardized_mobile}"
                )
            else:
                logging.error(
                    f"[{lead.id}] Mobile & contact not found in Close: {standardized_mobile} ({full_name})"
                )

    if lead.phone:
        standardized_phone = format_phone_number(lead.phone)
        if standardized_phone not in phone_numbers:
            if existing_contact:
                new_phone = {"phone": standardized_phone, "type": "direct"}
                if "phones" in update_payload:
                    update_payload["phones"].append(new_phone)
                else:
                    update_payload["phones"] = existing_contact["phones"] + [new_phone]
                logging.warning(
                    f"[{lead.id}] Phone not found in Close: {standardized_phone}"
                )
            else:
                logging.error(
                    f"[{lead.id}] Phone & contact not found in Close: {standardized_phone} ({full_name})"
                )

    if update_payload and not existing_contact:
        logging.error(f"[{lead.id}] PAYLOAD EXISTS BUT NOT CONTACT?? {update_payload}")

    if existing_contact and update_payload:
        return f"contact/{existing_contact.get('id')}", update_payload

    return None


async def main():
    close_contacts, email_addresses, phone_numbers = get_close_contacts()
    if args.verbose:
        logging.info(
            f"{len(close_contacts)} Close contacts, {len(phone_numbers)} phone numbers, {len(email_addresses)} email addresses"
        )

    # Get Zendesk Leads
    zendesk_leads = zendesk.get_all("leads")
    if args.verbose:
        logging.info(f"Evaluating {len(zendesk_leads)} Zendesk leads...")

    update_payloads = []
    for lead in zendesk_leads:
        update_payload = get_update_payload(
            lead, close_contacts, email_addresses, phone_numbers
        )
        if update_payload:
            update_payloads.append(update_payload)

    if update_payloads:
        logging.info(f"Updating {len(update_payloads)} contacts...")
        with open(
            f"output/contact_updates_from_zendesk_leads-{args.env}.json",
            "w",
        ) as f:
            json.dump(update_payloads, f)

        updated_contacts, failed_updates = await close.put_all(
            update_payloads, verbose=args.verbose
        )
        if updated_contacts:
            logging.info(f"Updated {len(updated_contacts)} contacts.")
            with open(
                f"output/contacts_updated_from_zendesk_leads-{args.env}.json",
                "w",
            ) as f:
                json.dump(updated_contacts, f)

        if failed_updates:
            logging.info(f"{len(failed_updates)} updates could not be made.")
            with open(
                f"output/contact_updates_from_zendesk_leads-failed-{args.env}.json",
                "w",
            ) as f:
                json.dump(failed_updates, f)
    else:
        logging.info("No contacts were updated.")


if __name__ == "__main__":
    asyncio.run(main())
