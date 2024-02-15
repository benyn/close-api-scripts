import argparse
import asyncio
import json
import logging
import sys
from CloseApiWrapper import CloseApiWrapper
from ZendeskApiWrapper import ZendeskApiWrapper
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    "--field-name", "-f", required=True, help="Secondary email field name"
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


async def get_field_ids(field_name) -> tuple[str | None, str | None]:
    field_api_id = await zendesk.get_contact_custom_field_search_api_id(field_name)
    if field_api_id is None:
        return (None, None)
    field_id = field_api_id.split(".", 1)[1]
    return (field_api_id, field_id)


async def get_contacts_with_secondary_email(secondary_email_field_api_id):
    return await zendesk.filter_contacts(
        ["email", secondary_email_field_api_id],
        {
            "attribute": {"name": secondary_email_field_api_id},
            "parameter": {"is_null": False},
        },
    )


def append_secondary_email(contact, secondary_email_field_id):
    primary_email = contact["email"]
    secondary_email = contact["custom_fields"][secondary_email_field_id]

    if primary_email == secondary_email:
        if args.verbose:
            logging.warning(
                f"The secondary email is the same as the primary email ({contact['email']})"
            )
        return None

    close_contact = close.find_contact_by_email(primary_email)
    if close_contact is None:
        if args.verbose:
            logging.warning(f"No Close contact found with email {primary_email}")
        return None

    emails = close_contact["emails"]
    if any(email["email"] == secondary_email for email in emails):
        if args.verbose:
            logging.warning(
                f"Secondary email {secondary_email} already exists for Close contact"
            )
        return None

    emails.append({"email": secondary_email, "type": "home"})
    updated_contact = close.put(f"contact/{close_contact['id']}", {"emails": emails})
    if args.verbose:
        logging.info(f"Appended: {secondary_email}\tto {primary_email}")

    return updated_contact


def update_contacts(contacts, secondary_email_field_id):
    updated_contacts = []
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = [
            executor.submit(append_secondary_email, contact, secondary_email_field_id)
            for contact in contacts
        ]
        for future in as_completed(futures):
            updated_contact = future.result()
            if updated_contact:
                updated_contacts.append(updated_contact)
    return updated_contacts


async def main():
    try:
        (secondary_email_field_api_id, secondary_email_field_id) = await get_field_ids(
            args.field_name
        )
        if secondary_email_field_api_id is None:
            logging.error(f"Zendesk contact custom field not found: {args.field_name}")
            sys.exit(0)

        contacts = await get_contacts_with_secondary_email(secondary_email_field_api_id)

        if args.verbose:
            logging.info(f"{len(contacts)} Zendesk contacts found.")

        updated_contacts = update_contacts(contacts, secondary_email_field_id)

        if updated_contacts:
            logging.info(f"Updated {len(updated_contacts)} contacts.")
            with open(
                f"output/contacts_updated_with_secondary_email-{args.env}.json",
                "w",
            ) as f:
                json.dump(updated_contacts, f)
        else:
            logging.info("No contacts were updated.")

    except Exception as e:
        logging.error(e)


asyncio.run(main())
