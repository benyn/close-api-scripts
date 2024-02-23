import argparse
from datetime import datetime
import json
import sys

from closeio_api import APIError
from CloseApiWrapper import CloseApiWrapper
from ZendeskApiWrapper import ZendeskApiWrapper
from utils.formatters import convert_utc_z_to_offset_format, get_full_name
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

if args.env == "dev":
    patient_navigator_field_id = "custom.cf_fzBs9wJXD6nBEB9VK4BGiDyHblMuzJIGiHmpnhy63Yx"
    # patient_navigator_field_id = "custom.cf_4rzCyZ6WLz7M4seash24mlx1TXM4JGvh785NqkngAl9"
    preferred_language_field_id = (
        "custom.cf_T9IwO37LOVzLJ7G1SJdvpqoPF7rfJ1L4UckJpetMmnx"
    )
    healthie_user_id_field_id = "custom.cf_4rzCyZ6WLz7M4seash24mlx1TXM4JGvh785NqkngAl9"
elif args.env == "prod":
    patient_navigator_field_id = "custom.cf_sxqmodpl8iU0TIgi57m8B6K9bFRjxeqMJAMaSvG29gr"
    # patient_navigator_field_id = "custom.cf_8ziVuLyvS1SE5dkH2QS6h919rMvs1uRDepx5ORwRd12"
    preferred_language_field_id = (
        "custom.cf_ntTSow5AspSigwP4YgCo3lwIy8Fs7WWBi7pO8HszLLh"
    )
    healthie_user_id_field_id = "custom.cf_8ziVuLyvS1SE5dkH2QS6h919rMvs1uRDepx5ORwRd12"
else:
    print("Unsupported environment")
    sys.exit(1)


def create_user_id_mapping():
    # Map Zendesk user IDs to Close user IDs based on email
    zendesk_user_email_to_id = zendesk.get_user_ids_by_email()
    close_user_email_to_id = close.get_user_ids_by_email()
    zendesk_user_id_to_close_user_id = {
        zendesk_user_email_to_id[email]: close_user_email_to_id[email]
        for email in zendesk_user_email_to_id
        if email in close_user_email_to_id
    }
    return zendesk_user_id_to_close_user_id


def get_last_close_lead_creation_date():
    if args.since:
        return datetime.fromisoformat(args.since)

    last_created_leads = close.search(
        {
            "type": "field_condition",
            "field": {
                "type": "regular_field",
                "object_type": "lead",
                "field_name": "date_created",
            },
            "condition": {
                "type": "moment_range",
                "before": {"type": "now"},
                "on_or_after": {
                    "direction": "past",
                    "moment": {"type": "now"},
                    "offset": {
                        "days": 0,
                        "hours": 2,
                        "minutes": 0,
                        "months": 0,
                        "seconds": 0,
                        "weeks": 0,
                        "years": 0,
                    },
                    "type": "offset",
                    "which_day_end": "start",
                },
            },
            "negate": True,
        },
        sort=[
            {
                "direction": "desc",
                "field": {
                    "type": "regular_field",
                    "object_type": "lead",
                    "field_name": "date_created",
                },
            }
        ],
        results_limit=1,
        fields=["id", "name", "date_created"],
    )

    if last_created_leads:
        if args.verbose:
            print(
                f'Last created lead: {last_created_leads[0]["name"]} ({last_created_leads[0]["id"]})'
            )
        last_lead_date_created = last_created_leads[0]["date_created"]
        last_lead_creation_date = datetime.fromisoformat(last_lead_date_created)
        return last_lead_creation_date
    else:
        if args.verbose:
            print("No leads found in Close")
        return None


def map_zendesk_lead_to_close_lead(lead, user_id_mapping):
    name = get_full_name(lead.first_name, lead.last_name)

    urls = []
    if lead.twitter:
        urls.append({"url": f"https://twitter.com/{lead.twitter}", "type": "URL"})
    if lead.facebook:
        urls.append({"url": lead.facebook, "type": "URL"})
    # if lead.skype:
    #     urls.append({"url": lead.skype, "type": "URL"})
    if lead.linkedin:
        urls.append({"url": lead.linkedin, "type": "URL"})

    return {
        patient_navigator_field_id: user_id_mapping.get(lead.owner_id, None),
        "name": name,
        "date_created": convert_utc_z_to_offset_format(lead.created_at),
        "date_updated": convert_utc_z_to_offset_format(lead.updated_at),
        "contacts": [
            {
                "name": name,
                "title": lead.title,
                "emails": [{"email": lead.email, "type": "direct"}],
                "phones": [
                    {"phone": lead.mobile, "type": "mobile"},
                    {"phone": lead.phone, "type": "direct"},
                    {"phone": lead.fax, "type": "fax"},
                ],
                "urls": urls,
                preferred_language_field_id: lead.custom_fields.get("Language"),
            }
        ],
        "description": lead.description,
        "status_label": lead.status,
        "created_by": user_id_mapping.get(lead.creator_id, None),
        "url": lead.website,
        "addresses": [
            {
                "label": "mailing",
                "address_1": lead.address.line1,
                "city": lead.address.city,
                "state": lead.address.state,
                "zipcode": lead.address.postal_code,
                "country": lead.address.country,
            }
        ]
        if lead.address.line1
        or lead.address.city
        or lead.address.state
        or lead.address.postal_code
        or lead.address.country
        else None,
    }


def map_zendesk_contact_to_close_lead(contact, user_id_mapping):
    name = contact.name or get_full_name(contact.first_name, contact.last_name)

    urls = []
    if contact.twitter:
        urls.append({"url": f"https://twitter.com/{contact.twitter}", "type": "URL"})
    if contact.facebook:
        urls.append({"url": contact.facebook, "type": "URL"})
    if contact.linkedin:
        urls.append({"url": contact.linkedin, "type": "URL"})
    # if contact.skype:
    #     urls.append({"url": contact.skype, "type": "URL"})

    addresses = []
    if (
        contact.address.line1
        or contact.address.city
        or contact.address.state
        or contact.address.postal_code
        or contact.address.country
    ):
        addresses.append(
            {
                "label": "mailing",
                "address_1": contact.address.line1,
                "city": contact.address.city,
                "state": contact.address.state,
                "zipcode": contact.address.postal_code,
                "country": contact.address.country,
            }
        )

    if contact.shipping_address and (
        contact.shipping_address.line1
        or contact.shipping_address.city
        or contact.shipping_address.state
        or contact.shipping_address.postal_code
        or contact.shipping_address.country
    ):
        addresses.append(
            {
                "label": "mailing",
                "address_1": contact.shipping_address.line1,
                "city": contact.shipping_address.city,
                "state": contact.shipping_address.state,
                "zipcode": contact.shipping_address.postal_code,
                "country": contact.shipping_address.country,
            }
        )

    if contact.billing_address and (
        contact.billing_address.line1
        or contact.billing_address.city
        or contact.billing_address.state
        or contact.billing_address.postal_code
        or contact.billing_address.country
    ):
        addresses.append(
            {
                "label": "mailing",
                "address_1": contact.billing_address.line1,
                "city": contact.billing_address.city,
                "state": contact.billing_address.state,
                "zipcode": contact.billing_address.postal_code,
                "country": contact.billing_address.country,
            }
        )

    prospect_status_mapping = {
        "current": "Qualified",
        "lost": "Unqualified",
        "none": "Potential",
    }
    customer_status_mapping = {
        "current": "Patient",
        "past": "Disengaged",
        # "none": "Interested",
    }

    lead_status = customer_status_mapping.get(
        contact.customer_status, prospect_status_mapping.get(contact.prospect_status)
    )

    return {
        "created_by": user_id_mapping.get(contact.creator_id, None),
        "date_created": convert_utc_z_to_offset_format(contact.created_at),
        "date_updated": convert_utc_z_to_offset_format(contact.updated_at),
        "name": name,
        "contacts": [
            {
                "name": name,
                "title": contact.title,
                "emails": [{"email": contact.email, "type": "direct"}],
                "phones": [
                    {"phone": contact.phone, "type": "direct"},
                    {"phone": contact.mobile, "type": "mobile"},
                    {"phone": contact.fax, "type": "fax"},
                ],
                "urls": urls,
                preferred_language_field_id: contact.custom_fields.get("Language"),
            }
        ],
        patient_navigator_field_id: user_id_mapping.get(contact.owner_id, None),
        "description": contact.description,
        "status_label": lead_status,
        "url": contact.website,
        "addresses": addresses,
        healthie_user_id_field_id: contact.custom_fields.get("Healthie Ref"),
    }


def get_close_lead_id(resource_type, resource_id):
    # try:
    email_address = zendesk.get_email_address(resource_type, resource_id)
    # except ValueError as e:
    # print(f'Email not found for `{note["id"]}` because {str(e)}')
    # return None

    leads = close.search_leads_by_email(email_address, results_limit=1)
    if leads:
        return leads[0]["id"]
    else:
        print(f"Lead for `{email_address}` not found")
        return None


def map_zendesk_note_to_close_note(note, user_id_mapping):
    lead_id = get_close_lead_id(note.resource_type, note.resource_id)
    # try:
    #     email_address = zendesk.get_email_address(note.resource_type, note.resource_id)
    # except ValueError as e:
    #     print(f'Email not found for `{note["id"]}` because {str(e)}')
    #     return None
    #
    # leads = close.search_leads_by_email(email_address, results_limit=1)
    # if not leads:
    #     print(f"Lead for `{email_address}`")
    #     return None

    return {
        "user_id": user_id_mapping.get(note.creator_id, None),
        "note": note.content,
        "lead_id": lead_id,
        "activity_at": convert_utc_z_to_offset_format(note.created_at),
        "date_created": convert_utc_z_to_offset_format(note.created_at),
        "date_updated": convert_utc_z_to_offset_format(note.updated_at),
        "created_by": user_id_mapping.get(note.creator_id, None),
    }


def map_zendesk_task_to_close_task(task, user_id_mapping):
    lead_id = get_close_lead_id(task.resource_type, task.resource_id)

    return {
        "assigned_to": user_id_mapping.get(task.owner_id, None),
        "text": task.content,
        "date_created": convert_utc_z_to_offset_format(task.created_at),
        "date_updated": convert_utc_z_to_offset_format(task.updated_at),
        "date": convert_utc_z_to_offset_format(task.due_date),
        "created_by": user_id_mapping.get(task.creator_id, None),
        "is_complete": task.completed,
        "lead_id": lead_id,
    }


user_id_mapping = create_user_id_mapping()
last_lead_creation_date = get_last_close_lead_creation_date()


# last_lead_creation_date = datetime.fromisoformat("2024-02-09T00:00:00Z")
def sync_data(zendesk_resource_type, close_object_type):
    if args.verbose:
        print(f"Syncing Zendesk {zendesk_resource_type} as Close {close_object_type}s")

    zendesk_data = zendesk.get_all(zendesk_resource_type, last_lead_creation_date)
    if not zendesk_data:
        print(f"No new {zendesk_resource_type} since {last_lead_creation_date}")
        return

    with open(f"output/zendesk_{zendesk_resource_type}-{args.env}.json", "w") as f:
        json.dump(zendesk_data, f)

    function_mapping = {
        "leads": map_zendesk_lead_to_close_lead,
        "contacts": map_zendesk_contact_to_close_lead,
        "notes": map_zendesk_note_to_close_note,
        "tasks": map_zendesk_task_to_close_task,
    }
    mapper = function_mapping[zendesk_resource_type]
    if not mapper:
        raise ValueError("Invalid object type")

    close_objects = [
        mapper(data, user_id_mapping)
        for data in zendesk_data
        if close_object_type != "lead"
        or not data.email
        or not close.email_exists(data.email)
    ]

    if not close_objects:
        print(
            f"All {len(zendesk_data)} new Zendesk {zendesk_resource_type} already exist in Close"
        )
        if args.verbose:
            for item in zendesk_data:
                print(item["email"])
        return
    elif args.verbose:
        print(
            f"Syncing {len(close_objects)} new Zendesk {zendesk_resource_type} out of {len(zendesk_data)} total {zendesk_resource_type} not already in Close."
        )

    # Create new Leads
    created_objects = []
    failed_objects = []
    for obj in close_objects:
        try:
            post_obj = close.post(close_object_type, data=obj)
            created_objects.append(post_obj)
        except APIError as e:
            print(f"Couldn't add `{obj.get('name') or obj.get('id')}` because {str(e)}")
            failed_objects.append(obj)
            continue

    obj_type = (
        close_object_type.split("activity/")[1]
        if close_object_type.startswith("activity/")
        else close_object_type
    )

    print(f"Created {len(created_objects)} Close {close_object_type}s")
    with open(
        f"output/{obj_type}s_synced_from_zendesk_{zendesk_resource_type}-{args.env}.json",
        "w",
    ) as f:
        json.dump(created_objects, f)

    if failed_objects:
        print(f"{len(failed_objects)} Close {close_object_type}s")
        with open(
            f"output/{obj_type}s_not_synced_from_zendesk_{zendesk_resource_type}-{args.env}.json",
            "w",
        ) as f:
            json.dump(failed_objects, f)


# sync_data("leads", "lead")
# sync_data("contacts", "lead")
sync_data("notes", "activity/note")
sync_data("tasks", "task")
