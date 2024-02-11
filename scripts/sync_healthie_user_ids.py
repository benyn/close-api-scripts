import argparse
import sys
from CloseApiWrapper import CloseApiWrapper
from ZendeskApiWrapper import ZendeskApiWrapper
from utils.csv import write_csv
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
args = arg_parser.parse_args()


def create_email_healthie_user_id_mapping(contacts):
    return {
        contact.get("email"): contact.get("custom_fields", {}).get("Healthie Ref")
        for contact in contacts
        if contact.get("email") and contact.get("custom_fields", {}).get("Healthie Ref")
    }


# Fetch Healthie user IDs stored in Zendesk contacts
zendesk_access_token = get_api_key("api.getbase.com", args.env)
zendesk = ZendeskApiWrapper(access_token=zendesk_access_token)

zendesk_contacts = zendesk.get_all_items("contacts")
print(f"{len(zendesk_contacts)} Zendesk contacts")
email_to_healthie_user_id = create_email_healthie_user_id_mapping(zendesk_contacts)
print(f"{len(email_to_healthie_user_id)} email to Healthie user ID mappings")

# Fetch Close Leads that lack Healthie User IDs
close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)

if args.env == "dev":
    healthie_user_id_field_id = "custom.cf_4rzCyZ6WLz7M4seash24mlx1TXM4JGvh785NqkngAl9"
elif args.env == "prod":
    healthie_user_id_field_id = "custom.cf_8ziVuLyvS1SE5dkH2QS6h919rMvs1uRDepx5ORwRd12"
else:
    print("Unsupported environment")
    sys.exit(0)

close_leads = close.get_all_items(
    "lead",
    params={
        "_fields": f"id,name,contacts,{healthie_user_id_field_id}",
    },
)
print(f"{len(close_leads)} Close leads")

# Update Close Leads with Healthie user IDs
leads_with_healthie_user_id_count = 0
leads_without_primary_email_count = 0
updated_leads = []
leads_without_healthie_user_id = []
for lead in close_leads:
    # Check if the lead has a HEALTHIE_USER_ID_FIELD_ID and has contacts with emails
    if lead.get(healthie_user_id_field_id) or not lead.get("contacts"):
        leads_with_healthie_user_id_count += 1
        continue

    primary_contact = lead["contacts"][0]
    if not primary_contact.get("emails"):
        leads_without_primary_email_count += 1
        continue

    primary_email = primary_contact["emails"][0].get("email")
    if not primary_email:
        leads_without_primary_email_count += 1
        continue

    healthie_user_id = email_to_healthie_user_id.get(primary_email)
    if healthie_user_id:
        close.put(
            f'lead/{lead["id"]}', data={healthie_user_id_field_id: healthie_user_id}
        )
        updated_leads.append(
            {
                "lead_id": lead["id"],
                "lead_name": lead["name"],
                "primary_email": primary_email,
                "healthie_user_id": healthie_user_id,
            }
        )
    else:
        leads_without_healthie_user_id.append(
            {
                "lead_id": lead["id"],
                "lead_name": lead["name"],
                "primary_email": primary_email,
            }
        )

print(f"Updated {len(updated_leads)} leads.")
print(
    f"{len(leads_without_healthie_user_id)} leads were not updated because healthie_user_id was not found."
)
print(f"{leads_with_healthie_user_id_count} leads already have Healthie user IDs.")
print(f"{leads_without_primary_email_count} leads do not have primary email.")

common_headers = ["lead_id", "lead_name", "primary_email"]
write_csv(
    f"output/leads_updated_with_healthie_user_id-{args.env}.csv",
    common_headers + ["healthie_user_id"],
    updated_leads,
)
write_csv(
    f"output/leads_without_healthie_user_id-{args.env}.csv",
    common_headers,
    leads_without_healthie_user_id,
)
