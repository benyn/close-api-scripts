import argparse
import json
import sys
from typing import Any, List, cast
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Assign Leads to Patient Navigators based on Deal Owners, Task Assignees, and Note Creators"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = arg_parser.parse_args()


close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)

if args.env == "dev":
    patient_navigator_field_id = "cf_fzBs9wJXD6nBEB9VK4BGiDyHblMuzJIGiHmpnhy63Yx"
elif args.env == "prod":
    patient_navigator_field_id = "cf_sxqmodpl8iU0TIgi57m8B6K9bFRjxeqMJAMaSvG29gr"
else:
    print("Unsupported environment")
    sys.exit(1)

patient_navigator_field_id_with_prefix = f"custom.{patient_navigator_field_id}"


def update_lead_patient_navigator_with_opportunity_owner(lead):
    # Find the user_id of the primary opportunity.
    opp_user_id = next(
        (
            opportunity["user_id"]
            for opportunity in lead["opportunities"]
            if "user_id" in opportunity
        ),
        None,
    )
    if opp_user_id:
        updated_lead = close.put(
            f"lead/{lead['id']}",
            data={patient_navigator_field_id_with_prefix: opp_user_id},
        )
        return updated_lead
    return None


def update_lead_patient_navigator_with_task_assignee(lead, patient_navigator_ids):
    # Find the assigned_to of the first task assigned to a Patient Navigator.
    assignee_id = next(
        (
            task["assigned_to"]
            for task in lead["tasks"]
            if "assigned_to" in task and task["assigned_to"] in patient_navigator_ids
        ),
        None,
    )
    if assignee_id:
        updated_lead = close.put(
            f"lead/{lead['id']}",
            data={patient_navigator_field_id_with_prefix: assignee_id},
        )
        return updated_lead
    elif args.verbose and lead["tasks"]:
        print(
            f"Patient navigator user ID not found among {len(lead['tasks'])} tasks for {lead['id']} {lead['name']}"
        )
    return None


def update_lead_patient_navigator_with_note_author(lead, patient_navigator_ids):
    lead_notes = close.get(
        "activity/note",
        params={"lead_id": lead["id"], "_fields": "created_by,date_created"},
    )["data"]
    lead_notes = cast(List[Any], lead_notes)
    # Find the created_by of the most recently created note by a Patient Navigator.
    # Favor `created_by` over `user_id`, since the latter is the id of the user
    # whose API key was used for migration.
    lead_notes.sort(key=lambda x: x["date_created"], reverse=True)
    note_created_by = next(
        (
            note["created_by"]
            for note in lead_notes
            if "created_by" in note and note["created_by"] in patient_navigator_ids
        ),
        None,
    )
    if note_created_by:
        updated_lead = close.put(
            f"lead/{lead['id']}",
            data={patient_navigator_field_id_with_prefix: note_created_by},
        )
        return updated_lead
    elif args.verbose:
        print(
            f"Patient navigator user ID not found among {len(lead_notes)} notes for {lead['id']} {lead['name']}"
        )
    return None


def update_lead_patient_navigator_with_email_user(lead, patient_navigator_ids):
    lead_emails = close.get(
        "activity/email",
        params={"lead_id": lead["id"], "_fields": "user_id,date_created"},
    )["data"]
    lead_emails = cast(List[Any], lead_emails)
    # Find the user_id of the most recently created note by a Patient Navigator.
    lead_emails.sort(key=lambda x: x["date_created"], reverse=True)
    email_user_id = next(
        (
            email["user_id"]
            for email in lead_emails
            if "user_id" in email and email["user_id"] in patient_navigator_ids
        ),
        None,
    )
    if email_user_id:
        updated_lead = close.put(
            f"lead/{lead['id']}",
            data={patient_navigator_field_id_with_prefix: email_user_id},
        )
        return updated_lead
    elif args.verbose:
        print(
            f"Patient navigator user ID not found among {len(lead_emails)} emails for {lead['id']} {lead['name']}"
        )
    return None


unassigned_leads = close.search(
    {
        "type": "and",
        "queries": [
            {
                "type": "field_condition",
                "field": {
                    "type": "custom_field",
                    "custom_field_id": patient_navigator_field_id,
                },
                "condition": {"type": "exists"},
                "negate": True,
            },
            {
                "type": "or",
                "queries": [
                    {
                        "type": "field_condition",
                        "field": {
                            "type": "regular_field",
                            "object_type": "lead",
                            "field_name": "num_opportunities",
                        },
                        "condition": {"gt": 0, "type": "number_range"},
                    },
                    {
                        "type": "field_condition",
                        "field": {
                            "type": "regular_field",
                            "object_type": "lead",
                            "field_name": "num_tasks",
                        },
                        "condition": {"gt": 0, "type": "number_range"},
                    },
                    {
                        "type": "field_condition",
                        "field": {
                            "type": "regular_field",
                            "object_type": "lead",
                            "field_name": "num_notes",
                        },
                        "condition": {"gt": 0, "type": "number_range"},
                    },
                    {
                        "type": "field_condition",
                        "field": {
                            "type": "regular_field",
                            "object_type": "lead",
                            "field_name": "num_emails",
                        },
                        "condition": {"gt": 0, "type": "number_range"},
                    },
                ],
            },
        ],
    },
    fields=["id", "name", "opportunities", "tasks"],
    results_limit=10,
)

if not unassigned_leads:
    print("No unassigned leads found.")
    sys.exit(0)

if args.verbose:
    print(
        f"Processing {len(unassigned_leads)} leads without assigned Patient Navigators..."
    )

patient_navigator_ids = close.get_user_ids_by_group("Patient Navigators")

updated_leads = []
unchanged_leads = []
for lead in unassigned_leads:
    updated_lead_opp_owner = update_lead_patient_navigator_with_opportunity_owner(lead)
    if updated_lead_opp_owner:
        if args.verbose:
            print(
                f"✅ {lead['id']} {lead['name']}\t-> {updated_lead_opp_owner[patient_navigator_field_id_with_prefix]} (opportunity owner)"
            )
        updated_leads.append(updated_lead_opp_owner)
        continue

    updated_lead_task_assignee = update_lead_patient_navigator_with_task_assignee(
        lead, patient_navigator_ids
    )
    if updated_lead_task_assignee:
        if args.verbose:
            print(
                f"✅ {lead['id']} {lead['name']}\t-> {updated_lead_task_assignee[patient_navigator_field_id_with_prefix]} (task assignee)"
            )
        updated_leads.append(updated_lead_task_assignee)
        continue

    updated_lead_note_author = update_lead_patient_navigator_with_note_author(
        lead, patient_navigator_ids
    )
    if updated_lead_note_author:
        if args.verbose:
            print(
                f"✅ {lead['id']} {lead['name']}\t-> {updated_lead_note_author[patient_navigator_field_id_with_prefix]} (note author)"
            )
        updated_leads.append(updated_lead_note_author)
        continue

    updated_lead_email_user = update_lead_patient_navigator_with_email_user(
        lead, patient_navigator_ids
    )
    if updated_lead_email_user:
        if args.verbose:
            print(
                f"✅ {lead['id']} {lead['name']}\t-> {updated_lead_email_user[patient_navigator_field_id_with_prefix]} (email user)"
            )
        updated_leads.append(updated_lead_email_user)
        continue

    if args.verbose:
        print(
            f"❓ No opportunity owner, task assignee, note creator, or email user found for {lead['id']} {lead['name']}"
        )
    unchanged_leads.append(lead)


if updated_leads:
    print(f"Updated {len(updated_leads)} out of {len(unassigned_leads)} leads.")
    with open(f"output/leads_updated_with_pn-{args.env}.json", "w") as f:
        json.dump(updated_leads, f)
else:
    print("No leads were updated.")

if unchanged_leads:
    print(f"{len(unchanged_leads)} leads could not be updated.")
    with open(f"output/leads_unchanged_with_pn-{args.env}.json", "w") as f:
        json.dump(unchanged_leads, f)
