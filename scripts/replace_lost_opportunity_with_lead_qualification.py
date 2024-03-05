import argparse
import sys
from typing import cast

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key
from utils.get_lead_id import get_lead_id
from utils.prompt_user_for_choice import prompt_user_for_choice


parser = argparse.ArgumentParser(
    description="Replace Lost opportunity with Lead Qualification form"
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument(
    "--lead-id",
    "-l",
    required=True,
    help="Lead ID",
)
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

if args.env:
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

api = CloseApiWrapper(api_key)


MARK_AS_NOT_INTERESTED = "Mark as Not Interested"
MARK_AS_UNQUALIFIED = "Mark as Unqualified"


def prompt_user_for_next_step(loss_reason: str) -> str:
    options = [MARK_AS_NOT_INTERESTED, MARK_AS_UNQUALIFIED]
    selected_option = prompt_user_for_choice(f"Next Step ({loss_reason})", options)
    return cast("str", selected_option)


def prompt_user_for_unqualified_reason(loss_reason: str) -> str:
    options = ["Low BMI", "High BMI", "No budget"]
    selected_option = prompt_user_for_choice(
        f"Unqualified Reasons ({loss_reason})", options
    )
    return cast("str", selected_option)


def get_next_step(loss_reason: str) -> str:
    if loss_reason in ["Other", "No Response"] or loss_reason is None:
        return prompt_user_for_next_step(loss_reason)
    elif loss_reason == "Medical Disqualification":
        return MARK_AS_UNQUALIFIED
    else:
        return MARK_AS_NOT_INTERESTED


def get_value_for_update(
    existing_value: str | None, new_values: list[str | None], field_name
) -> str | None:
    seen = set()
    options = []
    for value in [existing_value] + new_values:
        if value and value not in seen:
            seen.add(value)
            options.append(value)
    new_value = prompt_user_for_choice(field_name, options)
    if new_value != existing_value:
        return new_value
    else:
        print(f"Keeping {field_name} as {existing_value}")
        return None


def get_entries_for_update(existing_values: list[str], new_value: str, field_id: str):
    if not existing_values:
        return {field_id: [new_value]}
    elif new_value not in existing_values:
        return {f"{field_id}.add": new_value}
    else:
        print(f"Keeping existing values ({existing_values}) despite {new_value}")
        return {}


def get_lead_qualification_update_payload(lead_qualification, opportunity):
    payload = {}

    # Custom field IDs
    opp_custom_field_ids = api.get_custom_field_name_prefixed_id_mapping("opportunity")
    loss_reason_custom_field_id = opp_custom_field_ids["Loss Reason"]
    loss_reason_details_custom_field_id = opp_custom_field_ids["Loss Reason Details"]

    lq_custom_field_ids = api.get_custom_field_name_prefixed_id_mapping(
        f"activity/{lead_qualification['custom_activity_type_id']}"
    )
    next_step_custom_field_id = lq_custom_field_ids["Next Step"]
    unqualified_reasons_custom_field_id = lq_custom_field_ids["Unqualified Reasons"]
    notes_custom_field_id = lq_custom_field_ids["Notes"]

    # Loss Reason
    loss_reason = opportunity.get(loss_reason_custom_field_id)

    # Next Step
    existing_next_step = lead_qualification.get(next_step_custom_field_id)
    next_step = get_next_step(loss_reason)
    next_step_update = get_value_for_update(
        existing_next_step, [next_step], f"Next Step ({loss_reason})"
    )
    if next_step_update:
        payload[next_step_custom_field_id] = next_step_update

    # Loss Reason -> Concerns or Unqualified Reasons
    if (next_step_update or existing_next_step) == MARK_AS_NOT_INTERESTED:
        concerns_custom_field_id = lq_custom_field_ids["Concerns"]
        existing_concerns = lead_qualification.get(concerns_custom_field_id)
        entries_for_update = get_entries_for_update(
            existing_concerns, loss_reason, concerns_custom_field_id
        )
        if entries_for_update:
            payload.update(entries_for_update)
    elif (next_step_update or existing_next_step) == MARK_AS_UNQUALIFIED:
        existing_unqualified_reasons = lead_qualification.get(
            unqualified_reasons_custom_field_id
        )
        unqualified_reason = prompt_user_for_unqualified_reason(loss_reason)
        entries_for_update = get_entries_for_update(
            existing_unqualified_reasons,
            unqualified_reason,
            unqualified_reasons_custom_field_id,
        )
        if entries_for_update:
            payload.update(entries_for_update)

    # Notes
    existing_notes = lead_qualification.get(notes_custom_field_id)
    new_note_candidates = [
        opportunity.get(loss_reason_details_custom_field_id),
        opportunity.get("note"),
    ]
    notes_update = get_value_for_update(existing_notes, new_note_candidates, "Notes")
    if notes_update:
        payload[notes_custom_field_id] = notes_update

    # Draft -> Published
    if lead_qualification["status"] == "draft":
        payload["status"] = "published"

    return payload


def get_lead_qualification_create_payload(opportunity):
    # Custom Activity Type ID
    cat_id = api.get_custom_activity_type_id("Lead Qualification")
    payload = {"custom_activity_type_id": cat_id}

    # Custom Field IDs
    opp_custom_field_ids = api.get_custom_field_name_prefixed_id_mapping("opportunity")
    loss_reason_custom_field_id = opp_custom_field_ids["Loss Reason"]
    loss_reason_details_custom_field_id = opp_custom_field_ids["Loss Reason Details"]

    lq_custom_fields = api.get_custom_fields(f"activity/{cat_id}")
    lq_custom_field_id_set = {f"custom.{f['id']}" for f in lq_custom_fields}
    lq_custom_field_ids = {f["name"]: f"custom.{f['id']}" for f in lq_custom_fields}
    next_step_custom_field_id = lq_custom_field_ids["Next Step"]

    # Copy opportunity field values to payload
    copied_default_field_ids = {
        "created_by",
        "date_created",
        "date_updated",
        "lead_id",
        "updated_by",
        "user_id",
    }
    for key, value in opportunity.items():
        if key in copied_default_field_ids or key in lq_custom_field_id_set:
            payload[key] = value

    # Loss Reason
    loss_reason = opportunity.get(loss_reason_custom_field_id)

    # Next Step
    next_step = get_next_step(loss_reason)
    payload[next_step_custom_field_id] = next_step

    # Loss Reason -> Concerns or Unqualified Reasons
    if next_step == MARK_AS_NOT_INTERESTED:
        concerns_custom_field_id = lq_custom_field_ids["Concerns"]
        if loss_reason not in ["No Response"]:
            payload[concerns_custom_field_id] = loss_reason
    elif next_step == MARK_AS_UNQUALIFIED:
        unqualified_reasons_custom_field_id = lq_custom_field_ids["Unqualified Reasons"]
        unqualified_reason = prompt_user_for_unqualified_reason(loss_reason)
        payload[unqualified_reasons_custom_field_id] = [unqualified_reason]

    # Notes
    note_candidates = [
        opportunity.get(loss_reason_details_custom_field_id),
        opportunity.get("note"),
    ]
    note_candidates = [note for note in note_candidates if note]
    note = prompt_user_for_choice("Notes", note_candidates)
    if note:
        notes_custom_field_id = lq_custom_field_ids["Notes"]
        payload[notes_custom_field_id] = note

    return payload


def upsert_lead_qualification(opportunity, lead_qualification: dict | None):
    if lead_qualification:
        payload = get_lead_qualification_update_payload(lead_qualification, opportunity)
        updated_custom_activity_instance = api.put(
            f"activity/custom/{lead_qualification['id']}", payload
        )
        print(f"Updated {updated_custom_activity_instance['id']}")
        return updated_custom_activity_instance

    lead_qualification_data = get_lead_qualification_create_payload(opportunity)
    post_custom_activity_instance = api.post("activity/custom", lead_qualification_data)
    print(f"Created {post_custom_activity_instance['id']}")
    return post_custom_activity_instance


lead_id = get_lead_id(args.lead_id)
lost_opportunities = api.get_all(
    "opportunity", params={"lead_id": lead_id, "status_type": "lost"}
)

if not lost_opportunities:
    print("No lost opportunities found.")
    sys.exit(0)
elif len(lost_opportunities) > 1:
    # TODO: Support multiple lost opportunities.
    print("Multiple lost opportunities found.")
    sys.exit(0)

if args.verbose:
    print(
        f"{len(lost_opportunities)} lost opportunit{'y' if len(lost_opportunities) == 1 else 'ies'}"
    )

last_lost_opportunity = lost_opportunities[-1]

# Check for existing Lead Qualification custom activity instances
source_custom_activity_instance_id_field_id = api.get_prefixed_custom_field_id(
    "opportunity", "source_custom_activity_instance_id"
)
source_custom_activity_instance_id = last_lost_opportunity.get(
    source_custom_activity_instance_id_field_id
)
lead_qualification = (
    api.get(f"activity/custom/{source_custom_activity_instance_id}")
    if source_custom_activity_instance_id
    else api.get_last_lead_qualification(lead_id, args.verbose)
)
if lead_qualification:
    print(lead_qualification["id"])
upserted_lead_qualification = upsert_lead_qualification(
    last_lost_opportunity, lead_qualification
)
if upserted_lead_qualification:
    api.delete(f"opportunity/{last_lost_opportunity['id']}")
    print(f"Deleted {last_lost_opportunity['id']}")
