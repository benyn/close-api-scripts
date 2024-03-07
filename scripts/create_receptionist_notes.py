import argparse
import os
import sys
from datetime import datetime
from dateutil import tz
from CloseApiWrapper import CloseApiWrapper
from utils.csv import read_csv_to_dict
from utils.formatters import get_full_name
from utils.get_api_key import get_api_key
from utils.get_lead_id import get_lead_id
from utils.prompt_user_for_choice import prompt_user_for_choice


arg_parser = argparse.ArgumentParser(
    description="Enroll Lead in one of the Follow Up Workflows"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument("--csv-file", "-f", required=True, help="Lead ID")
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()


if not os.path.isfile(args.csv_file):
    print(f"Error: The file '{args.csv_file}' does not exist.")
    sys.exit(1)

records = read_csv_to_dict(args.csv_file)
if not records:
    print(f"No records found in the CSV file")
    sys.exit(1)

close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


def convert_answerconnect_datetime_to_utc(datetime_str: str) -> str:
    local_time = datetime.strptime(datetime_str, "%m/%d/%Y %I:%M %p %Z")
    utc_time = local_time.astimezone(tz.tzutc()).isoformat()
    return utc_time


def get_call_type(record: dict) -> str | None:
    has_procedure_to_close_call_type = {"Yes": "Post-Op Patient", "No": "Existing Lead"}
    answerconnect_call_type_to_close_call_type = {
        "New Patient": "New Lead",
        "Questions": "Questions",
        "Solicitations": None,
        "All Else": None,
    }

    working_with_patient_navigator = record.get("Working with a Patient Navigator")
    procedure_in_last_week = record.get("Procedure in the last week")
    call_type = record.get("Call Type")

    if working_with_patient_navigator == "Yes":
        return has_procedure_to_close_call_type.get(procedure_in_last_week)
    elif working_with_patient_navigator == "No":
        return answerconnect_call_type_to_close_call_type.get(call_type)

    return None


def get_lead_id(record: dict) -> str | None:
    if "Patient Email" in record:
        print("PATIENT EMAIL EXISTS")
        contact = close.find_contact_by_email(record["Patient Email"])
        print(f"contact {contact}")
        if contact:
            return contact["lead_id"]

    # TODO: Implement search for non-email cases

    return None


def createReceptionistNote(record: dict) -> dict:
    if not record.get("url"):
        raise ValueError("The 'url' field is required but not provided in the record.")

    lead_id = get_lead_id(record)
    message_taken = convert_answerconnect_datetime_to_utc(record["Message taken"])
    caller_name = get_full_name(record["Caller First Name"], record["Caller Last Name"])
    requested_person_name = (
        record["Calling For"]
        if "Calling For" in record and record["Calling For"] != "Others"
        else record.get("Person Requested")
    )
    call_type = get_call_type(record)
    message_id = record["url"].split("/")[-1]
    print(message_id)

    return {
        "lead_id": lead_id,
        "activity_at": message_taken,
        "date_created": message_taken,
        "Caller Name": caller_name,
        "Requested Representative Name": requested_person_name,
        "Call Type": call_type,
        "Question": record.get("Question"),
        "Message": record["Message"],
        "Full Message Link": record["url"],
        "answerconnect_message_id": message_id,
    }


def replaceReceptionistNoteCustomFieldKeys(
    activity: dict, custom_field_ids: dict
) -> dict:
    new_activity = {}
    for key, value in activity.items():
        new_key = custom_field_ids.get(key, key)
        new_activity[new_key] = value
    return new_activity


cat_id = close.get_custom_activity_type_id("Receptionist Note")
custom_field_ids = close.get_custom_field_name_prefixed_id_mapping(f"activity/{cat_id}")


for record in records:
    activity = createReceptionistNote(record)
    activity = replaceReceptionistNoteCustomFieldKeys(activity, custom_field_ids)
    activity["custom_activity_type_id"] = cat_id

    post_activity = close.post("activity/custom", data=activity)
    print(post_activity)
