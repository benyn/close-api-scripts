import argparse
import asyncio
import csv
from collections import defaultdict, namedtuple
from datetime import datetime
import json
import logging
import os
import sys
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Sync CallTrackingMetrics records to Close"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument(
    "--data-path", "-f", required=True, help="Path to CTM data file"
)
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


if not os.path.exists(args.data_path):
    logging.error(f"The data file {args.data_path} does not exist.")
    sys.exit(0)


# Close API client
close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


def read_json_file(path: str):
    with open(path, "r") as f:
        return json.load(f)


FailureReason = namedtuple("FailureReason", ["reason", "details"])

ADMIN = ""
CM = ""
SS = ""
DP = ""
CD_OLD = ""
CD_NEW = ""
RHQ = ""
RHQ_NUMBERS = set([""])
CLOSE_NUMBERS = set(
    [
        "",
    ]
)

caller_number_to_user_email = {}

automated_message_starts = [
    "",
    "",
    "",
]

message_phrase_to_agent_email = {
    "SS": SS,
    "DP": DP,
    "CM1": CM,
    "CM2": CM,
}

call_status_disposition_map = {
    "answered": ("completed", "answered"),
    "no answer": ("no-answer", "no-answer"),
    "hangup": ("cancel", "no-answer"),
    "canceled": ("cancel", "no-answer"),
    "busy": ("busy", "busy"),
    "completed": ("completed", "answered"),  # IVR only, N/A to Close
}

sms_status_map = {
    ("inbound", "received"): "inbox",
    ("inbound", "sent"): "inbox",
    ("inbound", "failed"): "inbox",
    ("inbound", "delivered"): "inbox",
    ("outbound", "delivered"): "sent",
    ("outbound", "failed"): "error",
    ("outbound", "undelivered"): "error",
    ("outbound", "sent"): "sent",
    ("outbound", "delivery_failed"): "error",
    ("outbound", "unsent"): "draft",
    ("outbound", "queued"): "draft",
    ("outbound", "unreachable"): "error",
    ("outbound", "sending_failed"): "error",
}


def get_user_email(ctm_activity) -> str | None:
    if ctm_activity.get("business_number") in RHQ_NUMBERS:
        return RHQ

    global caller_number_to_user_email  # Access the global mapping
    caller_number = ctm_activity["caller_number"]
    direction = ctm_activity["direction"]

    agent = ctm_activity.get("agent")
    if agent:
        agent_email = agent["email"] if agent["email"] != CD_OLD else CD_NEW
        caller_number_to_user_email[caller_number] = agent_email
        return agent_email

    if direction == "msg_outbound":
        # Exception for outbound text messages part of a Chat
        if ctm_activity.get("parent_id") == "MSGA":
            return CM

        message_body = ctm_activity.get("message_body", "")

        # Filter out automated messages
        if any(message_body.startswith(phrase) for phrase in automated_message_starts):
            return None

        for phrase, email in message_phrase_to_agent_email.items():
            if phrase.lower() in message_body.lower():
                caller_number_to_user_email[caller_number] = email
                return email

        return None

    # Return the previous agent email, if any, for msg_inbound, inbound, outbound
    # Outbound calls should have assigned agents, but there are few error cases
    # Inbound text messages that end up here are
    previous_agent_email = caller_number_to_user_email.get(caller_number)

    if not previous_agent_email and direction == "inbound":
        # Allocate no answer, hangup, canceled calls to admin
        return ADMIN

    return previous_agent_email


def convert_to_close_call(
    activity, user_id: str, lead_id: str, contact_id: str
) -> tuple[str, dict[str, str]]:
    status_and_disposition = call_status_disposition_map.get(activity["status"])
    if status_and_disposition is None:
        raise ValueError(f"Unexpected call activity status: '{activity['status']}'")

    status, disposition = status_and_disposition
    if status == "no-answer" and activity["talk_time"] > 0:
        status = "completed"
        disposition = "vm-left"

    dt = datetime.utcfromtimestamp(activity["unix_time"]).isoformat()

    return "activity/call", {
        "lead_id": lead_id,
        "contact_id": contact_id,
        "user_id": user_id,
        "direction": activity["direction"],
        "status": status,
        "disposition": disposition,
        "phone": activity["caller_number"],
        "duration": activity["talk_time"],
        "local_phone": activity["tracking_number"],
        "note": activity.get("notes"),
        "activity_at": dt,
        "date_created": dt,
    }


def convert_to_close_sms(
    activity, user_id: str, lead_id: str, contact_id: str
) -> tuple[str, dict[str, str]]:
    direction = activity["direction"].removeprefix("msg_")
    status = sms_status_map.get((direction, activity["status"]))
    if status is None:
        raise ValueError(
            f"Unexpected SMS activity status: '{activity['status']}' ({activity['direction']})"
        )

    dt = datetime.utcfromtimestamp(activity["unix_time"]).isoformat()

    return "activity/sms", {
        "lead_id": lead_id,
        "contact_id": contact_id,
        "user_id": user_id,
        "remote_phone": activity["caller_number"],
        "local_phone": activity["tracking_number"],
        "direction": direction,
        "status": status,
        "text": activity["message_body"],
        "activity_at": dt,
        "date_created": dt,
    }


converter_map = {
    "outbound": convert_to_close_call,
    "inbound": convert_to_close_call,
    "msg_inbound": convert_to_close_sms,
    "msg_outbound": convert_to_close_sms,
    "chat": None,
}


async def ctm_activity_to_close_activity(
    ctm_activity,
    email_to_user_ids: dict[str, str],
    phone_to_lead_and_contact_ids: dict[str, str],
):
    # Filter out unsupported activity types, e.g., fax
    direction = ctm_activity["direction"]
    converter = converter_map.get(direction)
    if converter is None:
        return None, FailureReason("Direction Not Applicable", direction)

    # TODO: EXCLUDE DRAFT UNSENT SMS?

    # Filter out calls forwarded to Close
    business_number = ctm_activity.get("business_number")
    if business_number in CLOSE_NUMBERS:
        return None, FailureReason("Forwarded to Close", business_number)

    # Get contact
    caller_number = ctm_activity["caller_number"]
    lead_and_contact_id = phone_to_lead_and_contact_ids.get(caller_number)
    if lead_and_contact_id is None:
        return None, FailureReason("No Associated Contact", caller_number)
    lead_id, contact_id = lead_and_contact_id

    # Get user
    user_email = get_user_email(ctm_activity)
    if user_email is None:
        # Two msg_inbound cases are test messages.
        # 144 msg_outbound cases are automated
        if ctm_activity["status"] == "answered" or direction == "outbound":
            print(
                f"ANSWERED OR OUTBOUND: {ctm_activity['id']} {caller_number} {ctm_activity['called_at']} {ctm_activity['status']} {direction}"
            )
        return None, FailureReason("No User Email", direction)

    # Get Close user ID
    user_id = email_to_user_ids.get(user_email)
    if user_id is None:
        return None, FailureReason("No User ID", user_email)

    close_activity = converter(ctm_activity, user_id, lead_id, contact_id)
    return close_activity, None


def print_failure_reasons_and_save_details_to_csv(failure_counts, csv_file_path):
    # Calculate total counts for each reason
    total_counts = {
        reason: sum(details.values()) for reason, details in failure_counts.items()
    }

    # Sort reasons by total count in descending order
    sorted_reasons = sorted(
        failure_counts.keys(), key=lambda r: total_counts[r], reverse=True
    )

    # Print only the top-level failure reasons
    for reason in sorted_reasons:
        print(f"Failure Reason: {reason} (Total Count: {total_counts[reason]})")

    # Save details and counts to a CSV file
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["reason", "detail", "count"])  # Write header

        for reason in sorted_reasons:
            sorted_details = sorted(
                failure_counts[reason].items(), key=lambda item: item[1], reverse=True
            )
            for detail, count in sorted_details:
                writer.writerow([reason, detail, count])


async def main():
    activities = read_json_file(args.data_path)
    if not activities:
        logging.info("No activities")
        return

    if args.verbose:
        logging.info("Fetching Close contacts...")
    email_to_user_ids = close.get_user_ids_by_email()
    phone_to_lead_and_contact_ids = close.get_lead_and_contact_ids_by_phone()
    if args.verbose:
        logging.info(
            f"{len(email_to_user_ids)} emails, {len(phone_to_lead_and_contact_ids)} phone numbers"
        )

    if args.verbose:
        logging.info(
            f"Preparing {len(activities)} CallTrackingMetrics activities for sync..."
        )

    close_activities = []
    unsynced_ctm_activities = []
    failure_counts = defaultdict(lambda: defaultdict(int))

    for activity in activities:
        close_activity, failure = await ctm_activity_to_close_activity(
            activity, email_to_user_ids, phone_to_lead_and_contact_ids
        )
        if close_activity:
            close_activities.append(close_activity)
        else:
            unsynced_ctm_activities.append(activity)

        if failure:
            failure_counts[failure.reason][failure.details] += 1

    if close_activities:
        logging.info(f"Syncing {len(close_activities)} activities...")
        created_activites, failed_activities = await close.post_all(
            close_activities, verbose=args.verbose
        )
        if created_activites:
            logging.info(f"Synced {len(created_activites)} activities.")
            with open(
                f"output/activites_created_from_calltrackingmetrics-{args.env}.json",
                "w",
            ) as f:
                json.dump(created_activites, f)

        if failed_activities:
            logging.info(
                f"{len(failed_activities)} CallTrackingMetrics activities could not be posted."
            )
            with open(
                f"output/unsynced_ctm_activities-failed-{args.env}.json",
                "w",
            ) as f:
                json.dump(failed_activities, f)
    else:
        logging.info("No activites were created.")

    if unsynced_ctm_activities:
        logging.info(
            f"{len(unsynced_ctm_activities)} CallTrackingMetrics activities not synced."
        )
        with open(
            f"output/unsynced_ctm_activities-{args.env}.json",
            "w",
        ) as f:
            json.dump(unsynced_ctm_activities, f)
    else:
        logging.info("All CallTrackingMetrics activites were synced to Close.")

    print_failure_reasons_and_save_details_to_csv(
        failure_counts,
        f"output/calltrackingmetrics_sync_failure_reasons-{args.env}.csv",
    )


if __name__ == "__main__":
    asyncio.run(main())
