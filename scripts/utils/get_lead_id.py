def get_lead_id(input: str) -> str:
    if input.startswith("lead_"):
        return input
    if input.startswith("https://app.close.com/lead/") and input.endswith("/"):
        return input.split("/")[-2]
    else:
        raise ValueError("Invalid input format for lead ID.")


def get_lead_and_activity_id(input: str) -> tuple[str, str]:
    if not input.startswith("https://app.close.com/lead/"):
        ("Invalid input format for activity URL.")

    parts = input.split("#")
    if len(parts) != 2 or not parts[1].startswith("activityId="):
        raise ValueError("URL must contain an activity ID.")

    lead_id = parts[0].split("/")[-2]
    activity_id = parts[1].split("=")[1]

    if not lead_id.startswith("lead_"):
        raise ValueError("Invalid lead ID format.")

    return lead_id, activity_id
