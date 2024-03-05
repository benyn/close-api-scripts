def get_lead_id(input: str) -> str:
    if input.startswith("lead_"):
        return input
    if input.startswith("https://app.close.com/lead/") and input.endswith("/"):
        return input.split("/")[-2]
    else:
        raise ValueError("Invalid input format for lead ID.")
