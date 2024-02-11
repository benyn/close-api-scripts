def get_full_name(first_name, last_name):
    if not first_name and not last_name:
        return None
    return f"{first_name or ''} {last_name or ''}".strip()


def convert_utc_z_to_offset_format(timestamp):
    return timestamp.replace("Z", "+00:00")
