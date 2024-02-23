import re


def get_full_name(first_name, last_name):
    if not first_name and not last_name:
        return None
    return f"{first_name or ''} {last_name or ''}".strip()


def convert_utc_z_to_offset_format(timestamp):
    return timestamp.replace("Z", "+00:00")


def format_phone_number(phone_number):
    if not phone_number:
        return phone_number

    standardized = re.sub(r"[^\d+*#,;x]", "", phone_number)
    standardized = re.sub(r"[;,#*]+(\d+)", r"x\1", standardized)
    standardized = re.sub(r"[*#,;]", "", standardized)

    if not standardized.startswith("+"):
        if (
            len(standardized) == 10
            and "2" <= standardized[0] <= "9"
            and "0" <= standardized[1] <= "8"
        ):
            standardized = "+1" + standardized
        elif (
            len(standardized) == 11
            and standardized.startswith("1")
            and "2" <= standardized[1] <= "9"
            and "0" <= standardized[2] <= "8"
        ):
            standardized = "+" + standardized

    return standardized
