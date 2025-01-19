import argparse
import asyncio
import json
from dataclasses import dataclass

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key


@dataclass
class CustomFieldUpdate:
    env: str
    object_type: str
    field_name: str
    old_value: str
    new_value: str


def parse_arguments():
    parser = argparse.ArgumentParser(description="Update custom field values in Close.")
    parser.add_argument(
        "env", choices=["dev", "prod"], help="Target environment (dev/prod)"
    )
    parser.add_argument(
        "object_type",
        choices=["lead", "contact", "opportunity"],
        help="Type of object containing the custom field",
    )
    parser.add_argument("field_name", help="Name of the custom field to update")
    parser.add_argument("old_value", help="Current value to be replaced")
    parser.add_argument("new_value", help="New value to replace with")

    return parser.parse_args()


def build_custom_field_query(custom_field_id: str, old_value: str) -> dict:
    """Build an advanced filtering query to find objects with specific custom field value"""
    return {
        "type": "field_condition",
        "field": {
            "type": "custom_field",
            "custom_field_id": custom_field_id,
        },
        "condition": {
            "type": "text",
            "mode": "exact_value",
            "value": old_value,
        },
    }


async def update_custom_field_values(
    client: CloseApiWrapper, update_info: CustomFieldUpdate
) -> tuple[list[dict], list[dict]]:
    """Update all instances of a custom field value from old to new"""

    # Get the custom field ID
    custom_field_id = client.get_custom_field_id(
        update_info.object_type, update_info.field_name
    )
    if not custom_field_id:
        raise ValueError(
            f"Custom field '{update_info.field_name}' not found for {update_info.object_type} objects"
        )

    # Use advanced filtering to find all objects with the old value
    query = build_custom_field_query(custom_field_id, update_info.old_value)
    objects = client.search(query, object_type=update_info.object_type)

    if not objects:
        print(
            f"No {update_info.object_type}s found with '{update_info.old_value}' in {update_info.field_name}"
        )
        return [], []

    print(f"Found {len(objects)} {update_info.object_type}s to update")

    # Prepare update requests
    endpoint_and_data_list = [
        (
            f"{update_info.object_type}/{obj['id']}",
            {f"custom.{custom_field_id}": update_info.new_value},
        )
        for obj in objects
    ]

    # Update objects concurrently
    successful, failed = await client.put_all(
        endpoint_and_data_list, slice_size=10, verbose=True
    )

    return successful, failed


async def main(update_info: CustomFieldUpdate) -> None:
    api_key = get_api_key("api.close.com", f"admin_{update_info.env}")
    client = CloseApiWrapper(api_key)

    successful, failed = await update_custom_field_values(client, update_info)

    # Print results
    if successful:
        print(f"\nSuccessfully updated {len(successful)} {update_info.object_type}s")
        with open(
            f"output/{update_info.object_type}s_updated_with_{update_info.field_name}-{args.env}.json",
            "w",
        ) as f:
            json.dump(successful, f)
    else:
        print(f"No {update_info.object_type}s were updated")

    if failed:
        print(f"Failed to update {len(failed)} {update_info.object_type}s:")
        with open(
            f"output/{update_info.object_type}s_unchanged_with_{update_info.field_name}-{args.env}.json",
            "w",
        ) as f:
            json.dump(failed, f)


if __name__ == "__main__":
    args = parse_arguments()

    update_info = CustomFieldUpdate(
        env=args.env,
        object_type=args.object_type,
        field_name=args.field_name,
        old_value=args.old_value,
        new_value=args.new_value,
    )

    asyncio.run(main(update_info))
