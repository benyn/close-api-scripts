import argparse
import asyncio

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Delete duplicate Custom Activity instances"
)
parser.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
parser.add_argument("--custom-activity-type", "-t", help="Custom Activity type")
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(api_key)


def find_duplicates(instances):
    healthie_appointment_id_field_id = close.get_prefixed_custom_field_id(
        "shared", "healthie_appointment_id"
    )
    if not healthie_appointment_id_field_id:
        print("healthie_appointment_id custom field not found")
        return []

    # Group instances by their healthie_appointment_id values
    grouped_instances = {}
    for instance in instances:
        # Assuming each instance is a dictionary and the healthie_appointment_id is stored under a key
        # that matches the healthie_appointment_id_field_id variable's value
        appointment_id = instance.get(healthie_appointment_id_field_id)
        if appointment_id:
            if appointment_id in grouped_instances:
                grouped_instances[appointment_id].append(instance)
            else:
                grouped_instances[appointment_id] = [instance]
        else:
            print(f"healthie_appointment_id not found in {instance['id']}")

    # Filter out non-duplicates and sort each group of duplicates by date_updated
    sorted_duplicates = [
        sorted(instances, key=lambda x: x["date_updated"])
        for instances in grouped_instances.values()
        if len(instances) > 1
    ]

    return sorted_duplicates


def get_delete_enpoints(duplicates: list[list[dict]]):
    endpoints = []
    for instances in duplicates:
        if args.verbose:
            print(f"{len(instances)} instances")
        sorted_instances = sorted(instances, key=lambda x: x["date_created"])
        # Keep all but the first instance for deletion
        for instance_to_delete in sorted_instances[1:]:
            delete_endpoint = f"/activity/custom/{instance_to_delete['id']}/"
            endpoints.append(delete_endpoint)
    return endpoints


async def main():
    custom_activity_type_id = close.get_custom_activity_type_id(
        args.custom_activity_type
    )
    if not custom_activity_type_id:
        raise ValueError(
            f"Custom Activity type '{args.custom_activity_type}' not found"
        )

    # TODO: Specify fields since this now returns bare minimum fields
    instances = await close.get_custom_activity_instances(custom_activity_type_id)
    duplicates = find_duplicates(instances)
    if args.verbose:
        print(
            f"{len(duplicates)} sets of duplicates found among {len(instances)} instances"
        )

    # Delete duplicates
    delete_endpoints = get_delete_enpoints(duplicates)
    await close.delete_all(delete_endpoints)
    print(f"Deleted {len(delete_endpoints)} Custom Activity instances")


if __name__ == "__main__":
    asyncio.run(main())
