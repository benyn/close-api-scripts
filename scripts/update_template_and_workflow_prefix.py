import argparse
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


parser = argparse.ArgumentParser(description="Updates Template and Workflow Prefix.")
parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
parser.add_argument("--from-prefix", "-f", required=True, help="Current Prefix")
parser.add_argument("--to-prefix", "-t", required=True, help="New Prefix")
args = parser.parse_args()

api_key = get_api_key("api.close.com", f"admin_{args.env}")
api = CloseApiWrapper(api_key)


old_prefix = f"{args.from_prefix}:"
new_prefix = f"{args.to_prefix}:"


def update_item_prefix(item_type):
    items = api.get_all_items(item_type, params={"_fields": "id,name"})
    for item in items:
        old_name = item["name"]
        if old_name.startswith(old_prefix):
            new_name = new_prefix + old_name[len(old_prefix) :]
            api.put(f"{item_type}/{item['id']}", data={"name": new_name})
            print(f"[{item_type}] {old_name} -> {new_name}")


update_item_prefix("email_template")
update_item_prefix("sms_template")
update_item_prefix("sequence")
