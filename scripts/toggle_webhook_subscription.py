import argparse
import asyncio

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List and toggle webhook subscription statuses"
    )
    parser.add_argument(
        "env", choices=["dev", "prod"], help="Target environment (dev/prod)"
    )
    return parser.parse_args()


def get_sorted_webhooks(api: CloseApiWrapper) -> list[dict]:
    webhooks = api.get_all("webhook")
    webhooks = sorted(webhooks, key=lambda x: x["url"].lower())
    return webhooks


def display_webhooks(webhooks: list[dict]) -> None:
    print("\nWebhook Subscriptions:")
    print("-" * 80)
    for i, webhook in enumerate(webhooks, 1):
        status_color = "\033[92m" if webhook["status"] == "active" else "\033[91m"
        print(
            f"{i}. {status_color}{webhook['status']}\033[0m - {webhook['url']} "
            f"({len(webhook['events'])} events)"
        )
    print("-" * 80)


def get_webhook_selections() -> list[int]:
    while True:
        try:
            input_str = input(
                "\nEnter webhook numbers to toggle (comma-separated) or 'all', or press Enter to exit: "
            ).strip()

            if not input_str:
                return []

            if input_str.lower() == "all":
                return [-1]  # Special value to indicate all webhooks

            selections = [
                int(num.strip()) for num in input_str.split(",") if num.strip()
            ]
            return selections

        except ValueError:
            print("Invalid input. Please enter comma-separated numbers or 'all'")


async def toggle_webhook_statuses(
    api: CloseApiWrapper, webhooks_to_toggle: list[tuple[str, str]]
) -> None:
    endpoint_and_data_list = [
        (
            f"webhook/{webhook_id}/",
            {"status": "paused" if current_status == "active" else "active"},
        )
        for webhook_id, current_status in webhooks_to_toggle
    ]

    successful, failed = await api.put_all(
        endpoint_and_data_list, slice_size=5, verbose=True
    )

    print(f"\nSuccessfully toggled {len(successful)} webhooks")
    if failed:
        print(f"Failed to toggle {len(failed)} webhooks:")
        for failure in failed:
            print(f"  - {failure}")


async def main() -> None:
    args = parse_arguments()
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
    api = CloseApiWrapper(api_key)

    # Get all webhook subscriptions
    webhooks = get_sorted_webhooks(api)
    if not webhooks:
        print("No webhook subscriptions found")
        return

    while True:
        display_webhooks(webhooks)
        selections = get_webhook_selections()

        if not selections:
            break

        webhooks_to_toggle = []
        if selections == [-1]:  # All webhooks selected
            webhooks_to_toggle = [(w["id"], w["status"]) for w in webhooks]
        else:
            try:
                webhooks_to_toggle = [
                    (webhooks[i - 1]["id"], webhooks[i - 1]["status"])
                    for i in selections
                    if 1 <= i <= len(webhooks)
                ]
            except IndexError:
                print(
                    f"Invalid selection. Please enter numbers between 1 and {len(webhooks)}"
                )
                continue

        if webhooks_to_toggle:
            await toggle_webhook_statuses(api, webhooks_to_toggle)
            # Refresh webhooks list after toggling
            webhooks = get_sorted_webhooks(api)


if __name__ == "__main__":
    asyncio.run(main())
