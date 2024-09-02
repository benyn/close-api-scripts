import argparse
import asyncio

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key


def parse_arguments():
    parser = argparse.ArgumentParser(description="Update workflow schedules in Close.")
    parser.add_argument(
        "env", choices=["dev", "prod"], help="Target environment (dev/prod)"
    )
    parser.add_argument(
        "weekdays", help="Comma-separated list of weekdays to update (e.g., Mon, Tue)"
    )
    parser.add_argument("action", choices=["on", "off"], help="Turn weekday on or off")
    parser.add_argument(
        "--include", help="Comma-separated list of sequences to include"
    )
    parser.add_argument(
        "--exclude", help="Comma-separated list of sequences to exclude"
    )
    return parser.parse_args()


def weekday_to_int(weekday: str) -> int | None:
    weekdays = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
    return weekdays.get(weekday)


def update_schedule(
    schedule: dict | None, weekdays: list[int], turn_on: bool
) -> dict | None:
    if not schedule or "ranges" not in schedule:
        return None

    ranges = schedule["ranges"]
    updated = False

    if turn_on:
        for weekday in weekdays:
            if not any(r["weekday"] == weekday for r in ranges):
                # TODO: Handle cases where `ranges` is empty
                first_range = ranges[0]
                new_range = {
                    "weekday": weekday,
                    "start": first_range["start"],
                    "end": first_range["end"],
                }
                ranges.append(new_range)
                updated = True
    else:
        new_ranges = [r for r in ranges if r["weekday"] not in weekdays]
        if len(new_ranges) != len(ranges):
            ranges = new_ranges
            updated = True

    if not updated:
        return None

    return {"ranges": sorted(ranges, key=lambda x: x["weekday"])}


async def update_sequence(
    client: CloseApiWrapper, sequence: dict, weekday: int, turn_on: bool
) -> dict | None:
    updated_schedule = update_schedule(sequence.get("schedule"), weekday, turn_on)
    if updated_schedule:
        return await client.put_async(
            f"sequence/{sequence["id"]}", {"schedule": updated_schedule}
        )
    else:
        print(f"Not updated: {sequence["name"]}")


async def update_sequence_schedules(
    env: str,
    weekdays: list[str],
    turn_on: bool,
    include_sequences: list[str] | None = None,
    exclude_sequences: list[str] | None = None,
) -> None:
    api_key = get_api_key("api.close.com", f"admin_{env}")
    client = CloseApiWrapper(api_key)
    weekday_ints = [weekday_to_int(day) for day in weekdays]
    if None in weekday_ints:
        invalid_days = [
            day for day, int_val in zip(weekdays, weekday_ints) if int_val is None
        ]
        print(f"Invalid weekday(s): {', '.join(invalid_days)}")
        return

    sequences = await client.get_all_async("sequence")
    sequences_to_update = [
        sequence
        for sequence in sequences
        if sequence["status"] == "active"
        and (not include_sequences or sequence["name"] in include_sequences)
        and (not exclude_sequences or sequence["name"] not in exclude_sequences)
    ]

    print(f"Updating {len(sequences_to_update)} sequences...")

    tasks = [
        update_sequence(client, sequence, weekday_ints, turn_on)
        for sequence in sequences_to_update
    ]
    processed_sequences = await asyncio.gather(*tasks)
    updated_sequences = [
        sequence for sequence in processed_sequences if sequence is not None
    ]

    print(f"{len(updated_sequences)} sequence schedules updated successfully.")


if __name__ == "__main__":
    args = parse_arguments()

    env = args.env
    weekdays = args.weekdays.split(",")
    turn_on = args.action == "on"
    include_sequences = args.include.split(",") if args.include else None
    exclude_sequences = args.exclude.split(",") if args.exclude else None

    asyncio.run(
        update_sequence_schedules(
            env, weekdays, turn_on, include_sequences, exclude_sequences
        )
    )
