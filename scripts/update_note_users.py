import argparse
import asyncio
import json

from closeio_api import APIError
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


# THIS SCRIPT HAS NEVER BEEN TESTED OR USED

arg_parser = argparse.ArgumentParser(description="Update Note user to match created_by")
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument(
    "--user",
    "-u",
    help="Use this field if you only want to find notes for a specific users - enter email, ID, or name",
)
arg_parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = arg_parser.parse_args()


close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
api = CloseApiWrapper(close_api_key)


async def update_note_user_id(note):
    if note["user_id"] == note["created_by"]:
        print(
            f"⚠️ creator: {note['created_by_name']}\tuser: {note['user_name']} ({note['note']})."
        )
        print("returning because nothing to update")
        return None, None

    if user_id and note["user_id"] != user_id:
        print("returning because user id")
        return None, None

    print(note)

    data = note.copy()
    del data["id"]
    data["user_id"] = note["created_by"]

    try:
        print(f"✅ {note['user_name']}\t{note['note']}")
        post_note = await api.post_async("activity/note", data=data)
        post_note["old_note_id"] = note["id"]
        await api.delete_async(f"activity/note/{note['id']}")
        if args.verbose:
            print(f"✅ {post_note['user_name']}\t{post_note['note']}")
        return post_note, None
    except APIError as e:
        note["error"] = e
        print(f"Failed to post or delete note because {str(e)} ({data['note']})")
        return None, note


async def update_notes(notes, slice_size: int = 5):
    updated_notes = []
    errored_notes = []

    async def process_slice(notes_slice):
        tasks = [update_note_user_id(note) for note in notes_slice]
        results = await asyncio.gather(*tasks)
        for post_note, error_note in results:
            if post_note:
                updated_notes.append(post_note)
            elif error_note:
                errored_notes.append(error_note)

    # Slice the list and process in batches of 5 to avoid API rate limits
    for i in range(0, len(notes), slice_size):
        notes_slice = notes[i : i + slice_size]
        await process_slice(notes_slice)

    return updated_notes, errored_notes


user = api.get_user(args.user) if args.user else None
user_id = user["id"] if user else None

notes = api.get_all(
    "activity/note",
    params={
        # "_fields": "id,created_by,created_by_name,user_id,user_name,note",
    },
)
if args.verbose:
    print(f"Scanning {len(notes)} notes...")


async def main():
    updated_notes, errored_notes = await update_notes(notes)
    if updated_notes:
        print(f"Updated {len(updated_notes)} out of {len(notes)} notes.")
        with open(f"output/notes_updated-{args.env}.json", "w") as f:
            json.dump(updated_notes, f)
    else:
        print("No leads were updated.")

    if errored_notes:
        print(f"{len(errored_notes)} leads could not be updated.")
        with open(f"output/notes_unchanged-{args.env}.json", "w") as f:
            json.dump(errored_notes, f)


if __name__ == "__main__":
    asyncio.run(main())
