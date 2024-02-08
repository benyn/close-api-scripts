from time import strptime
from basecrm import Client


class ZendeskApiWrapper(Client):
    """
    Zendesk API wrapper that makes it easier to paginate through resources and get all items
    with a single function call alongside some convenience functions.
    """

    def __init__(self, **options):
        super().__init__(**options)

    def get_user_ids_by_email(self):
        users = self.users.list()
        return {user["email"]: user["id"] for user in users}

    def get_all_items(self, object_type, since_date=None):
        """
        Fetch objects (leads, contacts, deals, notes, tasks) updated after a certain date from Zendesk Sell.

        :param api_token: Your Zendesk Sell API token.
        :param object_type: Type of the object to fetch (e.g., 'leads', 'contacts', 'deals', 'notes', 'tasks').
        :param since_date: The date to fetch objects from (YYYY-MM-DD format). Optional.
        :return: A list of objects.
        """

        if since_date:
            since_date = strptime(since_date, "%Y-%m-%d")

        # Mapping from object type to the corresponding method in the client
        method_mapping = {
            "leads": self.leads.list,
            "contacts": self.contacts.list,
            "deals": self.deals.list,
            "notes": self.notes.list,
            "tasks": self.tasks.list,
        }

        if object_type not in method_mapping:
            raise ValueError("Invalid object type")

        # Fetch items
        items = []
        page = 1
        while True:
            fetched_items = method_mapping[object_type](
                page=page, per_page=50, sort_by="updated_at:desc"
            )
            if not fetched_items:
                break

            if since_date is None:
                items.extend(fetched_items)
            else:
                # Filter items by updated_at date
                for item in fetched_items:
                    item_updated_at = strptime(item.updated_at, "%Y-%m-%dT%H:%M:%SZ")
                    if item_updated_at < since_date:
                        # All subsequent objects will be older, so we can break the loop
                        return items

                    items.append(item)

            page += 1

        return items
