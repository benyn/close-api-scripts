import asyncio
from datetime import datetime
from typing import cast
from aiohttp import ClientSession
from basecrm import Client


class ZendeskApiWrapper(Client):
    """
    Zendesk API wrapper that makes it easier to paginate through resources and get all items
    with a single function call alongside some convenience functions.
    """

    def __init__(self, **options):
        super().__init__(**options)
        self.headers = {
            "Authorization": f"Bearer {self.config.access_token}",
            "Content-Type": "application/json",
        }
        self.records_per_page = 200

    def get_user_ids_by_email(self) -> dict[str, str]:
        users = self.users.list()
        return {user["email"]: user["id"] for user in users}

    def get_email_address(self, resource_type: str, resource_id: int) -> str:
        if resource_type == "lead":
            lead = self.leads.retrieve(resource_id)
            if not lead:
                raise ValueError(f"Lead with ID {resource_id} not found")
            return lead["email"]

        if resource_type == "contact":
            contact = self.contacts.retrieve(resource_id)
            if not contact:
                raise ValueError(f"Contact with ID {resource_id} not found")
            return contact["email"]

        elif resource_type == "deal":
            deal = self.deals.retrieve(resource_id)
            if not deal:
                raise ValueError(f"Deal with ID {resource_id} not found")
            if not deal["contact_id"]:
                raise ValueError(f"No contact associated with Deal ID {resource_id}")
            contact = self.contacts.retrieve(deal["contact_id"])
            if not contact:
                raise ValueError(
                    f"Contact associated with Deal ID {resource_id} not found"
                )
            return contact["email"]

        else:
            raise ValueError("Invalid resource type")

    def get_all(self, object_type: str, since_date: datetime | None = None):
        """
        Fetch objects (leads, contacts, deals, notes, tasks) updated after a certain date from Zendesk Sell.

        :param object_type: Type of the object to fetch (e.g., 'leads', 'contacts', 'deals', 'notes', 'tasks').
        :param since_date: The date to fetch objects from (YYYY-MM-DD format). Optional.
        :return: A list of objects.
        """

        # Mapping from object type to the corresponding method in the client
        method_mapping = {
            "leads": self.leads.list,
            "contacts": self.contacts.list,
            "deals": self.deals.list,
            "notes": self.notes.list,
            "tasks": self.tasks.list,
            "orders": self.orders.list,
        }

        if object_type not in method_mapping:
            raise ValueError("Invalid object type")

        # Fetch items
        items = []
        page = 1
        while True:
            # The default limit is 25 and maximum number that can be returned is 100.
            fetched_items = method_mapping[object_type](
                page=page, per_page=100, sort_by="updated_at:desc"
            )
            if not fetched_items:
                break

            if since_date is None:
                items.extend(fetched_items)
            else:
                # Filter items by updated_at date
                for item in fetched_items:
                    item_updated_at = datetime.fromisoformat(item.updated_at)
                    if item_updated_at <= since_date:
                        # All subsequent objects will be older, so we can break the loop
                        return items

                    items.append(item)

            page += 1

        return items

    async def get_custom_field_search_api_id(
        self, resource_type: str, name: str
    ) -> str | None:
        url = f"https://api.getbase.com/v3/{resource_type}/custom_fields"

        async with ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"API call failed: {await response.text()}")
                data = await response.json()
                custom_field_id = next(
                    (
                        cast(str, item["data"]["search_api_id"])
                        for item in data["items"]
                        if item["data"]["name"] == name
                    ),
                    None,
                )
                return custom_field_id

    async def get_contact_custom_field_search_api_id(self, name: str):
        return await self.get_custom_field_search_api_id("contacts", name)

    async def search(
        self,
        session: ClientSession,
        resource_type: str,
        query,
        hits: bool = None,
        page: int = None,
    ) -> dict:
        """
        Makes an asynchronous search API call to Zendesk Sell.

        Args:
        session (ClientSession): The aiohttp ClientSession object.
        resource_type (str): The type of resource to search. If None, searches all types.
        query (Any): The search query.
        hits (bool, optional): Whether to include hits in the response. Defaults to True.
        page (int, optional): The page number of the search results. Defaults to 0.

        Returns:
        dict: The JSON response from the API.
        """
        url = f"https://api.getbase.com/v3/{resource_type}/search"

        # Prepare payload
        if hits is None:
            hits = True

        if page is None:
            page = 0

        payload = {
            "items": [
                {
                    "data": {
                        "query": query,
                        "hits": hits,
                        "page": page,
                        "per_page": self.records_per_page,
                    }
                }
            ]
        }

        # Make the API call
        async with session.post(url, json=payload) as response:
            if response.status != 200:
                raise Exception(f"API call failed: {await response.text()}")
            return await response.json()

    async def count_contacts(self, session: ClientSession, query) -> int:
        result = await self.search(session, "contacts", query, hits=False)
        total_count = result["items"][0]["meta"]["total_count"]
        return total_count

    async def filter_contacts(self, attribute_names, filter_query):
        projection = [{"name": name} for name in attribute_names]
        query = {
            "projection": projection,
            "filter": {"and": [{"filter": filter_query}]},
        }
        async with ClientSession(headers=self.headers) as session:
            total_count = await self.count_contacts(session, query)
            page_count = (
                total_count + self.records_per_page - 1
            ) // self.records_per_page

            # Create a list of tasks for each page to be fetched
            tasks = [
                self.search(session, "contacts", query, page=page)
                for page in range(0, page_count)
            ]

            # Gather all tasks and wait for them to complete
            results = await asyncio.gather(*tasks)

            # Combine results from all pages
            combined_results = [
                item["data"]
                for result in results
                for item in result["items"][0]["items"]
            ]

            return combined_results

    async def get_contact(self, id: int):
        return await asyncio.to_thread(self.contacts.retrieve, id)

    async def get_deal(self, id: int):
        return await asyncio.to_thread(self.deals.retrieve, id)

    async def get_deal_and_associated_primary_contact(self, id: int):
        deal = await self.get_deal(id)
        contact = await self.get_contact(deal.contact_id) if deal.contact_id else None
        return (deal, contact)

    async def get_line_items(self, order_id: int):
        return await asyncio.to_thread(self.line_items.list, order_id)
