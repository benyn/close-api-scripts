from typing import Any, cast
from closeio_api import Client


class CloseApiWrapper(Client):
    """
    Close API wrapper that makes it easier to paginate through resources and get all items
    with a single function call alongside some convenience functions (e.g. getting all lead statuses).
    """

    def __init__(
        self, api_key=None, tz_offset=None, max_retries=5, development=False
    ):
        super().__init__(
            api_key=api_key,
            tz_offset=tz_offset,
            max_retries=max_retries,
            development=development,
        )

    def get_lead_statuses(self):
        organization_id = self.get('me')['organizations'][0]['id']
        return self.get(
            f"organization/{organization_id}",
            params={"_fields": "lead_statuses"},
        )["lead_statuses"]

    def get_opportunity_pipelines(self):
        organization_id = self.get('me')['organizations'][0]['id']
        return self.get(
            f"organization/{organization_id}",
            params={"_fields": "pipelines"},
        )["pipelines"]

    def get_custom_fields(self, type):
        return self.get(f"custom_field_schema/{type}")["fields"]

    def get_opportunity_statuses(self):
        organization_id = self.get('me')['organizations'][0]['id']
        pipelines = self.get(
            f"organization/{organization_id}",
            params={"_fields": "pipelines"},
        )["pipelines"]

        opportunity_statuses = []
        for pipeline in pipelines:
            opportunity_statuses.extend(pipeline['statuses'])

        return opportunity_statuses

    def get_user_ids_by_email(self):
        users = self.get("user", params={"_fields": "id,email"})
        return {user["email"]: user["id"] for user in users["data"]}

    def get_user_ids_by_group(self, group_name_prefix=None):
        groups = self.get("group", params={"_fields": "id,name"})["data"]
        return {
            member["user_id"]
            for group in groups
            if group_name_prefix is None or group["name"].startswith(group_name_prefix)
            for member in self.get(
                f"group/{group['id']}", params={"_fields": "id,name,members"}
            )["members"]
        }

    def get_custom_field_id(self, object_type: str, name: str) -> str | None:
        custom_fields = self.get(
            f"custom_field/{object_type}", params={"_fields": "id,name"}
        )["data"]
        return next(
            (cf["id"] for cf in custom_fields if cf["name"].lower() == name.lower()),
            None,
        )

    def get_all(self, url, params=None):
        if params is None:
            params = {}

        items = []
        has_more = True
        offset = 0
        while has_more:
            params["_skip"] = offset
            resp = self.get(url, params=params)
            items.extend(resp['data'])
            offset += len(resp["data"])
            has_more = resp["has_more"]

        return items

    def search(
        self,
        query: dict[str, Any],
        sort: list[dict[str, str | dict[str, str]]] | None = None,
        results_limit: int | None = None,
        fields: list[str] | None = None,
        limit: int | None = None,
        object_type: str | None = None,
    ) -> list[dict[str, Any]]:
        if object_type is None:
            object_type = "lead"

        if sort is None:
            sort = []

        if fields is not None:
            fields_with_obj_type = {object_type: fields}
        else:
            fields_with_obj_type = None

        payload = {
            "query": {
                "type": "and",
                "queries": [
                    {"type": "object_type", "object_type": object_type},
                    query,
                ],
            },
            "sort": sort,
            "results_limit": results_limit,
            "_fields": fields_with_obj_type,
            "_limit": limit,
            "cursor": None,
        }

        data = []
        has_more = True
        while has_more:
            resp = self.post("data/search", data=payload)
            data.extend(resp["data"])
            payload["cursor"] = resp["cursor"]
            has_more = bool(resp["cursor"])

        return data

    def count(self, query, object_type: str | None = None) -> int:
        if object_type is None:
            object_type = "lead"

        payload = {
            "query": {
                "type": "and",
                "queries": [
                    {"type": "object_type", "object_type": object_type},
                    query,
                ],
            },
            "include_counts": True,
            "results_limit": 0,
        }

        resp = self.post("data/search", data=payload)
        return cast(int, resp["count"]["total"])

    def create_contact_email_query(
        self, email: str
    ) -> dict[str, str | dict[str, str | dict[str, str]]]:
        return {
            "type": "has_related",
            "this_object_type": "contact",
            "related_object_type": "contact_email",
            "related_query": {
                "type": "field_condition",
                "field": {
                    "type": "regular_field",
                    "object_type": "contact_email",
                    "field_name": "email",
                },
                "condition": {
                    "type": "text",
                    "mode": "exact_value",
                    "value": email.lower(),
                },
            },
        }

    def create_lead_email_query(
        self, email: str
    ) -> dict[str, str | dict[str, str | dict[str, str | dict[str, str]]]]:
        return {
            "type": "has_related",
            "this_object_type": "lead",
            "related_object_type": "contact",
            "related_query": self.create_contact_email_query(email),
        }

    def search_leads_by_email(
        self, email: str, results_limit: int | None = None
    ) -> list[dict[str, Any]]:
        return self.search(
            self.create_lead_email_query(email),
            results_limit=results_limit,
        )

    async def find_lead_by_email(
        self, email: str, fields: list[str] | None = None
    ) -> dict[str, Any] | None:
        query = self.create_lead_email_query(email)
        leads = await asyncio.to_thread(
            self.search, query, results_limit=1, fields=fields
        )
        return leads[0] if leads else None

    def find_contact_by_email(self, email: str):
        contacts = self.search(
            self.create_contact_email_query(email),
            fields=["id", "emails"],
            results_limit=1,
            object_type="contact",
        )
        return contacts[0] if contacts else None

    def email_exists(self, email: str) -> bool:
        return self.count(self.create_lead_email_query(email)) > 0

    async def update_opportunity(self, id: str, data: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self.put, f"opportunity/{id}", data=data)
