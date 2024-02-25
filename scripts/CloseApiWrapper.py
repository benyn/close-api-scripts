import asyncio
from typing import Any, cast
from closeio_api import APIError, Client, ValidationError


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

    def get_lead_and_contact_ids_by_phone(self):
        contacts = self.get_all("contact", params={"_fields": "id,phones"})
        return {
            phone["phone"]: (contact["lead_id"], contact["id"])
            for contact in contacts
            for phone in contact["phones"]
        }

    def get_user(self, user_identifier):
        if user_identifier.startswith("user_"):
            try:
                return self.get(f"user/{user_identifier}")
            except APIError as e:
                print(f"Failed to fetch user with {user_identifier} because {e}")
                return None

        users = self.get_all("user")
        if "@" in user_identifier:
            return next(iter(u for u in users if u["email"] == user_identifier), None)
        else:
            return next(
                iter(
                    u
                    for u in users
                    if user_identifier.startswith(u["first_name"])
                    and user_identifier.endswith(u["last_name"])
                ),
                None,
            )

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

    def get_custom_activity_type_id(self, name: str) -> str | None:
        custom_activity_types = self.get_all(
            "custom_activity", params={"_fields": "id,name"}
        )
        for cat in custom_activity_types:
            if cat["name"].lower() == name.lower():
                return cat["id"]
        return None

    def get_custom_field_id(self, object_type: str, name: str) -> str | None:
        custom_fields = self.get(
            f"custom_field/{object_type}", params={"_fields": "id,name"}
        )["data"]
        return next(
            (cf["id"] for cf in custom_fields if cf["name"].lower() == name.lower()),
            None,
        )

    def get_custom_field_id_name_mapping(self, object_type: str) -> dict[str, str]:
        schema = self.get(f"custom_field_schema/{object_type}")
        mapping = {field["id"]: field["name"] for field in schema["fields"]}
        return mapping

    def get_all(self, url, params=None):
        if params is None:
            params = {}

        items = []
        has_more = True
        offset = 0
        while has_more:
            params["_skip"] = offset
            resp = self.get(url, params=params)
            items.extend(resp["data"])
            offset += len(resp["data"])
            has_more = resp.get("has_more")

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

    def create_has_related_custom_activity_query(self, custom_activity_type_id: str):
        return {
            "type": "has_related",
            "this_object_type": "lead",
            "related_object_type": "activity.custom_activity",
            "related_query": {
                "type": "and",
                "queries": [
                    {
                        "type": "field_condition",
                        "field": {
                            "type": "regular_field",
                            "object_type": "activity.custom_activity",
                            "field_name": "custom_activity_type_id",
                        },
                        "condition": {
                            "type": "term",
                            "values": [custom_activity_type_id],
                        },
                    },
                    {"type": "match_all"},
                ],
            },
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

    async def _dispatch_async(self, method_name: str, endpoint: str, data=None):
        return await asyncio.to_thread(self._dispatch, method_name, endpoint, data=data)

    async def _dispatch_slice(
        self,
        method_name: str,
        endpoint_and_data_slice: list[tuple[str, dict[str, Any]]],
    ) -> list[Any]:
        tasks = [
            self._dispatch_async(method_name, endpoint, data=data)
            for endpoint, data in endpoint_and_data_slice
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _dispatch_all(
        self,
        method_name: str,
        endpoint_and_data_list: list[tuple[str, dict[str, Any]]],
        slice_size: int,
        verbose: bool,
    ) -> tuple[list[Any], list[Any]]:
        successful_items = []
        failed_items = []
        for i in range(0, len(endpoint_and_data_list), slice_size):
            slice = endpoint_and_data_list[i : i + slice_size]
            if verbose:
                print(f"Processing items {i} through {i + len(slice) - 1}...")

            results = await self._dispatch_slice(method_name, slice)

            for idx, result in enumerate(results):
                if not isinstance(result, Exception):
                    successful_items.append(result)
                elif isinstance(result, ValidationError):
                    failed_items.append(
                        {
                            "error_type": "validation_error",
                            "errors": result.errors,
                            "field_errors": result.field_errors,
                            "data": slice[idx],
                        }
                    )
                else:
                    print(f"Task {idx} raised an exception: {result}")
                    failed_items.append(result)

        return successful_items, failed_items

    async def post_async(self, endpoint: str, data: dict[str, Any]):
        return await asyncio.to_thread(self.post, endpoint, data)

    async def put_async(self, endpoint: str, data: dict[str, Any]):
        return await asyncio.to_thread(self.put, endpoint, data)

    async def delete_async(self, endpoint: str):
        return await asyncio.to_thread(self.delete, endpoint)

    async def post_all(
        self,
        endpoint_and_data_list: list[tuple[str, dict[str, Any]]],
        slice_size: int = 5,
        verbose: bool = False,
    ) -> tuple[list[Any], list[Any]]:
        return await self._dispatch_all(
            "post",
            endpoint_and_data_list,
            slice_size,
            verbose,
        )

    async def put_all(
        self,
        endpoint_and_data_list: list[tuple[str, dict[str, Any]]],
        slice_size: int = 5,
        verbose: bool = False,
    ) -> tuple[list[Any], list[Any]]:
        return await self._dispatch_all(
            "put",
            endpoint_and_data_list,
            slice_size,
            verbose,
        )

    async def delete_all(
        self, endpoints: list[str], slice_size: int = 5, verbose: bool = False
    ) -> tuple[list[Any], list[Any]]:
        endpoint_and_data_list = [(endpoint, None) for endpoint in endpoints]
        return await self._dispatch_all(
            "delete", endpoint_and_data_list, slice_size, verbose
        )

    async def get_all_async(
        self, endpoint: str, params: dict[str, str] = None
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.get_all, endpoint, params)

    async def get_all_slice(
        self,
        endpoint_and_params_slice: list[tuple[str, dict[str, Any]]],
    ) -> list[Any]:
        tasks = [
            self.get_all_async(endpoint, params=params)
            for endpoint, params in endpoint_and_params_slice
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def get_all_concurrently(
        self,
        endpoint_and_params_list: list[tuple[str, dict[str, Any]]],
        slice_size: int = 10,
        verbose: bool = False,
    ) -> tuple[list[Any], list[Any]]:
        items = []
        errors = []
        for i in range(0, len(endpoint_and_params_list), slice_size):
            slice = endpoint_and_params_list[i : i + slice_size]
            if verbose:
                print(f"Getting items {i} through {i + len(slice) - 1}...")

            results = await self.get_all_slice(slice)

            for idx, result in enumerate(results):
                if not isinstance(result, Exception):
                    items.extend(result)
                else:
                    print(f"Task {idx} raised an exception: {result}")
                    errors.append(result)

        return items, errors

    async def get_custom_activity_instances(
        self,
        custom_activity_type_id: str,
        date_created_start: str = None,
        date_created_end: str = None,
    ):
        query = self.create_has_related_custom_activity_query(custom_activity_type_id)
        leads = self.search(query)
        print(f"{len(leads)} leads")
        endpoint_and_params_list = [
            (
                "activity/custom",
                {
                    "lead_id": lead["id"],
                    "custom_activity_type_id": custom_activity_type_id,
                    "date_created__gt": date_created_start,
                    "date_created__lt": date_created_end,
                },
            )
            for lead in leads
        ]
        custom_activity_instances, errors = await self.get_all_concurrently(
            endpoint_and_params_list, verbose=True
        )
        print(f"{len(custom_activity_instances)} custom activity instances")
        if errors:
            print(f"{len(errors)} errors")

        custom_activity_instances.sort(key=lambda x: x["date_created"])
        return custom_activity_instances
