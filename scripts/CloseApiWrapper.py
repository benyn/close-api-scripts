import asyncio
from datetime import date
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

    def get_id_name_mapping(
        self, endpoint: str, name_field_name: str = None
    ) -> dict[str, str]:
        name_field_name = name_field_name or "name"
        items = self.get(endpoint, params={"_fields": f"id,{name_field_name}"})["data"]
        return {item["id"]: item[name_field_name] for item in items}

    def get_name_id_mapping(
        self, endpoint: str, name_field_name: str = None
    ) -> dict[str, str]:
        name_field_name = name_field_name or "name"
        items = self.get(endpoint, params={"_fields": f"id,{name_field_name}"})["data"]
        return {item[name_field_name]: item["id"] for item in items}

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
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Invalid name. Name must be a non-empty string.")

        custom_activity_types = self.get_all(
            "custom_activity", params={"_fields": "id,name"}
        )
        for cat in custom_activity_types:
            if cat["name"].lower() == name.lower():
                return cat["id"]
        return None

    def get_custom_field_id(self, object_type: str, name: str) -> str | None:
        custom_fields = self.get(f"custom_field_schema/{object_type}")["fields"]
        return next(
            (cf["id"] for cf in custom_fields if cf["name"].lower() == name.lower()),
            None,
        )

    def get_prefixed_custom_field_id(self, object_type: str, name: str) -> str | None:
        custom_field_id = self.get_custom_field_id(object_type, name)
        return f"custom.{custom_field_id}" if custom_field_id else None

    def get_custom_field_id_name_mapping(self, object_type: str) -> dict[str, str]:
        schema = self.get(f"custom_field_schema/{object_type}")
        mapping = {field["id"]: field["name"] for field in schema["fields"]}
        return mapping

    def get_custom_field_name_prefixed_id_mapping(
        self, object_type: str
    ) -> dict[str, str]:
        schema = self.get(f"custom_field_schema/{object_type}")
        mapping = {field["name"]: f"custom.{field['id']}" for field in schema["fields"]}
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
        limit: int = 200,  # Maximum 200
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

    def build_contact_email_query(
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

    def build_lead_email_query(
        self, email: str
    ) -> dict[str, str | dict[str, str | dict[str, str | dict[str, str]]]]:
        return {
            "type": "has_related",
            "this_object_type": "lead",
            "related_object_type": "contact",
            "related_query": self.build_contact_email_query(email),
        }

    def build_custom_activity_type_id_query(self, custom_activity_type_id: str):
        return {
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
        }

    def build_has_related_custom_activity_query(self, custom_activity_type_id: str):
        return {
            "type": "has_related",
            "this_object_type": "lead",
            "related_object_type": "activity.custom_activity",
            "related_query": {
                "type": "and",
                "queries": [
                    self.build_custom_activity_type_id_query(custom_activity_type_id),
                    {"type": "match_all"},
                ],
            },
        }

    def build_date_range_query(
        self,
        object_type: str,
        field_name: str,
        start_date: date,
        end_date: date,
    ):
        return {
            "type": "field_condition",
            "field": {
                "type": "regular_field",
                "object_type": object_type,
                "field_name": field_name,
            },
            "condition": {
                "type": "moment_range",
                "on_or_after": {
                    "type": "fixed_local_date",
                    "which": "start",
                    "value": start_date.isoformat(),
                },
                "before": {
                    "type": "fixed_local_date",
                    "which": "end",
                    "value": end_date.isoformat(),
                },
            },
        }

    def search_leads_by_email(
        self, email: str, results_limit: int | None = None
    ) -> list[dict[str, Any]]:
        return self.search(
            self.build_lead_email_query(email),
            results_limit=results_limit,
        )

    async def find_lead_by_email(
        self, email: str, fields: list[str] | None = None
    ) -> dict[str, Any] | None:
        query = self.build_lead_email_query(email)
        leads = await asyncio.to_thread(
            self.search, query, results_limit=1, fields=fields
        )
        return leads[0] if leads else None

    def find_contact_by_email(self, email: str):
        contacts = self.search(
            self.build_contact_email_query(email),
            fields=["id", "emails"],
            results_limit=1,
            object_type="contact",
        )
        return contacts[0] if contacts else None

    def email_exists(self, email: str) -> bool:
        return self.count(self.build_lead_email_query(email)) > 0

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
        custom_activity_fields: list[str] | None = None,
        sort: list[dict[str, str | dict[str, str]]] | None = None,
    ):
        if sort is None:
            sort = [
                {
                    "field": {
                        "type": "regular_field",
                        "object_type": "activity.custom_activity",
                        "field_name": "date_created",
                    },
                    "direction": "asc",
                }
            ]

        query = self.build_custom_activity_type_id_query(custom_activity_type_id)

        return await asyncio.to_thread(
            self.search,
            query,
            sort,
            fields=custom_activity_fields,
            object_type="activity.custom_activity",
        )

    async def get_leads_with_custom_activity_instances(
        self,
        custom_activity_type_id: str,
        date_created_range: tuple[date, date] | None = None,
        lead_fields: list[str] | None = None,
        custom_activity_fields: list[str] | None = None,
        sort: list[dict[str, str | dict[str, str]]] | None = None,
        verbose: bool = False,
    ) -> list[dict[str, Any]]:
        if sort is None:
            sort = [
                {
                    "field": {
                        "type": "regular_field",
                        "object_type": "lead",
                        "field_name": "date_created",
                    },
                    "direction": "asc",
                }
            ]

        query = self.build_has_related_custom_activity_query(custom_activity_type_id)

        leads = self.search(query, sort=sort, fields=lead_fields)
        if verbose:
            print(
                f"Found {len(leads)} leads that have custom activity instances of type {custom_activity_type_id}"
            )

        # Break leads into lists of max 100 lead IDs each
        chunk_size = 100
        lead_id_chunks = [
            [lead["id"] for lead in leads[i : i + chunk_size]]
            for i in range(0, len(leads), chunk_size)
        ]
        if verbose:
            print(f"Split {len(leads)} leads into {len(lead_id_chunks)} chunks")

        if date_created_range:
            start_date, end_date = date_created_range
            if not all(isinstance(x, date) for x in (start_date, end_date)):
                raise ValueError(
                    f"Invalid date range: {start_date} ({type(start_date)}) – {end_date} ({type(end_date)})"
                )
        else:
            start_date = None
            end_date = None

        def build_query(lead_ids: list[str]) -> dict:
            # Base queries that are always included
            queries = [
                {
                    "type": "object_type",
                    "object_type": "activity.custom_activity",
                },
                {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "activity.custom_activity",
                        "field_name": "lead_id",
                    },
                    "condition": {
                        "type": "reference",
                        "reference_type": "lead",
                        "object_ids": lead_ids,
                    },
                },
                self.build_custom_activity_type_id_query(custom_activity_type_id),
            ]

            if start_date and end_date:
                # Add date range query if dates are provided
                date_query = self.build_date_range_query(
                    "activity.custom_activity",
                    "date_created",
                    start_date,
                    end_date,
                )
                queries.append(date_query)

            return {
                "query": {
                    "type": "and",
                    "queries": queries,
                },
                "sort": [
                    {
                        "field": {
                            "type": "regular_field",
                            "object_type": "activity.custom_activity",
                            "field_name": "date_created",
                        },
                        "direction": "asc",
                    }
                ],
                "_fields": {"activity.custom_activity": custom_activity_fields}
                if custom_activity_fields
                else None,
                "_limit": 200,
            }

        endpoint_and_data_list = [
            ("data/search", build_query(lead_ids)) for lead_ids in lead_id_chunks
        ]

        responses, errors = await self.post_all(endpoint_and_data_list, 5, verbose)

        if any(response.get("cursor") for response in responses):
            raise ValueError(
                "Some search results were not fetched due to pagination limits. Reduce chunk size and try again."
            )

        custom_activity_instances = [
            instance for response in responses for instance in response["data"]
        ]

        if verbose:
            print(
                f"Found {len(custom_activity_instances)} custom activity instances of type {custom_activity_type_id}"
            )
        if errors:
            raise Exception(
                f"{len(errors)} errors while fetching custom activity instances for leads: {errors}"
            )

        # Create a mapping of lead_id to instances
        instances_by_lead = {}
        for instance in custom_activity_instances:
            instances_by_lead.setdefault(instance["lead_id"], []).append(instance)

        # Each lead should have at least one custom activity instance since we searched for leads with instances
        # in `build_has_related_custom_activity_query`. If any are missing, something has gone wrong.
        return [
            {**lead, "custom_activity_instances": instances_by_lead[lead["id"]]}
            for lead in leads
        ]

    def get_last_lead_qualification(self, lead_id: str, verbose: bool) -> dict | None:
        lead_qualification_custom_activity_type_id = self.get_custom_activity_type_id(
            "Lead Qualification"
        )
        lead_qualifications = self.get_all(
            "activity/custom",
            params={
                "lead_id": lead_id,
                "custom_activity_type_id": lead_qualification_custom_activity_type_id,
            },
        )

        if not lead_qualifications:
            return None

        if verbose:
            print(
                f"{len(lead_qualifications)} Lead Qualification custom activit{'y' if len(lead_qualifications) == 1 else 'ies'}"
            )

        sorted_lead_qualifications = sorted(
            lead_qualifications, key=lambda x: x["date_created"], reverse=True
        )
        return sorted_lead_qualifications[0]

    def get_workflow_id(self, prefix: str, suffix: str) -> str | None:
        workflows = self.get_all("sequence", params={"_fields": "id,name"})
        for workflow in workflows:
            if workflow["name"].startswith(prefix) and workflow["name"].endswith(
                suffix
            ):
                return workflow["id"]
        return None
