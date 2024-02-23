import asyncio
import logging
import aiohttp
from aiohttp.typedefs import StrOrURL
from yarl import URL


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class APIError(Exception):
    """Raised when sending a request to the API failed."""

    def __init__(self, response: aiohttp.ClientResponse):
        super(response.text)
        self.response = response


class CallTrackingMetricsAPIClient:
    def __init__(self, auth_token=None, max_retries=5, debug=False):
        self.base_url = "https://api.calltrackingmetrics.com/api/v1/"
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Basic {auth_token}",
                "Content-Type": "application/json",
            },
        )
        self.max_retries: int = max_retries
        self.account_id: int | None = None
        self.debug: bool = debug

    async def close(self):
        await self.session.close()

    async def _dispatch(self, method, url: StrOrURL, **kwargs):
        if not isinstance(url, URL):
            url = self.base_url + url

        for attempt in range(self.max_retries):
            try:
                async with self.session.request(method, url, **kwargs) as response:
                    if response.status == 429:  # Too Many Requests
                        await asyncio.sleep(1)  # Wait for 1 second
                        continue
                    response.raise_for_status()
                    return await response.json()

            except aiohttp.ClientResponseError as e:
                if e.code == 404:
                    # Do not retry on 404
                    raise

                logging.error(f"Client Response Error: {e}")
                if attempt == self.max_retries - 1:
                    raise  # Re-raise the last exception
                await asyncio.sleep(1)  # Wait before retrying

            except aiohttp.ClientError as e:
                logging.error(f"Client Error: {e}")

                if attempt == self.max_retries - 1:
                    raise  # Re-raise the last exception
                await asyncio.sleep(1)  # Wait before retrying

        raise Exception("No response")

    async def get(self, endpoint: StrOrURL, params=None):
        return await self._dispatch("get", endpoint, params=params)

    async def get_all(self, endpoint: str, records_key: str, params=None):
        all_records = []
        url: StrOrURL = endpoint

        while True:
            try:
                response = await self.get(url, params)
                records = response.get(records_key)
                if not records:
                    break

                all_records.extend(records)

                next_page = response.get("next_page")
                if not next_page:
                    break
                url = URL(next_page)

                if self.debug:
                    logging.info(f"page {response['page']}/{response['total_pages']}")

            except AssertionError:
                logging.error("AssertionError:", exc_info=True)
                break

        return all_records

    async def get_accounts(self):
        return await self.get("accounts")

    async def set_account_id_to_primary_account_id(self):
        data = await self.get_accounts()
        accounts = data.get("accounts")
        primary_account_id = next((account["id"] for account in accounts), None)
        if not primary_account_id:
            raise Exception("No accounts found")
        self.account_id = primary_account_id

    async def get_calls(self, params=None):
        if params is None:
            params = {}
        params.setdefault("per_page", 150)  # per_page limit is 150

        if not self.account_id:
            await self.set_account_id_to_primary_account_id()

        return await self.get_all(
            f"accounts/{self.account_id}/calls", "calls", params=params
        )
