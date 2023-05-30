import os
import typing
from types import TracebackType

import anyio
import arrow
import httpx
from dotenv import load_dotenv
from loguru import logger

import src.severa.models as models

load_dotenv(r"C:\Users\vireima\tie-dashboard\.env")


SEVERA_CLIENT_ID = os.getenv("SEVERA_CLIENT_ID")
SEVERA_CLIENT_SECRET = os.getenv("SEVERA_CLIENT_SECRET")
SEVERA_SCOPE = os.getenv("SEVERA_CLIENT_SCOPE")
SEVERA_BASE_URL = "https://api.severa.visma.com/rest-api/v1.0/"

T = typing.TypeVar("T", bound="Client")
JSON = dict[str, typing.Any] | list[dict[str, typing.Any]]


class Client:
    def __init__(self: T) -> None:
        self._client_id = SEVERA_CLIENT_ID
        self._client_secret = SEVERA_CLIENT_SECRET
        self._client_scope = SEVERA_SCOPE

        self._client = httpx.AsyncClient(base_url=SEVERA_BASE_URL, http2=True)
        self._auth: typing.Optional[models.PublicAuthenticationOutputModel] = None
        self._request_limit = anyio.Semaphore(5)

    async def _authenticate(self) -> None:
        payload = {
            "client_Id": self._client_id,
            "client_Secret": self._client_secret,
            "scope": self._client_scope,
        }

        response = await self._client.post("token", json=payload)
        response.raise_for_status()

        self._auth = models.PublicAuthenticationOutputModel(**response.json())

    async def _reauthenticate(self: T) -> None:
        response = await self._client.post(
            "refreshtoken",
            headers={"client_Id": self._client_id},
            json=self._auth.refresh_token,
        )
        response.raise_for_status()

        self._auth = models.PublicAuthenticationOutputModel(**response.json())

    async def auth(self):
        if self._auth is None:
            await self._authenticate()
        else:
            # Check auth expiration
            access_expires = arrow.get(self._auth.access_token_expires_utc.isoformat())
            refresh_expires = arrow.get(
                self._auth.refresh_token_expires_utc.isoformat()
            )
            now = arrow.utcnow()

            if now > access_expires:
                if now < refresh_expires:
                    logger.debug("Refreshing auth")
                    await self._reauthenticate()
                else:
                    logger.debug("Access and refresh expired, authing again")
                    await self._authenticate()

        return {
            "client_Id": self._client_id,
            "Authorization": f"{self._auth.access_token_type} "
            f"{self._auth.access_token}",
        }

    async def __aenter__(self: T) -> T:
        await self._client.__aenter__()

        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def get_with_retries(self, endpoint: str, params, headers):
        retries = 0

        while retries < 6:
            async with self._request_limit:
                response: httpx.Response = await self._client.get(
                    endpoint, params=params, headers=headers
                )

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    # Too Many Requests

                    logger.warning("Got 429, sleeping 2s.")
                    await anyio.sleep(2.0)
                    logger.warning("Sleeping done.")
                else:
                    raise

            except httpx.RequestError as exc:
                logger.exception(exc)
                raise
            except httpx.HTTPError as exc:
                logger.exception(exc)
                raise
            else:
                logger.success(
                    f"{response.http_version} GET {endpoint} [{retries}]: "
                    f"{type(response).__name__} "
                    f"({len(response.json())}) in "
                    f"{response.elapsed.total_seconds():.2f}s."
                )
                return response

            retries += 1

        logger.error("Retry limit reached.")
        raise httpx.RequestError("Retry limit reached.")

    async def get(self, endpoint: str, params=None, **kwargs) -> JSON:
        next_page_available = True
        headers = {}

        if params is None:
            params = {}

        params.update(**kwargs)

        while next_page_available:
            headers.update(await self.auth())

            response = await self.get_with_retries(endpoint, params, headers)

            yield response.json()

            if next_page_available := ("NextPageToken" in response.headers):
                params.update({"pageToken": response.headers["NextPageToken"]})
                pass

    async def get_all(self, endpoint: str, params=None, **kwargs) -> list[JSON]:
        results = []
        async for json in self.get(endpoint, params, **kwargs):
            results += json if isinstance(json, list) else [json]
        return results