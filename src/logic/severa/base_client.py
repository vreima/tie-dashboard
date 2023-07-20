import asyncio
import time
import typing
from types import TracebackType

import anyio
import arrow
import httpx
from loguru import logger

from src.config import settings
from src.logic.severa import models

T = typing.TypeVar("T", bound="Client")
JSON = dict[str, typing.Any]


class RateLimiter:
    def __init__(self, amount: int, rate: float):
        self._amount = amount
        self._rate = rate
        self._timestamps = [0.0]

    async def wait(self) -> None:
        """
        Wait if there's need to limit the rate, otherwise return instantly.
        """
        nth_last_ts = self._timestamps[-self._amount :][0]
        time_since_nth_ping = time.monotonic() - nth_last_ts

        if time_since_nth_ping < self._rate:
            await asyncio.sleep(self._rate - time_since_nth_ping)

        self._timestamps.append(time.monotonic())


class Request:
    def __init__(self, endpoint: str, params, headers):
        self.endpoint = endpoint
        self.params = params
        self.headers = headers
        self.queue = asyncio.Queue()

    async def response(self) -> httpx.Response:
        """
        Wait for the response from HTTP GET sent in send().
        """
        return await self.queue.get()

    async def send(self, client: httpx.AsyncClient) -> None:
        """
        Send the HTTP GET using client.
        """
        if client.is_closed:
            logger.error(f"/{self.endpoint} -> Client is closed.")
            await self.queue.put(httpx.Response(httpx.codes.INTERNAL_SERVER_ERROR))
            return

        try:
            response: httpx.Response = await client.get(
                self.endpoint, params=self.params, headers=self.headers
            )
        except httpx.WriteError as exc:
            logger.error(f"/{self.endpoint} -> httpx.WriteError: {exc}")
            await self.queue.put(httpx.Response(httpx.codes.INTERNAL_SERVER_ERROR))
        else:
            await self.queue.put(response)


class Client:
    MAX_RETRIES = 6
    HTTP_ERROR_429 = 429

    def __init__(self: T) -> None:
        self._client_id: str = settings.severa_client_id
        self._client_secret: str = settings.severa_client_secret
        self._client_scope: str = settings.severa_client_scope

        self._client = httpx.AsyncClient(
            base_url=str(settings.severa_base_url), http2=True, timeout=120.0
        )
        self._auth: models.PublicAuthenticationOutputModel | None = None
        self._request_limit = anyio.Semaphore(4)
        self._ratelimit = RateLimiter(10, 1.0)
        self._request_queue = asyncio.Queue()
        self._requester_worker = None

    async def _requester_worker_func(self) -> None:
        """
        Worker coroutine to send HTTP requests in the queue with rate limiting.
        """
        while True:
            request: Request = await self._request_queue.get()

            await self._ratelimit.wait()

            asyncio.create_task(request.send(self._client))

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
        assert self._auth is not None

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

        self._requester_worker = asyncio.create_task(self._requester_worker_func())

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        if self._requester_worker:
            self._requester_worker.cancel()

        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def get_with_retries(self, endpoint: str, params, headers):
        retries = 0

        while retries < Client.MAX_RETRIES:
            headers.update(await self.auth())

            request = Request(endpoint, params, headers)
            await self._request_queue.put(request)
            response = await request.response()

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(f"{exc}\n{exc.response.text}")

                match exc.response.status_code:
                    case httpx.codes.UNAUTHORIZED:
                        await self._authenticate()
                    case httpx.codes.TOO_MANY_REQUESTS:
                        # Too Many Requests, shouldn't happen though
                        logger.warning("Got 429, sleeping 2s.")
                        await anyio.sleep(2.0)
                        logger.warning("Sleeping done.")
                    case _:
                        raise

            except (httpx.RequestError, httpx.HTTPError) as exc:
                logger.exception(exc)
                raise
            else:
                logger.success(
                    f"{response.http_version} GET {endpoint} {'' if retries < 1 else f'[retry {retries}] '}in "
                    f"{response.elapsed.total_seconds():.2f}s."
                )
                return response

            retries += 1

        logger.error("Retry limit reached.")
        raise httpx.RequestError("Retry limit reached.")

    async def get(self, endpoint: str, params=None, **kwargs):
        next_page_available = True
        headers = {}

        if params is None:
            params = {}

        params.update(**kwargs)

        while next_page_available:
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
