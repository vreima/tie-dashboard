import datetime
import json
import os
import time
import typing
from collections import namedtuple
from contextlib import asynccontextmanager
from typing import Annotated

import anyio
import arrow
import croniter
import httpx
import pandas as pd
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from loguru import logger

from src.database.database import Base
from src.logic.severa import base_client
from src.logic.severa.client import Client as SeveraClient
from src.logic.slack.client import Client as SlackClient
from src.security import get_current_username
from src.util.daterange import DateRange


@asynccontextmanager
async def lifespan(app: FastAPI):
    jobs = get_cronjobs()

    jobs.add_jobs(
        [
            Cronjob(*params)
            for params in [
                # (save, "0 2 * * *"),
                (save_sparse, "0 2 * * *")
            ]
        ]
    )

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            tg.start_soon(jobs.start, app)

            yield

            scope.cancel()


default_router = APIRouter(tags=["main"], lifespan=lifespan)


templates = Jinja2Templates(directory="src/static")


def pre(text: str, request: Request):
    """
    Render simple <pre></pre> -formatted HTML page for debugging.
    """
    return templates.TemplateResponse(
        "pre.html",
        {"request": request, "text": text},
    )


@default_router.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("favicon.ico")


@default_router.get("/")
async def root():
    return {"message": "Hello World."}


async def save_sparse():
    BASE = "kpi-dev-02"
    KPI = namedtuple("KPI", "id base_name collection_name span get")
    kpis = [
        KPI(
            "hours",
            BASE,
            "hours",
            DateRange(540),
            SeveraClient.fetch_hours,
        ),
        KPI(
            "sales",
            BASE,
            "sales",
            DateRange(540),
            SeveraClient.fetch_salesvalue,
        ),
        KPI(
            "billing",
            BASE,
            "billing",
            DateRange(120),
            SeveraClient.fetch_billing,
        ),
    ]

    logger.debug("/save_sparse: Fetching and saving kpis.")
    async with SeveraClient() as client:
        for kpi in kpis:
            t0 = time.monotonic()

            try:
                data = await kpi.get(client, kpi.span)
            except Exception as e:
                logger.exception(e)
            else:
                Base(kpi.base_name, kpi.collection_name).upsert(data)
                logger.success(
                    f"Documents for KPI '{kpi.id}' fetched and upserted in {time.monotonic() - t0:.2f}s."
                )

        inv_collection = Base(BASE, "invalid")
        inv_collection.create_index(23 * 60 * 60)
        inv_collection.upsert(client.get_invalid_sales())


async def save_only_invalid_salescase_info() -> pd.DataFrame:
    async with SeveraClient() as client:
        try:
            sales = await client.fetch_sales(force_refresh=True)  # noqa: F841
        except Exception as e:
            logger.exception(e)

        inv_collection = Base("kpi-dev-02", "invalid")
        # inv_collection.create_index(60 * 60)
        inv_collection.upsert(client.get_invalid_sales())

        return client.get_invalid_sales()


@default_router.get("/save_sparse")
async def read_save_sparse(
    request: Request,
    username: Annotated[str, Depends(get_current_username)],  # noqa: ARG001
) -> None:
    logger.debug(f"/save_sparse request from {request.client.host}")
    await save_sparse()


@default_router.get("/load/{base}/{collection}")
async def read_load(
    request: Request,
    base: str,
    collection: str,
    username: Annotated[str, Depends(get_current_username)],  # noqa: ARG001
):
    return pre(
        Base(base, collection).find(ids=True).to_string(show_dimensions=True), request
    )


@default_router.get("/read/{endpoint}")
async def read(
    endpoint: str,
    request: Request,
    username: Annotated[str, Depends(get_current_username)],  # noqa: ARG001
):
    async with base_client.Client() as client:
        return await client.get_all(
            endpoint,
            params={
                key: request.query_params.getlist(key) for key in request.query_params
            },
        )


class Cronjob:
    def __init__(self, endpoint: str | typing.Awaitable, cron: str):
        self.endpoint = endpoint
        self.cronstring = cron
        self.croniter = croniter.croniter(cron, arrow.utcnow().datetime)

        self.next_run: arrow.Arrow = arrow.Arrow(1900, 1, 1)

    def advance(self) -> arrow.Arrow:
        self.next_run = arrow.get(self.croniter.get_next(float))
        return self.next_run

    def time_to_next(self) -> datetime.timedelta:
        return self.next_run - arrow.utcnow()


class CronjobManager:
    def __init__(self):
        self.jobs = []
        self.started = False

    def add_jobs(self, jobs: typing.Iterable[Cronjob]):
        self.jobs.extend(jobs)

    def status(self) -> str:
        result = (
            "Service is running.\n" if self.started else "Service is not running.\n"
        )
        result += f"{len(self.jobs)} in queue:\n" + "\n".join(
            f'   {job.endpoint} running next {job.next_run.humanize()} at {job.next_run.to("Europe/Helsinki").format("HH:mm DD.MM.YYYY")}'
            for job in self.jobs
        )

        return result

    async def start(self, app: FastAPI):
        self.started = True

        async with anyio.create_task_group() as tg:
            for cj in self.jobs:
                logger.debug(f"Starting {cj.endpoint}.")
                tg.start_soon(run_cronjob, cj, app, name=cj.endpoint)


def get_cronjobs(manager: CronjobManager = CronjobManager()):  # noqa: B008
    return manager


async def run_cronjob(timing: Cronjob, app: APIRouter):
    while True:
        timing.advance()
        delay = timing.time_to_next().seconds
        await anyio.sleep(delay)

        if isinstance(timing.endpoint, str):
            async with httpx.AsyncClient(
                app=app,
                base_url=os.getenv("PUBLIC_URL"),
                http2=True,
                follow_redirects=True,
            ) as client:
                try:
                    response = await client.get(timing.endpoint)
                    logger.debug(f"{timing.endpoint}: response {response.status_code}.")
                except (httpx.HTTPStatusError, httpx.HTTPError) as e:
                    logger.exception(e)
                except Exception as e:
                    logger.exception(e)
        else:
            await timing.endpoint()


@default_router.get("/status")
async def status(request: Request):
    jobs = get_cronjobs()

    return pre(jobs.status(), request)


################
# Slack routes #
################


slack_router = APIRouter(prefix="/slack", tags=["slack"])


@slack_router.get("/offers.json")
async def get_offers_json(
    request: Request,  # noqa: ARG001
    channel: str = "CSFQ71ANA",
    reaction: str = "k",
    startDate: datetime.datetime | None = None,
):
    """
    Fetch all open (not marked with reaction)
    offers/messages from the channel #tie_tarjouspyynnöt.
    Timespan defaults to last two months.
    """
    start = (
        arrow.utcnow().shift(months=-2).floor("month")
        if startDate is None
        else arrow.get(startDate)
    )

    slack = SlackClient()

    return slack.fetch_unmarked_offers(channel, reaction, start)


@slack_router.get("/offers")
async def get_offers(
    request: Request,
    channel: str = "CSFQ71ANA", 
    reaction: str = "k", 
    startDate: datetime.datetime | None = None, 
):
    """
    Fetch all open (not marked with reaction)
    offers/messages from the channel #tie_tarjouspyynnöt.
    Timespan defaults to last two months.
    """

    return templates.TemplateResponse(
        "offers.html",
        {
            "request": request,
            channel: channel,
            reaction: reaction,
            startDate: startDate,
        },
    )


default_router.include_router(slack_router)


#################
# Severa routes #
#################

severa_router = APIRouter(prefix="/severa", tags=["severa"])


@severa_router.get("/salescases.json")
async def get_salescases_json():
    """
    Fetch invalid salescases from Visma Severa.
    """
    return (await save_only_invalid_salescase_info()).to_dict(orient="records")


@severa_router.get("/salescases")
async def get_salescases(request: Request):
    """
    Fetch invalid salescases from Visma Severa.
    """
    return templates.TemplateResponse("salescases.html", {"request": request})


@severa_router.get("/{endpoint:path}")
async def severa_endpoint(
    endpoint: str,
    request: Request,
    username: Annotated[str, Depends(get_current_username)],  # noqa: ARG001
):
    async with base_client.Client() as client:
        return pre(
            json.dumps(
                await client.get_all(
                    endpoint,
                    params={
                        key: request.query_params.getlist(key)
                        for key in request.query_params
                    },
                ),
                indent=4,
            ),
            request,
        )


default_router.include_router(severa_router)
