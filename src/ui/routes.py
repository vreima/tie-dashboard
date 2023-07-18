import datetime
import json

# import os
import time
import typing
from collections import namedtuple
from typing import Annotated

import anyio
import arrow
import croniter
import httpx
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from loguru import logger

import src.logic.processing
import src.logic.severa.models
import src.logic.slack.models as slack_models
import src.util.stable_hash
from src.config import settings
from src.database.database import Base
from src.logic.kpi import kpi
from src.logic.pressure.pressure import fetch_pressure
from src.logic.severa import base_client
from src.logic.severa.client import Client as SeveraClient
from src.logic.slack.client import Client as SlackClient
from src.logic.slack.client import send_weekly_slack_update_debug
from src.security import get_current_username
from src.util.daterange import DateRange

default_router = APIRouter(tags=["main"])


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
        for kpi_iter in kpis:
            t0 = time.monotonic()

            try:
                data = await kpi_iter.get(client, kpi_iter.span)
            except Exception as e:
                logger.exception(e)
            else:
                Base(kpi_iter.base_name, kpi_iter.collection_name).upsert(data)
                logger.success(
                    f"Documents for KPI '{kpi_iter.id}' fetched and upserted in {time.monotonic() - t0:.2f}s."
                )

        inv_collection = Base(BASE, "invalid")
        inv_collection.create_index(23 * 60 * 60)
        inv_collection.upsert(client.get_invalid_sales())


async def save_only_invalid_salescase_info() -> pd.DataFrame:
    async with SeveraClient() as client:
        try:
            await client.fetch_sales(force_refresh=True)
        except Exception as e:
            logger.exception(e)

        inv_collection = Base("kpi-dev-02", "invalid")
        # inv_collection.create_index(60 * 60)
        inv_collection.upsert(client.get_invalid_sales())

        return client.get_invalid_sales()


@default_router.get("/save_sparse")
async def read_save_sparse(
    request: Request,
    username: Annotated[str, Depends(get_current_username)],
) -> None:
    logger.debug(f"/save_sparse request from {request.client.host}")
    await save_sparse()


@default_router.get("/load/{base}/{collection}")
async def read_load(
    request: Request,
    base: str,
    collection: str,
    username: Annotated[str, Depends(get_current_username)],
):
    return pre(
        Base(base, collection).find(ids=True).to_string(show_dimensions=True), request
    )


@default_router.get("/read/{endpoint}")
async def read(
    endpoint: str,
    request: Request,
    username: Annotated[str, Depends(get_current_username)],
):
    async with base_client.Client() as client:
        return await client.get_all(
            endpoint,
            params={
                key: request.query_params.getlist(key) for key in request.query_params
            },
        )


class Cronjob:
    def __init__(self, endpoint: typing.Coroutine, cron: str, name: str):
        self.name = name
        self.endpoint = endpoint
        self.cronstring = cron
        self.croniter = croniter.croniter(cron, arrow.utcnow().datetime)

        self.next_run: arrow.Arrow = arrow.Arrow(1900, 1, 1)

    def try_advance(self) -> arrow.Arrow:
        previous_run = self.next_run
        self.next_run = arrow.get(self.croniter.get_next(float))

        logger.debug(f"Advancing from {previous_run} to {self.next_run}.")
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
            f'   [{job.name}] {job.endpoint} running next {job.next_run.humanize()} at {job.next_run.to("Europe/Helsinki").format("HH:mm DD.MM.YYYY")}'
            for job in self.jobs
        )

        return result

    async def start(self, app: FastAPI):
        self.started = True

        async with anyio.create_task_group() as tg:
            for cj in self.jobs:
                logger.debug(f"Starting cronjob '{cj.name}' running {cj.endpoint}.")
                tg.start_soon(run_cronjob, cj, app, name=cj.endpoint)


def get_cronjobs(manager: CronjobManager = CronjobManager()):  # noqa: B008
    return manager


async def run_cronjob(timing: Cronjob, app: FastAPI):
    with logger.contextualize(source=timing.name):
        while True:
            timing.try_advance()
            delay = timing.time_to_next().total_seconds()

            logger.debug(
                f"Cronjob sleeping for {delay}s (={delay / 60 / 60 / 24:.3f} days)."
            )

            await anyio.sleep(max(delay, 0))

            logger.debug("Cronjob task is called.")

            if isinstance(timing.endpoint, str):
                async with httpx.AsyncClient(
                    app=app,
                    base_url=settings.railway_static_url,
                    http2=True,
                    follow_redirects=True,
                ) as client:
                    try:
                        response = await client.get(timing.endpoint)
                        logger.debug(
                            f"{timing.endpoint}: response {response.status_code}."
                        )
                    except (httpx.HTTPStatusError, httpx.HTTPError) as e:
                        logger.exception(e)
                    except Exception as e:
                        logger.exception(e)
            else:
                await timing.endpoint()

            logger.debug("Cronjob task is done.")


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

    params = {
        "request": request,
        "channel": channel,
        "reaction": reaction,
    }

    if startDate is not None:
        params["startDate"] = startDate.isoformat()

    return templates.TemplateResponse("offers.html", params)


@slack_router.get("/send_debug_message")
async def send_debug_message():
    """
    Send weekly Viikkopalaveri msg mor often and to a debug channel.
    """
    logger.debug("/send_debug_message")
    await send_weekly_slack_update_debug()


@slack_router.post("/event")
async def handle_slack_event(
    event: slack_models.ChallengeModel | slack_models.AppMentionWrapperModel,
    bg: BackgroundTasks,
):
    """
    Main Slack Event API handler.
    """
    if isinstance(event, slack_models.ChallengeModel):
        # Return plain text challenge string to handle verification.
        return event.challenge
    else:
        bg.add_task(SlackClient.process_app_mention_event, SlackClient(), event)

        logger.debug(event.model_dump(mode="json"))


default_router.include_router(slack_router)


#################
# Severa routes #
#################

severa_router = APIRouter(prefix="/severa", tags=["severa"])


@severa_router.get(
    "/salescases.json",
)
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


###################
# Pressure routes #
###################

pressure_router = APIRouter(prefix="/kiire", tags=["pressure"])


@pressure_router.get("/pressure.json")
async def pressure(
    request: Request,  # noqa: ARG001
    startDate: str = "",
    endDate: str = "",
    users: str = "",
    businessunits: str = "",  # noqa: ARG001
):
    if not startDate:
        start = arrow.utcnow().shift(years=-1).floor("day")
    else:
        try:
            start = arrow.get(startDate).floor("day")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Query parameter 'startDate' has invalid date format, expected YYYY-mm-dd",
            ) from None

    if not endDate:
        end = arrow.utcnow().ceil("day")
    else:
        try:
            end = arrow.get(endDate).ceil("day")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Query parameter 'endDate' has invalid date format, expected YYYY-mm-dd",
            ) from None

    return await fetch_pressure(
        start, end, users.split(",") if len(users) > 0 else None
    )


@pressure_router.get("/")
async def pressure_dashboard(
    request: Request,
):
    return templates.TemplateResponse(
        "pressure_dashboard.html",
        {
            "request": request,
            "base_url": request.base_url,
            "hostname": request.base_url.hostname,
        },
    )


@pressure_router.get("/{user_name}")
async def user_pressure(request: Request, user_name: str):
    return templates.TemplateResponse(
        "pressure.html",
        {
            "request": request,
            "user_name": user_name,
            "base_url": request.base_url,
            "hostname": request.base_url.hostname,
        },
    )


@pressure_router.get("/save/{user_name}")
async def save_user_pressure(
    request: Request,  # noqa: ARG001
    user_name: str,
    x: float | None = None,
    y: float | None = None,
):
    Base("pressure", "pressure").upsert(
        pd.DataFrame(
            [
                {
                    "user": user_name,
                    "date": pd.Timestamp(arrow.utcnow().datetime),
                    "x": x,
                    "y": y,
                }
            ]
        )
    )
    return "OK"


default_router.include_router(pressure_router)


##############
# KPI routes #
##############


kpi_router = APIRouter(prefix="/kpi", tags=["kpi"])


class Datespan:
    def __init__(
        self, start: datetime.date | None = None, end: datetime.date | None = None
    ):
        today = arrow.utcnow()

        if start is None:
            self.start = today.shift(months=-4).floor("month")
        else:
            self.start = arrow.get(start, tzinfo="UTC")

        if end is None:
            self.end = today.shift(months=2).ceil("month")
        else:
            self.end = arrow.get(end, tzinfo="UTC")


DatespanDep = Annotated[Datespan, Depends()]


@kpi_router.get("/")
async def get_salesmargin(request: Request):
    return templates.TemplateResponse(
        "kpi_template.html",
        {"request": request},
    )


@kpi_router.get("/history")
async def get_history(request: Request):
    return templates.TemplateResponse(
        "billing_history.html",
        {"request": request},
    )


@kpi_router.get("/totals")
async def totals(span: DatespanDep):
    logger.debug(f"/totals: {DateRange(span.start, span.end)}")
    data = await src.logic.processing.load_and_merge(
        DateRange(span.start, span.end), forecasts_from_database=True
    )
    return data.to_dict(orient="records")


@kpi_router.get("/billing_history")
async def billing_history(span: DatespanDep):
    logger.debug(f"/billing_history: {DateRange(span.start, span.end)}")
    data = await src.logic.processing.load_merge_billing_forecast_history()
    return data.to_dict(orient="records")


@kpi_router.get("/billing")
async def billing(span: DatespanDep):
    logger.debug(f"/billing: {DateRange(span.start, span.end)}")
    data = await src.logic.processing.load_merge_billing()
    return data.to_dict(orient="records")


@kpi_router.get("/dbg_hours")
async def dbg_hours(span: DatespanDep):
    data = await kpi.hours_totals(span.start, span.end)
    logger.debug("\n" + str(data))
    return data.to_dict(orient="records")


@kpi_router.get("/salesmargin.json")
async def get_salesmargin_data(request: Request, span: DatespanDep):  # noqa: ARG001
    data = await kpi.sales_margin(span.start, span.end)
    json = data.to_dict(orient="records")
    return json


@kpi_router.get("/hours.json")
async def get_hours_data(request: Request, span: DatespanDep):  # noqa: ARG001
    return (await kpi.hours(span.start, span.end)).to_dict(orient="records")


default_router.include_router(kpi_router)
