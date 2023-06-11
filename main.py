import datetime
import json

# import panel as pn
import os
import time
import typing
from collections import namedtuple

import anyio
import arrow
import croniter
import httpx

# from bokeh.embed import server_document
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

import src.visualization
from src.database import Base
from src.daterange import DateRange
from src.severa import base_client
from src.severa.fetch import Fetcher

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/static")


def pre(text: str, request: Request):
    return templates.TemplateResponse(
        "pre.html",
        {"request": request, "text": text},
    )


@app.get("/")
async def root():
    return {"message": "Hello World. Welcome to FastAPI!"}


async def save_only_invalid_salescase_info():
    async with Fetcher() as fetcher:
        try:
            data = await fetcher.get_sales_work(DateRange(540))
        except Exception as e:
            logger.exception(e)

        inv_collection = Base("kpi-dev", "invalid")
        inv_collection.create_index(23 * 60 * 60)
        inv_collection.insert(fetcher.invalid_sales())

    return "ok"


async def save(only_kpis=None):
    if not only_kpis:
        only_kpis = None

    KPI = namedtuple("KPI", "id base_name collection_name span get")
    kpis = [
        KPI(
            "allocations",
            "kpi-dev",
            "allocations",
            DateRange(540),
            Fetcher.get_allocations_with_maxes,
        ),
        KPI(
            "sales-value",
            "kpi-dev",
            "sales-value",
            DateRange(540),
            Fetcher.get_sales_value,
        ),
        KPI(
            "sales-work",
            "kpi-dev",
            "sales-work",
            DateRange(540),
            Fetcher.get_sales_work,
        )
        # KPI("allocations, "kpi-dev", "allocations",
        # DateRange(540), Fetcher.get_resource_allocations),
    ]

    logger.debug("/save: Fetching and saving kpis.")
    async with Fetcher() as fetcher:
        for kpi in kpis:
            if only_kpis is not None and kpi.id not in only_kpis:
                break

            t0 = time.monotonic()

            try:
                data = await kpi.get(fetcher, kpi.span)
            except Exception as e:
                logger.exception(e)
            else:
                inserted = len(
                    Base(kpi.base_name, kpi.collection_name).insert(data).inserted_ids
                )
                logger.success(
                    f"{inserted} documents for KPI '{kpi.id}' fetched and saved in {time.monotonic() - t0:.2f}s."
                )

        inv_collection = Base(kpi.base_name, "invalid")
        inv_collection.create_index(23 * 60 * 60)
        inv_collection.insert(fetcher.invalid_sales())


@app.get("/save//")
async def read_save(request: Request) -> None:
    logger.debug(f"/save request from {request.client.host}")
    await save(request.query_params.getlist("kpi"))


@app.get("/save_invalid")
async def read_save(request: Request) -> None:
    logger.debug(f"/save_invalid request from {request.client.host}")
    await save_only_invalid_salescase_info()


@app.get("/load/{collection}")
async def read_load(request: Request, collection: str):
    return pre(
        Base("kpi-dev", collection).find().to_string(show_dimensions=True), request
    )


@app.get("/debug")
async def read_debug(request: Request):
    async with Fetcher() as f:
        data = await f.get_billing_forecast(DateRange(540))
    return pre(data.to_string(show_dimensions=True), request)


@app.get("/invalid_salescases")
async def invalid_salescases():
    return Base("kpi-dev", "invalid").find().to_dict(orient="records")


@app.get("/severa/{endpoint:path}")
async def severa_endpoint(endpoint: str, request: Request):
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


@app.get("/table")
async def tabulate(request: Request):
    return templates.TemplateResponse("table.html", {"request": request})


@app.get("/salescases")
async def salescases(request: Request):
    return templates.TemplateResponse("salescases.html", {"request": request})


@app.get("/read/{endpoint}")
async def read(endpoint: str, request: Request):
    async with base_client.Client() as client:
        return await client.get_all(
            endpoint,
            params={
                key: request.query_params.getlist(key) for key in request.query_params
            },
        )


async def ping():
    logger.debug("Got PING request.")


@app.get("/ping")
async def read_ping():
    await ping()


@app.get("/kpi")
async def altair_plot(request: Request, chart_num: int | None = None):
    t0 = time.monotonic()
    charts, n_rows = await src.visualization.ChartGroup().get_charts()

    if chart_num is not None:
        charts = [charts[chart_num]]

    vega_json = {f"chart{n}": chart.to_json() for n, chart in enumerate(charts)}

    return templates.TemplateResponse(
        "vega.html",
        {
            "request": request,
            "chart_ids": list(vega_json.keys()),
            "vega_json": vega_json,
            "n_rows": n_rows,
            "time": f"{time.monotonic() - t0:.1f}s",
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

    async def start(self):
        self.started = True

        async with anyio.create_task_group() as tg:
            for cj in self.jobs:
                logger.debug(f"Starting {cj.endpoint}.")
                tg.start_soon(run_cronjob, cj, name=cj.endpoint)


def get_cronjobs(manager: CronjobManager = CronjobManager()):  # noqa: B008
    return manager


async def run_cronjob(timing: Cronjob):
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


@app.get("/start")
async def start(background_tasks: BackgroundTasks, request: Request):
    jobs = get_cronjobs()

    if not jobs.started:
        jobs.add_jobs([Cronjob(*params) for params in [(save, "0 2 * * *")]])
        background_tasks.add_task(jobs.start)

    return pre(jobs.status(), request)
