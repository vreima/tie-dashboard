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


async def save():
    KPI = namedtuple("KPI", "id base_name collection_name span get")
    kpis = [
        KPI(
            "allocations",
            "kpi-dev",
            "allocations",
            DateRange(540),
            Fetcher.get_allocations_with_maxes,
        ),
        # KPI("allocations, "kpi-dev", "allocations",
        # DateRange(540), Fetcher.get_resource_allocations),
    ]

    logger.debug("/save: Fetching and saving kpis.")
    async with Fetcher() as fetcher:
        for kpi in kpis:
            t0 = time.monotonic()
            data = await kpi.get(fetcher, kpi.span)
            len(Base(kpi.base_name, kpi.collection_name).insert(data).inserted_ids)
            logger.success(
                f"KPI '{kpi.id}' fetched and saved in {time.monotonic() - t0:.2f}s."
            )


@app.get("/save/")
async def read_save(request: Request) -> None:
    logger.debug(f"/save request from {request.client.host}")
    await save()


@app.get("/load/{collection}")
async def read_load(request: Request, collection: str):
    return pre(
        Base("kpi-dev", collection).find().to_string(show_dimensions=True), request
    )


@app.get("/severa/{endpoint}")
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


async def ping():
    logger.debug("Got PING request.")


@app.get("/ping")
async def read_ping():
    await ping()


@app.get("/kpi")
async def altair_plot(request: Request, span: int = 30):
    t0 = time.monotonic()
    charts, n_rows = await src.visualization.ChartGroup(span).get_charts()

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
        jobs.add_jobs(
            [
                Cronjob(*params)
                for params in [(ping, "0/1 * * * *"), (save, "0 2 * * *")]
            ]
        )
        background_tasks.add_task(jobs.start)

    return pre(jobs.status(), request)


# @app.get("/del")
# def base_del():
#     start, end = arrow.get("2023-05-31").span("day")
#     Base(
#         "kpi-dev", "allocations"
#     ).delete({"date": {'$lte': end.datetime, '$gte': start.datetime}})


# @app.get("/panel")
# async def bkapp_page(request: Request):
#     logger.debug(f"GET /panel from {request.client.host}:{request.client.port}")
#     script = server_document("http://https://tie-dashboard.up.railway.app:5000/app")
#     logger.debug(f"Returning {script[:80]}...")

#     return templates.TemplateResponse(
#         "base.html", {"request": request, "script": script}
#     )


# from src.sliders.pn_app import createApp

# pn.serve(
#     {"/app": createApp},
#     port=int(os.getenv("PORT")),
#     # websocket_origin=["127.0.0.1:8000"],
#     address="0.0.0.0",
#     show=False,
# )
