import json
import time
from collections import namedtuple

# from bokeh.embed import server_document
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

import src.visualization
from src.database import Base
from src.daterange import DateRange
from src.severa import base_client
from src.severa.fetch import Fetcher

# import panel as pn
# import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/static")


@app.get("/")
async def root():
    return {"message": "Hello World. Welcome to FastAPI!"}


@app.get("/save/")
async def save() -> None:
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
    async with Fetcher() as fetcher:
        for kpi in kpis:
            t0 = time.monotonic()
            data = await kpi.get(fetcher, kpi.span)
            len(Base(kpi.base_name, kpi.collection_name).insert(data).inserted_ids)
            logger.success(
                f"KPI '{kpi.id}' fetched and saved in {time.monotonic() - t0:.2f}s."
            )


@app.get("/load/{collection}")
async def load(request: Request, collection: str):
    return templates.TemplateResponse(
        "pre.html",
        {
            "request": request,
            "text": Base("kpi-dev", collection).find().to_string(show_dimensions=True),
        },
    )


@app.get("/severa/{endpoint}")
async def severa_endpoint(endpoint: str, request: Request):
    async with base_client.Client() as client:
        return templates.TemplateResponse(
            "pre.html",
            {
                "request": request,
                "text": json.dumps(
                    await client.get_all(
                        endpoint,
                        params={
                            key: request.query_params.getlist(key)
                            for key in request.query_params
                        },
                    ),
                    indent=4,
                ),
            },
        )


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
            "time": f"{time.monotonic() - t0:.1f}s"
        },
    )


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
