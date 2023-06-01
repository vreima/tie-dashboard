import json
import time
from collections import namedtuple
from datetime import timedelta

import altair as alt
import pandas as pd
from bokeh.embed import server_document
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

import src.severa.base_client as base_client
from src.database import Base
from src.daterange import DateRange
from src.severa.fetch import Fetcher

# import panel as pn
# import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/static")


@app.get("/")
async def root():
    return {"message": "Hello World. Welcome to FastAPI!"}


@app.get("/save")
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
        # KPI("allocations, "kpi-dev", "allocations", DateRange(540), Fetcher.get_resource_allocations),
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
                            for key in request.query_params.keys()
                        },
                    ),
                    indent=4,
                ),
            },
        )


@app.get("/kpi")
async def altair_plot(request: Request, span: int = 30):
    def treb():
        font = "Trebuchet MS"

        return {
            "config": {
                "title": {"font": font, "subtitleFont": font},
                "axis": {"labelFont": font, "titleFont": font},
                "header": {"labelFont": font, "titleFont": font},
                "legend": {"labelFont": font, "titleFont": font},
            }
        }

    alt.themes.register("treb", treb)
    alt.themes.enable("treb")
    alt.renderers.set_embed_options(actions=False)
    alt.data_transformers.disable_max_rows()

    data = Base("kpi-dev", "allocations").find()

    delta = timedelta(days=span)

    grouped = (
        data[data["forecast-date"].between(data["date"], data["date"] + delta)]
        .groupby(["date", "type"])["value"]
        .sum()
        .reset_index()
    )

    # Pivot - unpivot
    g = grouped.pivot(columns="type", values="value", index="date").reset_index()
    g["total"] = g["external"] + g["internal"]
    g["billing-rate"] = g["external"] / g["total"]
    g["allocation-rate"] = g["total"] / g["max"]
    print(g)
    grouped = g.melt(id_vars=["date"]).convert_dtypes()
    print(grouped)
    # grouped["span"] = f'{grouped["date"] + delta:%d.%m.} - {grouped["date"] + delta:%d.%m.}'

    chart_base = alt.Chart(grouped).encode(
        x=alt.X("date(date):T").axis(title="Päiväys"),
        y=alt.Y("value:Q").axis(title="Allokoitu tuntimäärä (h)"),
        color=alt.Color("type:N", title="Sisäinen/projektityö/maksimi"),
        tooltip=[
            alt.Tooltip("value", title="Allokoitu tuntimäärä", format=".1f"),
            alt.Tooltip("total:Q", title="Allokoitu tuntimäärä yhteensä", format=".1f"),
            alt.Tooltip("type", title="Sisäinen/projektityö/maksimi"),
            alt.Tooltip("billing-rate:Q", title="Laskutusaste", format=".1%"),
            alt.Tooltip("allocation-rate:Q", title="Allokointiaste", format=".1%"),
            alt.Tooltip("date", title="Pvm", format="%d.%m.%Y"),
            # alt.Tooltip("span", title="Ennustusjakso"),
        ],
    )

    chart1 = (
        (
            chart_base.mark_area(
                point=alt.OverlayMarkDef(filled=False, fill="white", size=100)
            ).transform_filter(
                (alt.datum.type == "internal") | (alt.datum.type == "external")
            )
            + chart_base.mark_line(
                point=alt.OverlayMarkDef(filled=True, size=100), strokeDash=[4, 4]
            ).transform_filter(alt.datum.type == "max")
        ).properties(
            width="container",
            height=260,
        )
    ).interactive()

    # second

    users = await Fetcher().users()
    users_df = pd.DataFrame([{"user": u.guid, "name": u.firstName} for u in users])

    source = (
        data[(data["date"] == data["date"].max()) & (data["type"] != "max")]
        .groupby(["forecast-date", "user"])["value"]
        .sum()
        .reset_index()
    ).merge(users_df, on="user")

    brush = alt.selection_interval(encodings=["x"])

    base = (
        alt.Chart(source)
        .encode(x="forecast-date:T", y="value:Q")
        .properties(width="container", height=200)
    )

    upper = base.mark_area().encode(
        x=alt.X("forecast-date:T").scale(domain=brush), color="name:N"
    )

    lower = (
        base.mark_area()
        .encode(y="sum(value):Q")
        .properties(height=60)
        .add_params(brush)
    )

    chart2 = upper & lower

    charts = [chart1, chart2]

    vega_json = {
        key: json.dumps(chart.to_dict(), indent=2)
        for key, chart in zip(["chart1", "chart2"], charts)
    }

    return templates.TemplateResponse(
        "vega.html",
        {
            "request": request,
            "chart_ids": list(vega_json.keys()),
            "vega_json": vega_json,
        },
    )


# @app.get("/del")
# def base_del():
#     start, end = arrow.get("2023-05-31").span("day")
#     Base(
#         "kpi-dev", "allocations"
#     ).delete({"date": {'$lte': end.datetime, '$gte': start.datetime}})


@app.get("/panel")
async def bkapp_page(request: Request):
    logger.debug(f"GET /panel from {request.client.host}:{request.client.port}")
    script = server_document("http://https://tie-dashboard.up.railway.app:5000/app")
    logger.debug(f"Returning {script[:80]}...")

    return templates.TemplateResponse(
        "base.html", {"request": request, "script": script}
    )


from src.sliders.pn_app import createApp

pn.serve(
    {"/app": createApp},
    port=int(os.getenv("PORT")),
    # websocket_origin=["127.0.0.1:8000"],
    address="0.0.0.0",
    show=False,
)
