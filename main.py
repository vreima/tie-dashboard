import json
from datetime import timedelta

import altair as alt
import panel as pn
from bokeh.embed import server_document
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

import src.severa.base_client as base_client
from src.database import Base
from src.daterange import DateRange
from src.severa.fetch import Fetcher

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/static")


@app.get("/")
async def root():
    return {"message": "Hello World. Welcome to FastAPI!"}


@app.get("/save")
async def save() -> int:
    async with Fetcher() as fetcher:
        data = await fetcher.get_resource_allocations(DateRange(540))

    return len(Base("kpi-dev", "allocations").insert(data).inserted_ids)


@app.get("/load")
async def load(request: Request):
    return templates.TemplateResponse(
        "pre.html",
        {
            "request": request,
            "text": Base("kpi-dev", "allocations")
            .find()
            .to_string(show_dimensions=True),
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
def altair_plot(request: Request, span: int = 30):
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

    data = Base(
        "kpi-dev", "allocations"
    ).find()  # .to_json(orient="records", date_format="iso")
    # print(data.loc[:,["forecast-date", "value"]].head())

    delta = timedelta(days=span)
    grouped = (
        data[data["forecast-date"].between(data["date"], data["date"] + delta)]
        .groupby(["date", "is_internal"])["value"]
        .sum()
        .reset_index()
    )

    print(grouped.head())

    chart = (
        alt.Chart(grouped)
        .mark_area(point=alt.OverlayMarkDef(filled=False, fill="white"))
        .encode(
            x=alt.X("date(date):T").axis(title="Päiväys"),
            y=alt.Y("value:Q").axis(title="Allokoitu tuntimäärä (h)"),
            color=alt.Color("is_internal:N", title="Sisäinen työ"),
            tooltip=[
                alt.Tooltip("value", title="Allokoitu tuntimäärä", format=".1f"),
                alt.Tooltip("is_internal", title="Sisäinen työ"),
                alt.Tooltip("date", title="Pvm", format="%d.%m.%Y"),
            ],
        )
        .properties(
            width="container",
            height=260,
        )
    )

    return templates.TemplateResponse(
        "vega.html",
        {"request": request, "vega_json": json.dumps(chart.to_dict(), indent=2)},
    )


@app.get("/panel")
async def bkapp_page(request: Request):
    logger.debug(f"GET /panel from {request.client.host}:{request.client.port}")
    script = server_document("http://https://tie-dashboard.up.railway.app:5000/app")
    logger.debug(f"Returning {script[:80]}...")

    return templates.TemplateResponse(
        "base.html", {"request": request, "script": script}
    )


# from src.sliders.pn_app import createApp

# pn.serve(
#     {"/app": createApp},
#     port=5000,
#     # websocket_origin=["127.0.0.1:8000"],
#     address="0.0.0.0",
#     show=False,
# )
