import json
import os

import panel as pn
from bokeh.embed import server_document
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

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
            "text": json.dumps(
                list(Base("kpi-dev", "allocations").find()),
                indent=4,
            ),
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


@app.get("/panel")
async def bkapp_page(request: Request):
    script = server_document("http://127.0.0.1:5000/app")
    return templates.TemplateResponse(
        "base.html", {"request": request, "script": script}
    )


from src.sliders.pn_app import createApp

pn.serve(
    {"/app": createApp},
    port=5000,
    allow_websocket_origin=["127.0.0.1:8000"],
    address="127.0.0.1",
    show=False,
)
