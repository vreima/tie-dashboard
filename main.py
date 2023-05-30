import json
import os

import panel as pn
from bokeh.embed import server_document
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

import src.severa.base_client as base_client
from src.severa.fetch import Fetcher

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/static")


def get_database_connection(base_name: str) -> Database:
    connection_string = f"{os.getenv('MONGO_URL')}/"
    client = MongoClient(connection_string)
    return client[base_name]


@app.get("/")
async def root():
    return {"message": "Hello World. Welcome to FastAPI!"}


@app.get("/path")
async def demo_get():
    db = get_database_connection()
    collection = db["test_collection"]

    item = {
        # "_id": "2000",
        "name": "hilger01",
        "value": 201,
    }

    # collection.insert_many([item])

    res = ""
    try:
        items = collection.find()
        res += str(type(items)) + "\n"
        res += str(dir(items)) + "\n\n"
        for item in items:
            res += str(item) + "\n"
    finally:
        return res

    return {
        "message": "<br/>".join(
            f"{key}: {value}<br/>" for key, value in collection.find()
        )
    }


@app.get("/save")
async def save() -> int:
    async with Fetcher() as fetcher:
        data = await fetcher.get_resource_allocations()

    db = get_database_connection("kpi-dev")
    collection: Collection = db["work"]
    result = collection.insert_many(data.to_dict(orient="records"), ordered=False)
    return len(result.inserted_ids)


@app.get("/load")
async def load(request: Request):
    collection: Collection = get_database_connection("kpi-dev")["work"]
    return templates.TemplateResponse(
        "pre.html",
        {
            "request": request,
            "text": json.dumps(
                list(collection.find()),
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
