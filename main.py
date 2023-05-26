import os

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from pymongo import MongoClient

import src.severa.fetcher as fetcher
import json

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")


class Msg(BaseModel):
    msg: str


def get_database_connection():
    connection_string = f"{os.getenv('MONGO_URL')}/"
    client = MongoClient(connection_string)
    return client["test_base"]


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


@app.post("/path")
async def demo_post(inp: Msg):
    return {"message": inp.msg.upper()}


@app.get("/severa/{endpoint}")
async def demo_get_path_id(endpoint: str, request: Request):
    templates = Jinja2Templates(directory="src/static")

    async with fetcher.Client() as client:
        #return json.dumps(await client.get_all(endpoint, params={key: request.query_params.getlist(key) for key in request.query_params.keys()}), indent=4)
        
        return templates.TemplateResponse(
            "pre.html",
            {
                "request": request,
                "text": json.dumps(await client.get_all(endpoint, params={key: request.query_params.getlist(key) for key in request.query_params.keys()}), indent=4)
            },
        )
