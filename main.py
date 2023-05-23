import os

from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient

app = FastAPI()

class Msg(BaseModel):
    msg: str

def get_database_connection():
    connection_string = f"{os.getenv('MONGO_URL')}/test_atlas"
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
        "value": 201
    }

    collection.insert_many([item])

    
    return {"message": "<br/>".join(collection.find())}


@app.post("/path")
async def demo_post(inp: Msg):
    return {"message": inp.msg.upper()}


@app.get("/path/{path_id}")
async def demo_get_path_id(path_id: int):
    return {"message": f"This is /path/{path_id} endpoint, use post request to retrieve result"}