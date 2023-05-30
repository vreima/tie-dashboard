import os

import pandas as pd
from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


class Base:
    """
    Small wrapper to pymongo database.
    """

    def __init__(self, base: str, collection: str):
        self._client = MongoClient(f"{os.getenv('MONGO_URL')}/")
        self._coll = self._client[base][collection]

    def insert(self, data: pd.DataFrame):
        result = self._coll.insert_many(data.to_dict(orient="records"), ordered=False)

        a, b = len(result.inserted_ids), len(data)
        (logger.error if a < b else logger.success)(f"Inserted {a}/{b} items.")

        return result

    def find(self, query=None) -> list:
        if query is None:
            query = {}

        result = list(self._coll.find(query))
        logger.info(f"Query {query} resulted in {len(result)} results.")
        return result
