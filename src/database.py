import os
import time

import pandas as pd
from loguru import logger
from pymongo import MongoClient


class Base:
    """
    Small wrapper to pymongo database.
    """

    def __init__(self, base: str, collection: str):
        self._client = MongoClient(f"{os.getenv('MONGO_URL')}/")
        self._coll = self._client[base][collection]

    def create_index(self, expiration: float):
        self._coll.create_index("inserted", expireAfterSeconds=expiration)

    def insert(self, data: pd.DataFrame):
        result = self._coll.insert_many(data.to_dict(orient="records"), ordered=False)

        a, b = len(result.inserted_ids), len(data)
        (logger.error if a < b else logger.success)(
            f"[{self._coll.name}] Inserted {a}/{b} documents."
        )

        return result

    def find(self, query=None) -> pd.DataFrame:
        if query is None:
            query = {}

        t0 = time.monotonic()
        result = pd.DataFrame(self._coll.find(query, projection={"_id": False}))
        logger.info(
            f"[{self._coll.name}] Query '{query}' resulted in "
            f"{len(result)} results in {time.monotonic() - t0:.2f}s."
        )
        return result

    def delete(self, query):
        result = self._coll.delete_many(query)
        logger.info(
            f"[{self._coll.name}] Query '{query}' resulted in {result.deleted_count} deleted documents."
        )
