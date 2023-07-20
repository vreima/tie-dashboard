import time

import pandas as pd
from loguru import logger
from pymongo import InsertOne, MongoClient, ReplaceOne

from src.config import settings


class NotNanDict(dict):
    @staticmethod
    def is_nan(v):
        if isinstance(v, dict):
            return False
        return pd.isna(v)

    def __new__(self, a):
        return {k: v for k, v in a if not self.is_nan(v)}


class Base:
    """
    Small wrapper to pymongo database.
    """

    def __init__(self, base: str, collection: str):
        self._client: MongoClient = MongoClient(str(settings.mongo_url))
        self._coll = self._client[base][collection]

    def create_index(self, expiration: float):
        self._coll.create_index("inserted", expireAfterSeconds=expiration)

    def insert(self, data: pd.DataFrame, sparsify: bool = False):
        if sparsify:
            result = self._coll.insert_many(
                data.to_dict(orient="records", into=NotNanDict), ordered=False
            )
        else:
            result = self._coll.insert_many(
                data.to_dict(orient="records"), ordered=False
            )

        a, b = len(result.inserted_ids), len(data)
        (logger.error if a < b else logger.success)(
            f"[{self._coll.name}] Inserted {a}/{b} documents."
        )

        return result

    def upsert(self, data: pd.DataFrame) -> None:
        if "_id" in data.columns:
            items_with_id = data[~data["_id"].isna()].to_dict(
                orient="records", into=NotNanDict
            )
            items_without_id = data[data["_id"].isna()].to_dict(
                orient="records", into=NotNanDict
            )

            operations = [
                ReplaceOne({"_id": item["_id"]}, item, upsert=True)
                for item in items_with_id
            ] + [InsertOne(item) for item in items_without_id]
        else:
            items_without_id = data.to_dict(orient="records", into=NotNanDict)
            operations = [InsertOne(item) for item in items_without_id]

        result = self._coll.bulk_write(operations, ordered=False)

        if result.acknowledged:
            ins = result.inserted_count
            matched = result.matched_count
            ups = result.upserted_count
            mod = result.modified_count

            logger.success(
                f"[{self._coll.name}] Inserted {ins}, matched {matched}, modified {mod}, upserted {ups} documents of {len(data)}."
            )
        else:
            logger.error(f"[{self._coll.name}] Bulk write unacknowledged.")

    def find(self, query=None, ids=False) -> pd.DataFrame:
        if query is None:
            query = {}

        t0 = time.monotonic()
        result = pd.DataFrame(
            self._coll.find(query, projection={"_id": False})
            if not ids
            else self._coll.find(query)
        )
        logger.info(
            f"[{self._coll.name}] Query '{query}' resulted in "
            f"{len(result)} results in {time.monotonic() - t0:.2f}s."
        )
        return result.convert_dtypes()

    def find_max_value(self, key: str):
        return next(self._coll.find({}).sort(key, -1).limit(1))[key]

    def delete(self, query):
        result = self._coll.delete_many(query)
        logger.info(
            f"[{self._coll.name}] Query '{query}' resulted in {result.deleted_count} deleted documents."
        )
