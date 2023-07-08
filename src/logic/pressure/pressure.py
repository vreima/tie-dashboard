from datetime import datetime

import arrow
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from src.database.database import Base

templates = Jinja2Templates(directory="src/static")


class PressureReadingModel(BaseModel):
    user: str
    date: datetime
    x: float
    y: float


async def fetch_pressure(
    start: arrow.Arrow, end: arrow.Arrow, users: list[str] | None
) -> list[PressureReadingModel]:
    """
    Fetch 'kiirekysely' results from the database.
    """
    filter_query = {"date": {"$gte": start.datetime, "$lte": end.datetime}}

    if users:
        filter_query["user"] = {"$in": users}

    # TODO: businessunits

    return [
        PressureReadingModel(**item)
        for item in Base("pressure", "pressure")
        .find(filter_query)
        .to_dict(orient="records")
    ]
