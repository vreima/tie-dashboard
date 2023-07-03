import json

import arrow
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from src.daterange import DateRange
from src.severa.client import Client
from src.severa.process import cull_before, unravel

router = APIRouter()

templates = Jinja2Templates(directory="src/static")


@router.get("/billing")
async def get_billing(request: Request):
    return templates.TemplateResponse(
        "pre.html",
        {
            "request": request,
            "text": (await billing()).to_json(orient="records", indent=4),
        },
    )


@router.get("/get/billing")
async def get_billing_data(request: Request):
    return (await billing()).to_dict(orient="records")


#
# *****
#


async def sales_margin() -> pd.DataFrame:
    today = arrow.utcnow()
    start = today.shift(months=0).floor("month")
    end = today.shift(months=2).ceil("month")

    async with Client() as client:
        users = await client.users()
        hours = await client.fetch_hours(DateRange(start, end))
        billing = await client.fetch_billing(DateRange(start, end))

    unraveled = unravel(pd.concat([hours, billing], ignore_index=True), start, end)
    culled = cull_before(unraveled, today, ["workhours", "billing"], inclusive=False)

    return culled
