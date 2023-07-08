import arrow
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from loguru import logger

from src.database.database import Base
from src.logic.severa.client import Client
from src.util.daterange import DateRange
from src.util.process import cull_before, sanitize_dates, unravel

router = APIRouter(prefix="/kpi", tags=["kpi"])

templates = Jinja2Templates(directory="src/static")


@router.get("/salesmargin")
async def get_salesmargin(request: Request):
    return templates.TemplateResponse(
        "kpi_template.html",
        {"request": request, "base_url": request.base_url, "kpi": "salesmargin"},
    )


@router.get("/salesmargin.json")
async def get_salesmargin_data(request: Request):  # noqa: ARG001
    return (await sales_margin_db()).to_dict(orient="records")


@router.get("/hours.json")
async def get_hours_data(request: Request):  # noqa: ARG001
    return (await hours()).to_dict(orient="records")


#
# *****
#


async def hours() -> pd.DataFrame:
    today = arrow.utcnow()
    start = today.shift(months=-4).floor("month")
    end = today.shift(months=2).ceil("month")

    span = DateRange(start, end)

    async with Client() as client:
        users = await client.users()
        hours = await client.fetch_hours(span)

    username_by_user = {user.guid: user.firstName for user in users}

    total_hours = (
        cull_before(
            unravel(hours, start.datetime, end.datetime),
            today.floor("day"),
            ["workhours", "saleswork"],  # absences?
            inclusive=False,
        )
        .groupby(["user", "id", "date", "productive", "project"], dropna=False)["value"]
        .sum()
        .reset_index()
    )

    total_hours = total_hours[total_hours.date.between(start.datetime, end.datetime)]

    total_hours.loc[total_hours.id == "absences", "productive"] = False

    total_hours["username"] = total_hours.user.map(username_by_user)

    return total_hours


async def sales_margin_db() -> pd.DataFrame:
    today = arrow.utcnow()
    start = today.shift(months=-4).floor("month")
    end = today.shift(months=2).ceil("month")

    span_past, span_future = DateRange(start, end).cut(today)

    async with Client() as client:
        users = await client.users()
        hours = await client.fetch_hours(span_past)
        billing = await client.fetch_billing(span_past)
        # salesval = await f.fetch_salesvalue(span)
        # sales = await f.fetch_billing(span)

    # us = {user.guid: user.firstName for user in await client.users()}
    logger.debug("Fetching done")
    cost_by_user = {user.guid: user.workContract.hourCost.amount for user in users}
    username_by_user = {user.guid: user.firstName for user in users}

    base = Base("kpi-dev-02", "billing")
    billing_f = base.find({"forecast_date": today.floor("day").datetime})

    logger.debug("Billing done")

    base = Base("kpi-dev-02", "hours")
    hours_f = base.find({"forecast_date": today.floor("day").datetime})

    logger.debug("Fetching hours done")

    total_billing = (
        cull_before(
            unravel(pd.concat([billing, billing_f], ignore_index=True)),
            today.floor("day"),
            ["workhours", "billing"],
            inclusive=False,
        )
        .groupby(["user", "id", "date"], dropna=False)["value"]
        .sum()
        .reset_index()
    )

    logger.debug("Total billing done")

    cols = list(set(hours.columns) & set(hours_f.columns))
    h1 = sanitize_dates(hours, ["date", "start_date", "end_date", "forecast_date"])
    h2 = sanitize_dates(hours_f, ["date", "start_date", "end_date", "forecast_date"])
    h3 = h1.merge(h2, on=cols, how="outer")

    logger.debug("Hours merged")

    total_hours = (
        cull_before(
            unravel(h3, start.datetime, end.datetime),
            today.floor("day"),
            ["workhours", "saleswork"],  # absences?
            inclusive=False,
        )
        .groupby(["user", "id", "date"], dropna=False)["value"]
        .sum()
        .reset_index()
    )

    logger.debug("Total hours done")

    combined = pd.concat(
        [total_billing[total_billing.value != 0], total_hours],
        ignore_index=True,
    )
    combined = combined[combined.date.between(start.datetime, end.datetime)]

    logger.debug("Combination done")

    realized_total_hours = (
        combined[
            (combined.date <= today.datetime)
            & (combined.id.isin(["workhours", "absences"]))
        ]
        .groupby(["user", "date"])["value"]
        .sum()
        .reset_index()
        .copy()
    )

    forecasted_total_hours = (
        combined[(combined.date > today.datetime) & (combined.id.isin(["maximum"]))]
        .groupby(["user", "date"])["value"]
        .sum()
        .reset_index()
        .copy()
    )

    cost = pd.concat([realized_total_hours, forecasted_total_hours], ignore_index=True)

    cost["id"] = "cost"
    cost["hourly_cost"] = cost.user.map(cost_by_user)
    cost["value"] = cost.value * cost.hourly_cost

    logger.debug("Cost calc done")

    result = pd.concat([combined[combined.id == "billing"], cost], ignore_index=True)

    result["username"] = result.user.map(username_by_user)

    return result
