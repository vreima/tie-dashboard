import arrow
import pandas as pd
from loguru import logger

from src.database.database import Base
from src.logic.severa.client import Client
from src.util.daterange import DateRange
from src.util.process import cull_before, sanitize_dates, unravel

# async def totals(start: arrow.Arrow, end: arrow.Arrow) -> pd.DataFrame:
#     data = await kpi.hours_totals(span.start, span.end)
#     logger.debug("\n" + str(data))
#     return data.to_dict(orient="records")


async def hours(start: arrow.Arrow, end: arrow.Arrow) -> pd.DataFrame:
    today = arrow.utcnow()
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


async def hours_totals(start: arrow.Arrow, end: arrow.Arrow) -> pd.DataFrame:
    """ """
    data = await hours(start, end)

    data.loc[data.id == "maximum", "productive"] = False

    table = data.pivot_table(
        "value",
        index=["username"],
        columns=["id", "productive"],
        aggfunc="sum",
        fill_value=0,
        dropna=False,
        margins=True,
    )

    add = table.loc[
        :,
        [
            i
            for i in [
                ("absences", False),
                ("maximum", False),
                ("saleswork", False),
                ("workhours", True),
                ("workhours", False),
            ]
            if i in table.columns
        ],
    ].copy()
    add["workhours, unproductive"] = add[("workhours", False)]
    add["workhours, productive"] = add[("workhours", True)]
    add = add.drop("workhours", axis=1)

    logger.debug("\n" + str(table))
    logger.debug(table.columns)

    logger.debug("res:\n" + str(add))
    logger.debug("res2:\n" + str(add.droplevel(1, axis="columns")))

    return add.droplevel(1, axis="columns")


async def sales_margin_totals(start: arrow.Arrow, end: arrow.Arrow) -> pd.DataFrame:
    """ """
    data = await sales_margin(start, end)
    return data.pivot_table(
        "value",
        index=["username"],
        columns="id",
        aggfunc="sum",
        fill_value=0,
        dropna=False,
        margins=True,
    ).reset_index()


def unravel_and_cull(  # noqa: PLR0913
    data: pd.DataFrame,
    culling_columns: list[str],
    culling_date: arrow.Arrow | None = None,
    groupby: list[str] | None = None,
    start: arrow.Arrow | None = None,
    end: arrow.Arrow | None = None,
):
    """
    Helper function.
    """
    culling_date = culling_date or arrow.utcnow.floor("day")
    groupby = groupby or ["user", "id", "date"]

    return (
        cull_before(
            unravel(data, start.datetime, end.datetime),
            culling_date,
            culling_columns,
            inclusive=False,
        )
        .groupby(groupby, dropna=False)["value"]
        .sum()
        .reset_index()
    )


async def sales_margin(start: arrow.Arrow, end: arrow.Arrow) -> pd.DataFrame:
    today = arrow.utcnow()
    span_past, span_future = DateRange(start, end).cut(today)

    async with Client() as client:
        users = await client.users()
        hours = await client.fetch_hours(span_past)
        billing = await client.fetch_billing(span_past)
        # salesval = await f.fetch_salesvalue(span)

    cost_by_user = {user.guid: user.workContract.hourCost.amount for user in users}
    username_by_user = {user.guid: user.firstName for user in users}

    base = Base("kpi-dev-02", "billing")
    latest_date = base.find_max_value("forecast_date")
    logger.warning(f"{latest_date=}")
    billing_f = base.find({"forecast_date": latest_date})

    base = Base("kpi-dev-02", "hours")
    hours_f = base.find({"forecast_date": latest_date})

    total_billing = unravel_and_cull(
        pd.concat([billing, billing_f], ignore_index=True),
        culling_columns=["workhours", "billing"],
        groupby=["user", "id", "date"],
    )

    cols = list(set(hours.columns) & set(hours_f.columns))
    h1 = sanitize_dates(hours, ["date", "start_date", "end_date", "forecast_date"])
    h2 = sanitize_dates(hours_f, ["date", "start_date", "end_date", "forecast_date"])
    h3 = h1.merge(h2, on=cols, how="outer")

    total_hours = unravel_and_cull(
        h3,
        culling_columns=["workhours", "saleswork"],
        groupby=["user", "id", "date"],
        start=start,
        end=end,
    )

    combined = pd.concat(
        [total_billing[total_billing.value != 0], total_hours],
        ignore_index=True,
    )
    combined = combined[combined.date.between(start.datetime, end.datetime)]

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

    result = pd.concat(
        [combined[combined.id == "billing"], cost.drop("hourly_cost", axis=1)],
        ignore_index=True,
    )

    result["username"] = result.user.map(username_by_user)

    return result
