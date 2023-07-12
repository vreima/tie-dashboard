import asyncio
from datetime import datetime
from functools import partial
from typing import Any, Self
from collections.abc import Sequence

import arrow
import pandas as pd
from loguru import logger
from pandas.api.types import CategoricalDtype
from workalendar.europe import Finland

import src.logic.severa.client
from src.database.database import Base
from src.util.daterange import DateRange


class ProcessData:
    """
    A class for processing and unraveling data with 'start_date' and 'end_date' into
    daily rows (as 'date'). Leaves rows with a set 'date' value untouched.
    """

    def __init__(self, data: pd.DataFrame):
        self.data = data
        self.unraveled = pd.DataFrame()

        self.min_date_in_data = partial(
            self._aggregate_date, aggregate=pd.DataFrame.min
        )
        self.max_date_in_data = partial(
            self._aggregate_date, aggregate=pd.DataFrame.max
        )

    def validate_data(self) -> Self:
        """
        Overridable. Run all pre-unravel validations.
        """
        return self

    def _ensure_columns(self, columns_and_dtypes: dict[str, Any]) -> Self:
        """
        Ensure that the data DF contains specified columns and their dtypes match.
        """
        for column, dtype in columns_and_dtypes.items():
            if column not in self.data.columns:
                self.data[column] = pd.Series(dtype=dtype)
            elif pd.Series(None, dtype=dtype).dtype == pd.DatetimeTZDtype(tz="UTC"):
                self.data[column] = pd.to_datetime(self.data[column], utc=True)
            else:
                self.data[column] = self.data[column].astype(dtype)

            if column not in self.unraveled.columns:
                self.unraveled[column] = pd.Series(dtype=dtype)
            else:
                self.unraveled[column] = self.data[column].astype(dtype)

        return self

    def _ensure_value_order(self, min_column: str, max_column: str) -> Self:
        """
        If data[min_column] > data[max_column], swap the values. For example,
        ensure that start_date is always before end_date. Leaves null values as
        they are.
        """
        mask = self.data[min_column] > self.data[max_column]
        self.data.loc[mask, [min_column, max_column]] = self.data.loc[
            mask, [max_column, min_column]
        ].to_numpy()

        return self

    def _aggregate_date(
        self,
        date_columns: list[str],
        fallback: datetime | None,
        aggregate=pd.DataFrame.min,
    ):
        """
        Find minimun/maximum/mean/etc date in data, or use fallback.
        """

        # Might be maximum or mean or whatever, so naming is a bit off.
        # Double aggregation for first row-wise, then column-wise.
        minimum_in_data = aggregate(
            aggregate(
                self.data.loc[
                    :, [col for col in date_columns if col in self.data.columns]
                ],
            )
        )

        if pd.isna(minimum_in_data):
            if fallback is None:
                raise ValueError("No start date for unravel")

            return fallback

        return minimum_in_data

    def prepare_unravel(self, date_span_start: datetime, date_span_end: datetime):
        """
        Prepare data for unraveling, handle special ids etc.

        Example: id "maximum" in hours denotes maximum working hours of a person
        according to their working contract. It is a number, not tied to a date
        or a date span.

        """
        pass

    def data_to_unravel(self) -> Sequence[bool] | bool:
        """
        A boolean series for selecting rows to unravel. Defaults to rows with
        null/NA/NaT in the 'date' column. Overridable.
        """
        return pd.Series(self.data.get("date", pd.NaT), index=self.data.index).isna()

    def columns_to_unravel(self) -> Sequence[str]:
        """
        Names of value columns in the data. Defaults to 'value'. Overridable.
        """
        return ["value"]

    def _cull_past_forecasts(
        self,
        cullable_rows_mask=True,
        cutoff_date: datetime | None = None,
        keep_cutoff_date: bool = True,
    ) -> Self:
        """
        Remove rows that represent portions of unraveled forecast that are already
        happened. Ran after unravel. Cutoff_date defaults to today.
        """
        if self.unraveled.empty:
            logger.warning("cull_past_forecasts() maybe called before unravel().")
            return self

        if cutoff_date is None:
            cutoff_date = arrow.utcnow().floor("day").datetime

        if isinstance(cullable_rows_mask, bool):
            cullable_rows_mask = pd.Series(
                cullable_rows_mask, index=self.unraveled.index
            )

        is_realized_date_item = (
            self.unraveled.get("start_date").isna()
            & self.unraveled.get("end_date").isna()
        )

        dates_to_keep_filter = (
            (self.unraveled.date >= cutoff_date)
            if keep_cutoff_date
            else (self.unraveled.date > cutoff_date)
        )

        self.unraveled = self.unraveled[
            ~cullable_rows_mask
            | (cullable_rows_mask & (is_realized_date_item | dates_to_keep_filter))
        ]

        return self

    def cull_to_span(self, date_span_start: datetime, date_span_end: datetime) -> Self:
        """
        Overridable. Removes rows with 'date' outside of specified date frame. Ran after unravel.
        """
        if self.unraveled.empty:
            logger.warning("cull() maybe called before unravel().")
            return self

        self.unraveled = self.unraveled[
            self.unraveled.date.between(date_span_start, date_span_end)
        ]

        return self

    def unravel(
        self, date_span_start: datetime, date_span_end: datetime  # noqa: ARG002
    ) -> Self:
        """
        Overridable. Unravel date (by calling _unravel()).
        """
        return self

    def _unravel(  # noqa: PLR0913
        self,
        date_span_start: datetime,
        date_span_end: datetime,
        set_to_zero_if_on_holiday_mask=True,
        scale_with_number_of_days=True,
        scale_with_number_of_workdays=True,
    ) -> Self:
        """
        Unravel data with {start_date, end_date, value} into daily
        values with [{date, value}, {date, value}, ...]. Values are either
        copied straight or interpolated by either the number of days or the number
        of working days in the span.
        """
        min_date: datetime = self.min_date_in_data(
            ["date", "start_date"],
            fallback=date_span_start,
        )
        max_date: datetime = self.max_date_in_data(
            ["date", "end_date"],
            fallback=date_span_end,
        )

        self.prepare_unravel(min_date, max_date)

        unravel_mask = self.data_to_unravel()

        if self.data.loc[unravel_mask, :].empty:
            return self

        # Set '_date' column to a daily series
        dbg_mask = (
            self.data.loc[unravel_mask, "start_date"].isna()
            | self.data.loc[unravel_mask, "end_date"].isna()
        )
        if not dbg_mask.empty:
            logger.error(self.data.loc[unravel_mask & dbg_mask, :])
        self.data.loc[unravel_mask, "_date"] = self.data.loc[unravel_mask, :].apply(
            lambda x: pd.date_range(start=x["start_date"], end=x["end_date"], freq="D"),
            axis=1,
        )

        # Calculate and save the number of working days in span
        calendar = Finland()
        self.data.loc[unravel_mask, "_num_days"] = self.data.loc[unravel_mask, :].apply(
            lambda x: (x["end_date"] - x["start_date"]).days,
            axis=1,
        )
        self.data.loc[unravel_mask, "_num_workdays"] = self.data.loc[
            unravel_mask, :
        ].apply(
            lambda x: calendar.get_working_days_delta(
                x["start_date"].date(), x["end_date"].date(), include_start=True
            ),
            axis=1,
        )

        self.data.loc[unravel_mask, "_pre_scale"] = 1
        self.data.loc[unravel_mask, "_zero_if_holiday"] = False
        self.data.loc[
            unravel_mask & set_to_zero_if_on_holiday_mask, "_zero_if_holiday"
        ] = True

        scale_with_days_mask = unravel_mask & scale_with_number_of_days
        scale_with_workdays_mask = unravel_mask & scale_with_number_of_workdays
        self.data.loc[scale_with_days_mask, "_pre_scale"] = self.data.loc[
            scale_with_days_mask, "_num_days"
        ]

        self.data.loc[scale_with_workdays_mask, "_pre_scale"] = self.data.loc[
            scale_with_workdays_mask, "_num_workdays"
        ]

        # The actual unravel
        unraveled = (
            self.data.loc[unravel_mask, :].explode("_date").reset_index(drop=True)
        )

        unraveled["_is_workday"] = unraveled["_date"].map(calendar.is_working_day)
        unraveled["date"] = unraveled["_date"]

        # Scale values to workdays
        for column in self.columns_to_unravel():
            unraveled[column] = (
                unraveled[column]
                / unraveled["_pre_scale"]
                * (~(unraveled["_zero_if_holiday"] & ~unraveled["_is_workday"])).map(
                    int
                )
            )

        # Reset
        self.data = self.data.drop(
            [col for col in self.data.columns if col.startswith("_")], axis="columns"
        )

        self.unraveled = pd.concat(
            [
                # unraveled.drop(["_num_workdays", "_is_workday", "_zero_if_holiday"], axis=1),
                unraveled,
                self.data[~unravel_mask],
            ],
            ignore_index=True,
        )

        return self

    def process(self, date_span_start: datetime, date_span_end: datetime) -> Self:
        a = self.validate_data()
        logger.warning(type(a))
        b = a.unravel(date_span_start, date_span_end)
        logger.warning(type(b))
        return b.cull_to_span(date_span_start, date_span_end)


class ProcessHours(ProcessData):
    def validate_data(self) -> Self:
        columns = {
            "user": str,
            "id": CategoricalDtype(categories=["absences", "workhours", "saleswork"]),
            "value": float,
            "internal_guid": str,
            "project": str,
            "phase": str,
            "activity_type": str,
            "date": "datetime64[ns, UTC]",
            "start_date": "datetime64[ns, UTC]",
            "end_date": "datetime64[ns, UTC]",
            "forecast_date": "datetime64[ns, UTC]",
            "productive": "boolean",  # nullable boolean
            "_id": str,
        }
        return (
            super()
            ._ensure_columns(columns)
            ._ensure_value_order("start_date", "end_date")
        )

    def unravel(
        self,
        date_span_start: datetime,
        date_span_end: datetime,
    ) -> Self:
        return super()._unravel(
            date_span_start,
            date_span_end,
            set_to_zero_if_on_holiday_mask=self.data.id == "absences",
            scale_with_number_of_days=self.data.id == "absences",
            scale_with_number_of_workdays=self.data.id.isin(["workhours", "saleswork"]),
        )

    def cull_to_span(self, date_span_start: datetime, date_span_end: datetime) -> Self:
        return (
            super()
            .cull_to_span(date_span_start, date_span_end)
            ._cull_past_forecasts(self.unraveled.id.isin(["workhours", "saleswork"]))
        )


class ProcessBilling(ProcessData):
    def validate_data(self) -> Self:
        columns = {
            "user": str,
            "id": CategoricalDtype(categories=["billing"]),
            "value": float,
            "internal_guid": str,
            "project": str,
            "date": "datetime64[ns, UTC]",
            "start_date": "datetime64[ns, UTC]",
            "end_date": "datetime64[ns, UTC]",
            "forecast_date": "datetime64[ns, UTC]",
            "billing": float,
            "expense": float,
            "revenue": float,
            "labor_expense": float,
            "_id": str,
        }
        return (
            super()
            ._ensure_columns(columns)
            ._ensure_value_order("start_date", "end_date")
        )

    def unravel(
        self,
        date_span_start: datetime,
        date_span_end: datetime,
    ) -> Self:
        x = super()._unravel(
            date_span_start,
            date_span_end,
            set_to_zero_if_on_holiday_mask=True,
            scale_with_number_of_days=False,
            scale_with_number_of_workdays=True,
        )
        logger.warning(f"billing / {type(x)=}")
        return x

    def cull_to_span(self, date_span_start: datetime, date_span_end: datetime) -> Self:
        return (
            super()
            .cull_to_span(date_span_start, date_span_end)
            ._cull_past_forecasts(self.unraveled.id.isin(["billing"]))
        )


class ProcessSales(ProcessData):
    def validate_data(self) -> Self:
        columns = {
            "user": str,
            "id": CategoricalDtype(categories=["salesvalue"]),
            "value": float,
            "internal_guid": str,
            "project": str,
            "date": "datetime64[ns, UTC]",
            "forecast_date": "datetime64[ns, UTC]",
            "sold_by": str,
            "_id": str,
        }
        return super()._ensure_columns(columns)


class ProcessUsers(ProcessData):
    def validate_data(self) -> Self:
        columns = {
            "user": str,
            "first_name": str,
            "last_name": str,
            "id": CategoricalDtype(categories=["daily_hours", "hour_cost"]),
            "business_unit": str,
            "value": float,
            "start_date": "datetime64[ns, UTC]",
            "end_date": "datetime64[ns, UTC]",
            "_id": str,
        }
        return (
            super()
            ._ensure_columns(columns)
            ._ensure_value_order("start_date", "end_date")
        )

    def unravel(
        self,
        date_span_start: datetime,
        date_span_end: datetime,
    ) -> Self:
        return super()._unravel(
            date_span_start,
            date_span_end,
            set_to_zero_if_on_holiday_mask=self.data.id == "daily_hours",
            scale_with_number_of_days=False,
            scale_with_number_of_workdays=False,
        )

    def prepare_unravel(self, date_span_start: datetime, date_span_end: datetime):
        super().prepare_unravel(date_span_start, date_span_end)

        # Set ending date to all the date that have a start, but no end
        no_end_date_mask = (
            ~self.data["start_date"].isna() & self.data["end_date"].isna()
        )
        self.data.loc[no_end_date_mask, "end_date"] = date_span_end

        # We might have start and end dates in wrong order in rare cases here
        self._ensure_value_order("start_date", "end_date")


async def load_and_merge(span: DateRange, forecasts_from_database: bool = True):
    if forecasts_from_database:
        span_severa, span_future = span.cut(arrow.utcnow())
    else:
        span_severa = span

    forecasts_from_database = forecasts_from_database and bool(span_future)

    dfs = []

    async with src.logic.severa.client.Client() as client:
        async with asyncio.TaskGroup() as tg:
            user_info = tg.create_task(client.fetch_all_user_information())
            if span_severa:
                billing_p = tg.create_task(client.fetch_billing(span_severa))
                hours_p = tg.create_task(client.fetch_hours(span_severa))
                sales_p = tg.create_task(client.fetch_sales(span_severa))

            if forecasts_from_database:
                base = Base("kpi-dev-02", "billing")
                latest_date = base.find_max_value("forecast_date")
                logger.warning(f"{latest_date=}")
                billing_f_task = base.find({"forecast_date": latest_date})

                base = Base("kpi-dev-02", "hours")
                hours_f_task = base.find({"forecast_date": latest_date})

    if span_severa:
        users = ProcessUsers(user_info.result()).process(
            span.start.datetime, span.end.datetime
        )
        hours = ProcessHours(hours_p.result()).process(
            span.start.datetime, span.end.datetime
        )
        billing = ProcessBilling(billing_p.result()).process(
            span.start.datetime, span.end.datetime
        )
        sales = ProcessSales(sales_p.result()).process(
            span.start.datetime, span.end.datetime
        )

        dfs = [users.unraveled, billing.unraveled, hours.unraveled, sales.unraveled]

    if forecasts_from_database:
        billing_f = ProcessBilling(billing_f_task).process(
            span.start.datetime, span.end.datetime
        )
        hours_f = ProcessHours(
            hours_f_task[hours_f_task.id != "maximum"].copy()
        ).process(span.start.datetime, span.end.datetime)
        dfs += [billing_f.unraveled, hours_f.unraveled]

    return pd.concat(
        dfs,
        ignore_index=True,
    )