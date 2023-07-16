import operator
from enum import Enum
from typing import Annotated

import arrow
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from workalendar.europe import Finland

from src.util.daterange import DateRange

DateTimeUTCType = Annotated[pd.DatetimeTZDtype, "ns", "utc"]


class ForecastInputModel(pa.DataFrameModel):
    forecast_date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)
    value: Series[float] = pa.Field(ignore_na=False, nullable=False)


class PartiallySpannedModel(pa.DataFrameModel):
    start_date: Series[DateTimeUTCType] | None = pa.Field(
        ignore_na=False, nullable=True
    )
    end_date: Series[DateTimeUTCType] | None = pa.Field(ignore_na=False, nullable=True)


class FullySpannedModel(pa.DataFrameModel):
    start_date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)
    end_date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)


class UnraveledSpanModel(FullySpannedModel):
    date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)


class WeekdayCalculationOutputModel(UnraveledSpanModel):
    _is_workday: Series[bool] = pa.Field(ignore_na=False, nullable=False)
    _num_workdays: Series[int] = pa.Field(ge=0, ignore_na=False, nullable=False)
    _num_all_days: Series[int] = pa.Field(ge=0, ignore_na=False, nullable=False)


class ValueRecalculationModel(WeekdayCalculationOutputModel):
    value: Series[float] = pa.Field(ignore_na=False, nullable=False)


class UnraveledRowInputModel(pa.DataFrameModel):
    date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)


class UnraveledForecastModel(ForecastInputModel):
    date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)


class UnraveledForecastWithLengthModel(UnraveledForecastModel):
    forecast_length: Series[int] = pa.Field(ge=0, ignore_na=False, nullable=False)


class BillingInputModel(UnraveledForecastModel):
    id: Series[str] = pa.Field(isin=["billing"], ignore_na=False, nullable=False)

    user: Series[str] = pa.Field(ignore_na=False, nullable=False)
    value: Series[float] = pa.Field(ignore_na=False, nullable=False, coerce=True)
    internal_guid: Series[str] = pa.Field(ignore_na=False, nullable=False)
    project: Series[str] = pa.Field(ignore_na=False, nullable=False)

    billing: Series[float] = pa.Field(
        ignore_na=False, nullable=True, default=0, coerce=True
    )
    expense: Series[float] = pa.Field(
        ignore_na=False, nullable=True, default=0, coerce=True
    )
    revenue: Series[float] = pa.Field(
        ignore_na=False, nullable=True, default=0, coerce=True
    )
    labor_expense: Series[float] = pa.Field(
        ignore_na=False, nullable=True, default=0, coerce=True
    )

    forecast_date: Series[DateTimeUTCType] = pa.Field(
        ignore_na=False, nullable=False, coerce=True
    )
    start_date: Series[DateTimeUTCType] | None = pa.Field(
        ignore_na=False, nullable=True, coerce=True
    )
    end_date: Series[DateTimeUTCType] | None = pa.Field(
        ignore_na=False, nullable=True, coerce=True
    )
    date: Series[DateTimeUTCType] | None = pa.Field(
        ignore_na=False, nullable=True, coerce=True
    )


class BillingOutputModel(UnraveledForecastModel):
    date: Series[DateTimeUTCType] = pa.Field(ignore_na=False, nullable=False)
    forecast_date: Series[DateTimeUTCType] = pa.Field(
        ignore_na=False, nullable=False, coerce=True
    )
    forecast_length: Series[int] = pa.Field(ge=0, ignore_na=False, nullable=False)
    user: Series[str] = pa.Field(ignore_na=False, nullable=False)
    value: Series[float] = pa.Field(ignore_na=False, nullable=False)
    project: Series[str] = pa.Field(ignore_na=False, nullable=False)


# custom checks: https://pandera.readthedocs.io/en/stable/extensions.html


@pa.check_types(lazy=True)
def expand_start_and_end(
    df: DataFrame[PartiallySpannedModel], maximum_span: DateRange
) -> DataFrame[FullySpannedModel]:
    if "start_date" not in df.columns:
        df["start_date"] = maximum_span.start.datetime
    else:
        df["start_date"] = df["start_date"].fillna(maximum_span.start.datetime)

    if "end_date" not in df.columns:
        df["end_date"] = maximum_span.end.datetime
    else:
        df["end_date"] = df["end_date"].fillna(maximum_span.end.datetime)

    return df


@pa.check_types(lazy=True)
def unravel(df: DataFrame[FullySpannedModel]) -> DataFrame[UnraveledSpanModel]:
    dates_for_unraveling: Series[pd.DatetimeIndex] = df.apply(
        lambda x: pd.date_range(start=x["start_date"], end=x["end_date"], freq="D"),
        axis=1,
    )

    df["_date"] = dates_for_unraveling
    unraveled = df.explode("_date").reset_index(drop=True)
    unraveled["date"] = unraveled["_date"]
    return unraveled


@pa.check_types(lazy=True)
def calculate_weekday_statistics(
    df: DataFrame[UnraveledSpanModel],
) -> DataFrame[WeekdayCalculationOutputModel]:
    calendar = Finland()

    number_of_days = df.apply(
        lambda x: (x["end_date"] - x["start_date"]).days,
        axis=1,
    )

    number_of_workdays = df.apply(
        lambda x: calendar.get_working_days_delta(
            x["start_date"].date(), x["end_date"].date(), include_start=True
        ),
        axis=1,
    )

    is_workday = df["date"].map(calendar.is_working_day)

    df["_is_workday"] = is_workday
    df["_num_all_days"] = number_of_days
    df["_num_work_days"] = number_of_workdays

    return df


class ValueRecalculationMethod(Enum):
    SCALE_VALUE_TO_NUMBER_OF_WORKDAYS = 1
    SCALE_VALUE_TO_NUMBER_OF_ALL_DAYS = 2


@pa.check_types(lazy=True)
def recalculate_values(
    df: DataFrame[ValueRecalculationModel],
    recalculation_method: ValueRecalculationMethod,
    set_holidays_to_zero: bool = True,
) -> DataFrame[ValueRecalculationModel]:
    scale_factor: Series[int] | int = 1

    if (
        recalculation_method
        is ValueRecalculationMethod.SCALE_VALUE_TO_NUMBER_OF_ALL_DAYS
    ):
        scale_factor = 1.0 / df["_num_all_days"]
    elif (
        recalculation_method
        is ValueRecalculationMethod.SCALE_VALUE_TO_NUMBER_OF_WORKDAYS
    ):
        scale_factor = 1.0 / df["_num_work_days"]
    else:
        raise ValueError(f"unrecognized value for method: '{recalculation_method}'")

    df["value"] *= scale_factor

    if set_holidays_to_zero:
        df.loc[~df["_is_workday"], "value"] = 0

    return df


@pa.check_types(lazy=True)
def cull_spanned(
    df: DataFrame[UnraveledSpanModel], last_date_to_cull: arrow.Arrow
) -> DataFrame[UnraveledSpanModel]:
    return df[df["date"] > last_date_to_cull.datetime]


@pa.check_types(lazy=True)
def cull(
    df: DataFrame[UnraveledRowInputModel], maximum_span: DateRange
) -> DataFrame[UnraveledRowInputModel]:
    return df[
        df["date"].between(
            left=maximum_span.start.datetime, right=maximum_span.end.datetime
        )
    ]


@pa.check_types(lazy=True)
def calculate_forecast_length(
    df: DataFrame[UnraveledForecastModel],
) -> DataFrame[UnraveledForecastWithLengthModel]:
    forecast_length = df["date"] - df["forecast_date"]

    df["forecast_length"] = forecast_length.map(operator.attrgetter("days"))

    return df[df["forecast_length"] >= 0]


@pa.check_types(lazy=True)
def process_billing_forecasts(
    df: DataFrame[BillingInputModel],
) -> DataFrame[BillingOutputModel]:
    span_min = df["start_date"].min()
    span_max = df["end_date"].max()

    expanded = expand_start_and_end(df, DateRange(span_min, span_max))
    unravaled = unravel(expanded)
    unraveled_with_stats = calculate_weekday_statistics(unravaled)
    recalculated = recalculate_values(
        unraveled_with_stats,
        ValueRecalculationMethod.SCALE_VALUE_TO_NUMBER_OF_WORKDAYS,
        set_holidays_to_zero=True,
    )
    recalculated_with_span = calculate_forecast_length(recalculated)

    return recalculated_with_span.drop(
        [
            "_date",
            "_is_workday",
            "_num_all_days",
            "_num_work_days",
            "start_date",
            "end_date",
            "internal_guid",
            "billing",
            "revenue",
            "expense",
            "labor_expense",
        ],
        axis=1,
    )
