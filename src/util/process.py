import datetime
import re
from itertools import product

import arrow
import pandas as pd
from workalendar.europe import Finland


def sanitize_dates(data: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    """
    Convert all date columns to UTC and handle NaT values.
    """
    for column in date_columns:
        data[column] = pd.to_datetime(data.loc[:, column], utc=True)

    return data


def unravel_subset(data_subset: pd.DataFrame) -> pd.DataFrame:
    mask = data_subset.date.isna() & ~(
        data_subset["start_date"].isna() | data_subset["end_date"].isna()
    )

    data = data_subset[mask]

    if len(data) < 1:
        return data_subset

    # Calculate dates and workdays
    calendar = Finland()
    data.loc[:, "date"] = data.loc[:, :].apply(
        lambda x: pd.date_range(start=x["start_date"], end=x["end_date"]), axis=1
    )

    data.loc[:, "_num_workdays"] = data.loc[:, :].apply(
        lambda x: calendar.get_working_days_delta(
            x["start_date"].date(), x["end_date"].date(), include_start=True
        ),
        axis=1,
    )

    # Unravel daterange into rows
    unraveled = data.explode("date").reset_index(drop=True)
    unraveled["_is_workday"] = unraveled.date.map(calendar.is_working_day).map(int)

    # Scale values to workdays
    unraveled["value"] = (
        unraveled["value"] / unraveled["_num_workdays"] * unraveled["_is_workday"]
    )

    return pd.concat(
        [
            unraveled.drop(["_num_workdays", "_is_workday"], axis=1),
            data_subset[~mask],
        ],
        ignore_index=True,
    )


def unravel(
    data: pd.DataFrame,
    date_span_start: datetime.datetime = None,
    date_span_end: datetime.datetime = None,
) -> pd.DataFrame:
    date_cols = ["start_date", "end_date", "date", "forecast_date"]
    for date_col in date_cols:
        if date_col not in data.columns:
            data[date_col] = pd.NaT

    data_view = sanitize_dates(
        data.copy(), ["start_date", "end_date", "date", "forecast_date"]
    )

    min_date = data_view.loc[:, ["date", "start_date"]].min().min()
    max_date = data_view.loc[:, ["date", "end_date"]].max().max()

    if pd.isna(min_date):
        if date_span_start is None:
            raise ValueError("No start date for unravel")
        else:
            min_date = date_span_start

    if pd.isna(max_date):
        if date_span_end is None:
            raise ValueError("No end date for unravel")
        else:
            max_date = date_span_end

    data_view.loc[data_view.id == "maximum", "start_date"] = pd.Timestamp(min_date)
    data_view.loc[data_view.id == "maximum", "end_date"] = pd.Timestamp(max_date)

    calendar = Finland()
    data_view.loc[data_view.id == "maximum", "value"] = data_view.loc[
        data_view.id == "maximum", :
    ].apply(
        lambda x: x["value"]
        * calendar.get_working_days_delta(
            x["start_date"].date(), x["end_date"].date(), include_start=True
        ),
        axis=1,
    )

    return unravel_subset(data_view).convert_dtypes()


def cull_before(
    data: pd.DataFrame, date: arrow.Arrow, ids: list[str], inclusive: bool = True
):
    """
    Cull forescasts that extend to times before date. Includes date if inclusive == True.
    """
    id_match = True if ids is None else data.id.isin(ids)
    is_realized_date_item = data.get("start_date").isna() & data.get("end_date").isna()

    date_filter = (
        (data.date >= date.datetime) if inclusive else (data.date > date.datetime)
    )

    return data[~id_match | (id_match & (is_realized_date_item | date_filter))]


def search_string_for_datetime(input_str: str) -> arrow.Arrow | None:
    """
    Search a string for a datetime value using some common abbreviated formats.
    Return None if no datetimes are found.
    """
    date_formats = [
        "DD.MM.YYYY",
        "DD.M.YYYY",
        "D.MM.YYYY",
        "D.M.YYYY",
        "DD-MM-YYYY",
        "D-M-YYYY",
        "YYYY-MM-DD",
        "YYYY-M-DD",
        "YYYY-MM-D",
        "YYYY-M-D",
    ]

    time_formats = [
        "HH:mm",
        "H:mm",
        "HH:m",
        "H:m",
        "HH.mm",
        "H.mm",
        "HH.m",
        "H.m",
        "HH",
        "H",
        "",
    ]

    mid_regex = r"[\s?(klo)?\.?\s?]"
    date_formats_with_implicit_year = [fmt.replace("YYYY", "") for fmt in date_formats]

    # Cleanup
    input_str = input_str.replace("*", "")

    this_year = arrow.now("Europe/Helsinki").year

    for d_fmt, t_fmt in product(date_formats, time_formats):
        try:
            fmt = rf"{d_fmt}{mid_regex}{t_fmt}"
            return arrow.get(
                input_str, fmt, tzinfo="Europe/Helsinki", normalize_whitespace=True
            )
        except arrow.parser.ParserError:
            continue
        except ValueError:
            continue

    for d_fmt, t_fmt in product(date_formats_with_implicit_year, time_formats):
        try:
            return arrow.get(
                input_str,
                rf"{d_fmt}{mid_regex}{t_fmt}",
                tzinfo="Europe/Helsinki",
                normalize_whitespace=True,
            ).replace(year=this_year)
        except arrow.parser.ParserError:
            continue
        except ValueError:
            continue

    match = re.search(r"vko (\d+)", input_str)
    if match:
        vko = match.group(1)
        return arrow.get(f"{this_year}-W{vko:0>2}-5T12:00", tzinfo="Europe/Helsinki")

    return None
