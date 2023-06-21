import arrow
import pandas as pd
from workalendar.europe import Finland


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
    data: pd.DataFrame, date_span_start=None, date_span_end=None
) -> pd.DataFrame:
    data_view = data.copy()

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


def cull_before(data: pd.DataFrame, date: arrow.Arrow, ids: list[str]):
    """
    Cull forescasts that extend to times before date.
    """
    id_match = True if ids is None else data.id.isin(ids)
    is_realized_date_item = data.get("start_date").isna() & data.get("end_date").isna()

    return data[
        ~id_match | (id_match & (is_realized_date_item | (data.date >= date.datetime)))
    ]
