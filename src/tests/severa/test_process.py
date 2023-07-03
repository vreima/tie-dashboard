import datetime

import arrow
import pandas as pd
import pytest
from pytest import approx

import src.severa.process as process
from src.daterange import DateRange


@pytest.fixture
def empty_dataframe():
    return pd.DataFrame(
        {
            "id": pd.Series(),
            "start_date": pd.Series(),
            "end_date": pd.Series(),
            "date": pd.Series(),
            "forecast_date": pd.Series(),
            "value": pd.Series(),
        }
    )


@pytest.fixture
def forecast_dataframe(empty_dataframe):
    return pd.concat(
        [
            empty_dataframe,
            pd.DataFrame(
                {
                    "id": "id",
                    "start_date": arrow.get("2023-01-01").datetime,
                    "end_date": arrow.get("2023-01-10").datetime,
                    "date": pd.NaT,
                    "forecast_date": arrow.get("2023-01-01").datetime,
                    "value": 10.0,
                },
                index=[0],
            ),
        ]
    )


@pytest.fixture
def forecast_dataframe_large(empty_dataframe):
    return pd.concat(
        [
            empty_dataframe,
            pd.DataFrame(
                {
                    "id": ["id_1", "id_2"],
                    "start_date": arrow.get("2023-01-01").datetime,
                    "end_date": arrow.get("2023-01-10").datetime,
                    "date": pd.NaT,
                    "forecast_date": arrow.get("2023-01-01").datetime,
                    "value": 10.0,
                },
                index=[0, 1],
            ),
        ]
    )


@pytest.fixture
def realized_dataframe(empty_dataframe):
    return pd.concat(
        [
            empty_dataframe,
            pd.DataFrame(
                {
                    "id": "id",
                    "start_date": pd.NaT,
                    "end_date": pd.NaT,
                    "date": arrow.get("2023-01-01").datetime,
                    "forecast_date": arrow.get("2023-01-01").datetime,
                    "value": 1.0,
                },
                index=[0],
            ),
        ]
    )


class TestProcess:
    def test_sanitize_dates(self):
        data = pd.DataFrame(
            data={
                "start_date": [
                    arrow.get("2023-01-01T10:30+02:00").datetime,
                    arrow.get("2023-01-01").naive,
                    pd.NaT,
                ],
                "end_date": [
                    arrow.get("2023-01-10T10:30+04:00").datetime,
                    arrow.get("2023-01-10").naive,
                    pd.NaT,
                ],
                "date": [pd.NaT, pd.NaT, arrow.get("2023-01-7").naive],
                "value": 10.0,
            },
            index=[0, 1, 2],
        )

        cols = ["start_date", "end_date", "date"]

        result = process.sanitize_dates(data, ["start_date", "end_date", "date"])

        for col in cols:
            assert pd.api.types.is_datetime64tz_dtype(result[col])
            assert result[col].dt.tz is datetime.timezone.utc

    def test_unravel_with_forecasts(self, forecast_dataframe):
        result = process.unravel(forecast_dataframe)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10
        assert result.loc[:, "value"].values == approx(1.0)

    def test_unravel_without_forecasts(self, realized_dataframe):
        result = process.unravel(realized_dataframe)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.loc[:, "value"].values == approx(1.0)

    def test_unravel_with_maximum(self):
        data = pd.DataFrame(
            data={
                "id": "maximum",
                "start_date": None,
                "end_date": None,
                "date": None,
                "value": 10.0,
                "forecast_date": arrow.get("2023-01-05").datetime,
            },
            index=[0],
        )

        result = process.unravel(
            data, arrow.get("2023-01-01").datetime, arrow.get("2023-01-10").datetime
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10
        assert result.loc[:, "value"].values == approx(1.0)

    def test_cull_one_id(self, forecast_dataframe_large):
        unraveled = process.unravel(forecast_dataframe_large)

        result = process.cull_before(unraveled, arrow.get("2023-01-05"), ["id_1"])

        assert isinstance(result, pd.DataFrame)

        assert len(result[result.id == "id_1"]) == 6
        assert min(result[result.id == "id_1"].date.dt.day) == 5
        assert max(result[result.id == "id_1"].date.dt.day) == 10

        assert len(result[result.id != "id_1"]) == 10
        assert min(result[result.id != "id_1"].date.dt.day) == 1
        assert max(result[result.id != "id_1"].date.dt.day) == 10

    def test_cull_uninclusive(self, forecast_dataframe):
        unraveled = process.unravel(forecast_dataframe)

        result = process.cull_before(
            unraveled, arrow.get("2023-01-05"), ["id"], inclusive=False
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert min(result.date.dt.day) == 6
        assert max(result.date.dt.day) == 10

    def test_cull_multiple_ids(self, forecast_dataframe_large):
        unraveled = process.unravel(forecast_dataframe_large)

        result = process.cull_before(
            unraveled, arrow.get("2023-01-05"), ["id_1", "id_2"]
        )

        assert isinstance(result, pd.DataFrame)

        assert len(result[result.id == "id_1"]) == 6
        assert min(result[result.id == "id_1"].date.dt.day) == 5
        assert max(result[result.id == "id_1"].date.dt.day) == 10

        assert len(result[result.id != "id_1"]) == 6
        assert min(result[result.id != "id_1"].date.dt.day) == 5
        assert max(result[result.id != "id_1"].date.dt.day) == 10

    def test_cull_no_matching_id(self, forecast_dataframe_large):
        unraveled = process.unravel(forecast_dataframe_large)

        result = process.cull_before(
            unraveled, arrow.get("2023-01-05"), ["id_3", "id_8"]
        )

        assert (result == unraveled).all().all()
