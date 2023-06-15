from src.severa.client import Client
from src.daterange import DateRange

import pandas as pd
import pytest


class TestClient:
    @pytest.mark.asyncio
    async def test_hours_return_types(self):
        span = DateRange(4)

        async with Client() as client:
            assert isinstance(await client.fetch_realized_maximum(span), pd.DataFrame)
            assert isinstance(await client.fetch_forecasted_maximum(span), pd.DataFrame)
            assert isinstance(await client.fetch_realized_absences(span), pd.DataFrame)
            assert isinstance(
                await client.fetch_forecasted_absences(span), pd.DataFrame
            )
            assert isinstance(await client.fetch_realized_workhours(span), pd.DataFrame)
            assert isinstance(
                await client.fetch_forecasted_workhours(span), pd.DataFrame
            )
            assert isinstance(
                await client.fetch_forecasted_saleshours(span), pd.DataFrame
            )
            assert isinstance(await client.fetch_realized_hours(span), pd.DataFrame)
            assert isinstance(await client.fetch_forecasted_hours(span), pd.DataFrame)

    @pytest.mark.asyncio
    async def test_billing_return_types(self):
        span = DateRange(4)

        async with Client() as client:
            assert isinstance(await client.fetch_realized_billing(span), pd.DataFrame)
            assert isinstance(await client.fetch_forecasted_billing(span), pd.DataFrame)

    @pytest.mark.asyncio
    async def test_salesvalue_return_types(self):
        span = DateRange(4)

        async with Client() as client:
            assert isinstance(
                await client.fetch_realized_salesvalue(span), pd.DataFrame
            )
            assert isinstance(
                await client.fetch_forecasted_salesvalue(span), pd.DataFrame
            )

    @pytest.mark.asyncio
    async def test_util_return_types(self):
        testdata = pd.DataFrame()

        async with Client() as client:
            assert isinstance(await client.lookup_usernames(testdata), pd.DataFrame)
            assert isinstance(await client.lookup_businessunits(testdata), pd.DataFrame)
