import typing
from enum import Enum
from functools import partial, reduce
from types import TracebackType

import anyio
import arrow
import pandas as pd
from loguru import logger

import src.severa.models as models
from src.daterange import DateRange
from src.severa.base_client import Client

T = typing.TypeVar("T", bound="Client")


def combine_series(df: pd.DataFrame) -> pd.Series:
    """
    Sum a dataframe, but with pd.Series.add rather than operator __add__.
    """
    return reduce(partial(pd.Series.add, fill_value=0), df)


def group_sum(
    grouping: list[str], df: pd.DataFrame, column_to_sum: str = "values"
) -> pd.DataFrame:
    """
    Group a df and sum the column_to_sum.
    """
    grouped = df.groupby(grouping)
    return pd.DataFrame(
        {column_to_sum: combine_series(v[column_to_sum])} | dict(zip(grouping, k))
        for k, v in grouped
    )


class Fetcher:
    def __init__(self):
        self._client = Client()
        self._users: list[models.UserOutputModel] = []

    async def users(self) -> list[models.UserOutputModel]:
        if self._users:
            return self._users

        self._users = [
            models.UserOutputModel(**user_json)
            for user_json in await self._client.get_all(
                "users",
                businessUnitGuids=list(self.businessunits.values()),
                isActive=True,
            )
        ]
        return self._users

    async def __aenter__(self: T) -> T:
        await self._client.__aenter__()

        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc_value, traceback)

    class SalesStatus(Enum):
        TARJOUS = "04a8c06b-bddb-ed4f-586a-0a2098587633"
        TILAUS = "fb1b8ca5-2026-4e0f-3169-500d1ad7603e"
        HYLÄTTY = "baaa8b0a-b77a-b10a-8372-7760a4b99d77"

    @property
    def salesstatus(self) -> dict[str, str]:
        return {
            "05 Tarjous": "04a8c06b-bddb-ed4f-586a-0a2098587633",
            "Tilaus": "fb1b8ca5-2026-4e0f-3169-500d1ad7603e",
            "Hylätty tarjous": "baaa8b0a-b77a-b10a-8372-7760a4b99d77",
        }

    @property
    def businessunits(self) -> dict[str, str]:
        return {
            "TIE": "f6d9f1e8-afae-1a74-5bbd-54d840a3e40e",
            "BAD": "2a82464c-50b8-0df1-1cfc-51f5ae1bf667",
        }

    @property
    def kpi_definitions(self) -> dict[str, str]:
        return {kpi["guid"]: kpi for kpi in self.all_kpis}

    @property
    def businessunits_by_guid(self) -> dict[str, str]:
        return {v: k for k, v in self.businessunits.items()}

    async def get_resource_allocations(self, span: DateRange):
        async def allocation_helper(user: models.UserOutputModel, json: list):
            allocations = await self._client.get_all(
                f"users/{user.guid}/resourceallocations/allocations",
                **span,  # type: ignore
                # "projectBusinessUnitGuid": businessunit_guid
            )

            for allocation_json in allocations:
                allocation = models.ResourceAllocationOutputModel(**allocation_json)
                dStartDate = arrow.get(allocation.derivedStartDate)
                dEndDate = arrow.get(allocation.derivedEndDate)
                alloc_span = DateRange(dStartDate, dEndDate)
                hours_per_day = allocation.calculatedAllocationHours / len(alloc_span)

                index = pd.date_range(
                    allocation.derivedStartDate,
                    allocation.derivedEndDate,
                    freq="D",
                )

                df = pd.DataFrame(
                    index=index,
                    data={
                        "businessunit-user": self.businessunits_by_guid[
                            user.businessUnit.guid
                        ],
                        "is_internal": allocation.project.isInternal,
                        "value": hours_per_day,
                        "user": user.guid,
                        "project": allocation.project.guid,
                        "phase": allocation.phase.guid,
                        "date": arrow.utcnow().date(),
                        "id": "allocation",
                    },
                )

                df["forecast-date"] = index
                df.reset_index(inplace=True, drop=True)
                json.append(df)

        # await all the allocations from all the users
        dfs = []
        logger.info("Starting task group...")
        async with anyio.create_task_group() as tg:
            for user in await self.users():
                tg.start_soon(allocation_helper, user, dfs, name=user.firstName)

        logger.info("...done.")

        return pd.concat(dfs)
