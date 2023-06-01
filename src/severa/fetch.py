import typing
from enum import Enum
from functools import partial, reduce
from types import TracebackType

import anyio
import arrow
import pandas as pd
from workalendar.europe import Finland

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

    async def get_resource_allocations(self, span: DateRange) -> pd.DataFrame:
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
                    tz="utc",
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
                        "date": arrow.utcnow().floor("day").datetime,
                        "id": "allocation",
                    },
                )

                df["forecast-date"] = index
                df.reset_index(inplace=True, drop=True)
                json.append(df)

        # await all the allocations from all the users
        dfs = []
        async with anyio.create_task_group() as tg:
            for user in await self.users():
                tg.start_soon(allocation_helper, user, dfs, name=user.firstName)

        return pd.concat(dfs).convert_dtypes()

    async def process_user_absences(
        self, user: models.UserOutputModel, span: DateRange
    ) -> pd.Series:
        start = span.start.format("YYYY-MM-DDTHH:mm:ssZZ")
        end = span.end.format("YYYY-MM-DDTHH:mm:ssZZ")
        daily_absences = pd.Series(dtype=float)

        for activity_json in await self._client.get_all(
            "activities",
            {
                "activityCategories": "Absences",
                "startDateTime": start,
                "endDateTime": end,
                "userGuids": [user.guid],
            },
        ):
            activity = models.ActivityModel(**activity_json)

            start_time = arrow.Arrow.fromdatetime(activity.startDateTime)
            end_time = arrow.Arrow.fromdatetime(activity.endDateTime)

            if activity.isAllDay:
                absence_span = DateRange(start_time, end_time)

                temp_absences = pd.Series(
                    user.workContract.dailyHours,
                    index=pd.date_range(
                        absence_span.start.date(),
                        absence_span.end.date(),
                        freq="D",
                        tz="utc",
                    ),
                )
            else:
                temp_absences = pd.Series(dtype=float)

                # Iterating to fill a pd.Series... not good, fix later
                for s, e in arrow.Arrow.span_range(
                    "day", activity.startDateTime, activity.endDateTime, exact=True
                ):
                    # Save the number of hours for each day in the duration
                    # of the absence. Most probably these absences are
                    # short, couple of hours within a single day.
                    temp_absences[s.date()] = (e - s).seconds / 60.0 / 60.0

            daily_absences = daily_absences.add(temp_absences, fill_value=0.0)

        return daily_absences

    async def get_maximum_allocable_hours(self, span: DateRange):
        """
        Return an array containing maximum allocable hours for each day in span.
        Takes into account 1. persons' workcontracts, 2. weekends and holiday,
        3. planned abcenses (vacations etc).
        """
        cal = Finland()
        dates_in_span = pd.date_range(
            span.start.date(), span.end.date(), freq="D", tz="utc"
        )
        workday_mask = (
            pd.Series(dates_in_span, index=dates_in_span)
            .apply(cal.is_working_day)
            .apply(int)
        )

        user_allocables = []

        async def user_max_allocable_hours(user: models.UserOutputModel) -> None:
            hours = workday_mask * user.workContract.dailyHours
            hours = hours.add(
                -await self.process_user_absences(user, span), fill_value=0
            )
            hours[hours < 0] = 0

            df = pd.DataFrame(
                index=hours.index,
                data={
                    "businessunit-user": self.businessunits_by_guid[
                        user.businessUnit.guid
                    ],
                    "user": user.guid,
                    "date": arrow.utcnow().floor("day").datetime,
                    "id": "allocation",
                },
            )

            df["value"] = hours
            user_allocables.append(df)

        async with anyio.create_task_group() as tg:
            for user in await self.users():
                tg.start_soon(user_max_allocable_hours, user)

        result = pd.concat(user_allocables).convert_dtypes()
        result["forecast-date"] = result.index
        result.reset_index(inplace=True, drop=True)
        return result

    async def get_allocations_with_maxes(self, span: DateRange):
        max_hours = await self.get_maximum_allocable_hours(span)
        allocs = await self.get_resource_allocations(span)

        allocs["type"] = (allocs["is_internal"] == True).transform(
            lambda x: "internal" if x else "external"
        )
        max_hours["type"] = "max"
        total = (
            pd.concat(
                [allocs.drop("is_internal", axis=1), max_hours],
                ignore_index=True,
                sort=False,
            )
            .convert_dtypes()
            .astype({"type": "category"})
        )

        # Drop old forecasts
        return total[total["forecast-date"] > total["date"]]
