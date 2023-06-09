import typing
from enum import Enum
from functools import partial, reduce
from types import TracebackType

import anyio
import arrow
import pandas as pd
from loguru import logger
from workalendar.europe import Finland

from src.daterange import DateRange
from src.severa import models
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
        {column_to_sum: combine_series(v[column_to_sum])}
        | dict(zip(grouping, k, strict=True))
        for k, v in grouped
    )


async def gather(f, args_list: typing.Iterable) -> list:
    async def save_result(result: list, f, *args) -> None:
        result.append(await f(*args))

    results = []
    async with anyio.create_task_group() as tg:
        for args in args_list:
            tg.start_soon(save_result, results, f, *args)

    return results


class Fetcher:
    def __init__(self):
        self._client = Client()
        self._users: list[models.UserOutputModel] = []

        self._sales_cache = None

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
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
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

    @property
    async def users_by_guid(self) -> dict[str, models.UserOutputModel]:
        return {user.guid: user for user in await self.users()}

    #
    # Allocations
    #

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

                allocation_df = pd.DataFrame(
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

                allocation_df["forecast-date"] = index
                allocation_df = allocation_df.reset_index(drop=True)
                json.append(allocation_df)

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

            user_max_hours_df = pd.DataFrame(
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

            user_max_hours_df["value"] = hours
            user_allocables.append(user_max_hours_df)

        async with anyio.create_task_group() as tg:
            for user in await self.users():
                tg.start_soon(user_max_allocable_hours, user)

        result = pd.concat(user_allocables).convert_dtypes()
        result["forecast-date"] = result.index
        result = result.reset_index(drop=True)
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

    #
    # Sales
    #

    async def process_single_sale(self, sale: models.ProjectOutputModel):
        can_calculate_value = True
        # First, expected value
        # order_date_offset = 0
        if sale.expectedOrderDate is None:
            self._invalid_sales["Arvioitu tilauspäivä puuttuu"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )
            can_calculate_value = False
        # else:
        # order_date = arrow.get(sale.expectedOrderDate.isoformat())
        # order_date_offset = (order_date - span.start).days

        # expected_value = np.zeros(len(span))
        if sale.expectedValue is None:
            self._invalid_sales["Myynnin arvo puuttuu"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )
            can_calculate_value = False

        expected_value = (
            pd.Series(
                [sale.expectedValue.amount * sale.probability / 100.0],
                index=[pd.Timestamp(sale.expectedOrderDate)],
            )
            if can_calculate_value
            else pd.Series(dtype=float)
        )

        phases = [
            models.PhaseModelWithHierarchyInfo(**phase_json)
            for phase_json in await self._client.get_all(
                f"projects/{sale.guid}/phaseswithhierarchy"
            )
        ]

        expected_work_hours = pd.Series(dtype=float)

        if not phases:
            self._invalid_sales["Vaihe puuttuu"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )
        else:
            for phase in phases:
                if (phase.workHoursEstimate is not None) and (
                    phase.workHoursEstimate > 0
                ):
                    start = arrow.get(phase.startDate.isoformat())
                    end = arrow.get(phase.deadline.isoformat())
                    phase_range = DateRange(start, end)

                    work_hour_estimate = (
                        phase.workHoursEstimate * sale.probability / 100.0
                    )
                    daily_work = work_hour_estimate / len(phase_range)

                    expected_work_hours = pd.Series(
                        daily_work,
                        index=pd.date_range(phase.startDate, phase.deadline, freq="D"),
                    )
                elif not phase.hasChildren:
                    # only report problems in leaf phases
                    self._invalid_sales["Vaiheen työmääräarvio puuttuu"].append(
                        {
                            "name": f"{sale.name} / {phase.name}",
                            "phase": phase.name,
                            "soldby": sale.salesPerson.firstName,
                            "owner": sale.projectOwner.firstName,
                            "guid": sale.guid,
                        }
                    )
                    logger.error(f"Phase with no workHoursEstimate: {phase.name}")

                MINIMUM_SUM_EPSILON = 0.5
                if sum(expected_work_hours) < MINIMUM_SUM_EPSILON:
                    self._invalid_sales["Työmääräarvio puuttuu"].append(
                        {
                            "name": sale.name,
                            "soldby": sale.salesPerson.firstName,
                            "owner": sale.projectOwner.firstName,
                            "guid": sale.guid,
                        }
                    )

        user = (await self.users_by_guid).get(sale.projectOwner.guid, None)
        user_businessunit = (
            "Muu" if not user else self.businessunits_by_guid[user.businessUnit.guid]
        )

        expected_value_df = pd.DataFrame(
            index=expected_value.index,
            data={
                "businessunit-project": self.businessunits_by_guid[
                    sale.businessUnit.guid
                ],
                "businessunit-user": user_businessunit,
                "user": sale.projectOwner.guid,
                "id": "sales-value",
                "project": sale.guid,
                "customer": sale.customer.guid,
                "date": pd.Timestamp(arrow.utcnow().datetime),
            },
        )
        expected_value_df["value"] = expected_value
        expected_value_df["forecast-date"] = expected_value.index

        expected_work_df = pd.DataFrame(
            index=expected_work_hours.index,
            data={
                "businessunit-project": self.businessunits_by_guid[
                    sale.businessUnit.guid
                ],
                "businessunit-user": user_businessunit,
                "user": sale.projectOwner.guid,
                "id": "sales-work",
                "project": sale.guid,
                "customer": sale.customer.guid,
                "type": "sales-estimate",
                "date": pd.Timestamp(arrow.utcnow().datetime),
            },
        )
        expected_work_df["value"] = expected_work_hours
        expected_work_df["forecast-date"] = expected_work_hours.index

        return pd.concat([expected_value_df, expected_work_df], ignore_index=True)

    def invalid_sales(self) -> pd.DataFrame:
        result = pd.DataFrame(
            [{**v, "id": k} for k, lst in self._invalid_sales.items() for v in lst]
        )
        result["inserted"] = pd.Timestamp(arrow.utcnow().datetime)
        return result.convert_dtypes()

    async def process_businessunit_sales(
        self, businessunit_guid: str
    ) -> list[pd.DataFrame]:
        """ """
        sales = await self._client.get_all(
            "salescases",
            {
                "businessUnitGuids": [businessunit_guid],
                "isClosed": False,
                "salesStatusTypeGuids": self.SalesStatus.TARJOUS.value,
            },
        )

        return await gather(
            self.process_single_sale,
            ((models.ProjectOutputModel(**sale),) for sale in sales),
        )

    async def get_sales_information(
        self, span: DateRange  # noqa: ARG002
    ) -> pd.DataFrame:
        """
        Return future sales values (€) and work (hours) for span.
        """
        if self._sales_cache is not None:
            return self._sales_cache

        self._invalid_sales = {
            "Arvioitu tilauspäivä puuttuu": [],
            "Myynnin arvo puuttuu": [],
            "Deadline puuttuu": [],
            "Työmääräarvio puuttuu": [],
            "Vaiheen työmääräarvio puuttuu": [],
            "Vaihe puuttuu": [],
        }

        all_expected_sales = await gather(
            self.process_businessunit_sales,
            ((businessunit,) for businessunit in self.businessunits.values()),
        )

        # flatten list for one level, not actually sum anything
        all_expected_sales = sum(all_expected_sales, start=[])

        self._sales_cache = pd.concat(
            all_expected_sales, ignore_index=True
        ).convert_dtypes()
        return self._sales_cache

    async def get_sales_value(self, span: DateRange) -> pd.DataFrame:
        sales = await self.get_sales_information(span)
        return sales[sales["id"] == "sales-value"]

    async def get_sales_work(self, span: DateRange) -> pd.DataFrame:
        sales = await self.get_sales_information(span)
        return sales[sales["id"] == "sales-work"]
