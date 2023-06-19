import time
import typing
from enum import Enum
from types import TracebackType

import anyio
import arrow
import pandas as pd
from workalendar.europe import Finland

from src.daterange import DateRange
from src.severa import models
from src.severa.base_client import Client as BaseClient
from src.stable_hash import get_hash

T = typing.TypeVar("T", bound="Client")

SALES_CACHE_REFRESH_AFTER_SECONDS = 60 * 60  # Save sales for 1h
PROJECTS_CACHE_REFRESH_AFTER_SECONDS = 60 * 60  # Save projects for 1h


class SalesStatus(Enum):
    TARJOUS = "04a8c06b-bddb-ed4f-586a-0a2098587633"
    TILAUS = "fb1b8ca5-2026-4e0f-3169-500d1ad7603e"
    HYLÄTTY = "baaa8b0a-b77a-b10a-8372-7760a4b99d77"


async def gather(f_args_list: typing.Iterable) -> list:
    async def save_result(result: list, f: typing.Callable, *args) -> None:
        result.append(await f(*args))

    results: list = []
    async with anyio.create_task_group() as tg:
        for f, *args in f_args_list:
            tg.start_soon(save_result, results, f, *args)

    return results


class Client:
    def __init__(self: T):
        self._client = BaseClient()
        self._users: dict[str, models.UserOutputModel] = {}

        self._sales_cache = None
        self._projects_cache = None

        self._sales_cache_refresh_time = 0.0
        self._projects_cache_refresh_time = 0.0

        # Aliases
        self.fetch_forecasted_absences = self.fetch_absences
        self.fetch_realized_absences = self.fetch_absences

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

    async def users(self) -> list[models.UserOutputModel]:
        if not self._users:
            self._users = {
                user_json["guid"]: models.UserOutputModel(**user_json)
                for user_json in await self._client.get_all(
                    "users",
                    businessUnitGuids=self.businessunits,
                    isActive=True,
                )
            }

        return list(self._users.values())

    async def user_by_guid(self, user_guid: str) -> models.UserOutputModel:
        if not self._users:
            await self.users()

        return self._users[user_guid]

    async def project_by_guid(self, project_guid: str) -> models.ProjectOutputModel:
        return (await self.fetch_projects_with_cache())[project_guid]

    @property
    def businessunits(self) -> list[str]:
        # TIE, BAD
        return [
            "f6d9f1e8-afae-1a74-5bbd-54d840a3e40e",
            "2a82464c-50b8-0df1-1cfc-51f5ae1bf667",
        ]

    def businessunit_to_shortname(self, businessunit_guid: str) -> str:
        return {
            "f6d9f1e8-afae-1a74-5bbd-54d840a3e40e": "TIE",
            "2a82464c-50b8-0df1-1cfc-51f5ae1bf667": "BAD",
            "12f817a4-1f54-8c34-9489-0ab1d58ccf48": "HAL",
            "1a361efe-8738-1ecf-0c16-e2bf91e21e3e": "JOH",
            "341f92f5-cc81-d933-dbf4-fc80849815ea": "LAH",
            "4260f308-7383-841a-8b5f-a40976bc22ca": "SOV",
            "6b49ead7-573f-f186-c508-1e8606b5e59a": "VIS",
        }.get(businessunit_guid, "MUU")

    ###########################
    # Fetching hours          #
    ###########################

    async def fetch_maximums(self) -> pd.DataFrame:
        """
        Get maximum workhours per person by their work contract.
        """
        return pd.DataFrame(
            [
                {
                    "user": user.guid,
                    "value": user.workContract.dailyHours,
                    "id": "maximum",
                    "internal_guid": user.guid,
                }
                for user in await self.users()
            ]
        ).convert_dtypes()

    async def fetch_absences(self, span: DateRange) -> pd.DataFrame:
        start = span.start.format("YYYY-MM-DDTHH:mm:ssZZ")
        end = span.end.format("YYYY-MM-DDTHH:mm:ssZZ")

        absences = [
            models.ActivityModel(**json)
            for json in await self._client.get_all(
                "activities",
                {
                    "activityCategories": "Absences",
                    "startDateTime": start,
                    "endDateTime": end,
                    "userGuids": [user.guid for user in await self.users()],
                },
            )
        ]

        if not absences:
            return pd.DataFrame(
                {
                    "user": pd.Series(dtype=str),
                    "value": pd.Series(dtype=float),
                    "date": pd.Series(dtype="datetime64[ns, utc]"),
                    "activity_type": pd.Series(dtype=str),
                    "id": pd.Series(dtype=str),
                }
            )

        result = pd.DataFrame(
            [
                {
                    "user": absence.ownerUser.guid,
                    "value": (
                        absence.endDateTime - absence.startDateTime
                    ).total_seconds()
                    / 60
                    / 60,
                    "date": pd.Timestamp(
                        arrow.get(absence.startDateTime).to("utc").datetime
                    ),
                    "is_all_day": absence.isAllDay,
                    "activity_type": absence.activityType.guid,
                    "id": "absences",
                    "internal_guid": absence.guid,
                }
                for absence in absences
            ]
        )

        async def daily_hours_by_user_guid(guid: str) -> float:
            return (await self.user_by_guid(guid)).workContract.dailyHours

        if not result.empty:
            # "AllDay" absences result in 24h durations, fix them after the fact
            result.loc[result["is_all_day"], "value"] = [
                await daily_hours_by_user_guid(guid)
                for guid in result.loc[result["is_all_day"], "user"]
            ]

            # Discard holidays, weekends etc
            cal = Finland()
            working_days = result.date.map(cal.is_working_day)
            result = result[working_days]

        return result.drop("is_all_day", axis=1).convert_dtypes()

    async def fetch_realized_user_workhours(
        self, user: models.UserOutputModel, span: DateRange
    ) -> pd.DataFrame:
        hours = (
            models.WorkHourOutputModel(**json)
            for json in await self._client.get_all(
                f"users/{user.guid}/workhours",
                **span,
            )
        )

        return pd.DataFrame(
            [
                {
                    "user": user.guid,
                    "value": hour.quantity,
                    "date": pd.Timestamp(hour.eventDate, tz="utc"),
                    "project": hour.project.guid,
                    "phase": hour.phase.guid,
                    "productive": hour.isProductive,
                    "internal_guid": hour.guid,
                    "id": "workhours",
                }
                for hour in hours
            ]
        )

    async def fetch_realized_workhours(self, span: DateRange) -> pd.DataFrame:
        return pd.concat(
            await gather(
                [
                    (self.fetch_realized_user_workhours, user, span)
                    for user in await self.users()
                ],
            ),
            ignore_index=True,
        ).convert_dtypes()

    async def fetch_forecasted_user_workhours(
        self, user: models.UserOutputModel, span: DateRange
    ) -> pd.DataFrame:
        allocations = [
            models.ResourceAllocationOutputModel(**allocation_json)
            for allocation_json in await self._client.get_all(
                f"users/{user.guid}/resourceallocations/allocations",
                **span,  # type: ignore
            )
        ]

        return pd.DataFrame(
            [
                {
                    "internal_guid": alloc.guid,
                    "productive": not alloc.project.isInternal,
                    "value": alloc.calculatedAllocationHours,
                    "user": user.guid,
                    "project": alloc.project.guid,
                    "phase": alloc.phase.guid,
                    "start_date": arrow.get(alloc.derivedStartDate).datetime,
                    "end_date": arrow.get(alloc.derivedEndDate).datetime,
                    "id": "workhours",
                }
                for alloc in allocations
            ]
        )

    async def fetch_forecasted_workhours(self, span: DateRange) -> pd.DataFrame:
        return pd.concat(
            await gather(
                [
                    (self.fetch_forecasted_user_workhours, user, span)
                    for user in await self.users()
                ],
            ),
            ignore_index=True,
        ).convert_dtypes()

    async def fetch_forecasted_saleshours(self, span: DateRange) -> pd.DataFrame:
        saleshours = await self.fetch_sales()
        # TODO: span
        return saleshours[saleshours.id == "saleswork"]

    async def fetch_hours(self, span: DateRange) -> pd.DataFrame:
        span_past, span_future = span.cut(arrow.utcnow())

        awaitables = [(self.fetch_absences, span)]

        if span_past:
            awaitables += [(self.fetch_realized_workhours, span_past)]

        if span_future:
            awaitables += [
                (self.fetch_forecasted_workhours, span_future),
                (self.fetch_forecasted_saleshours, span_future),
            ]

        dfs = [await self.fetch_maximums()] + await gather(awaitables)
        result = pd.concat(dfs, ignore_index=True)
        result["forecast_date"] = arrow.utcnow().floor("day").datetime
        result["_id"] = result.apply(
            lambda x: get_hash(
                (x.get("internal_guid"), x.get("id"), x.get("forecast_date"))
            ),
            axis=1,
        )

        return result.convert_dtypes()

    ###########################
    # Fetching sales          #
    ###########################

    async def fetch_sales(self) -> pd.DataFrame:
        """
        Return future sales values (€) and work (hours).
        """
        if self._sales_cache is not None and (
            time.monotonic() - self._sales_cache_refresh_time
            < SALES_CACHE_REFRESH_AFTER_SECONDS
        ):
            return self._sales_cache

        self._invalid_sales: dict[str, list] = {
            "Arvioitu tilauspäivä puuttuu": [],
            "Myynnin arvo puuttuu": [],
            "Deadline puuttuu": [],
            "Työmääräarvio puuttuu": [],
            "Vaiheen työmääräarvio puuttuu": [],
            "Vaihe puuttuu": [],
            "Avainsanat puuttuvat": [],
            "Arvioitu tilauspäivä on menneisyydessä": [],
        }

        self._sales_cache = await self.force_fetch_all_sales()
        self._sales_cache_refresh_time = time.monotonic()

        return self._sales_cache

    async def force_fetch_all_sales(self) -> pd.DataFrame:
        sales = await self._client.get_all(
            "salescases",
            {
                "businessUnitGuids": self.businessunits,
                "isClosed": False,
                "salesStatusTypeGuids": SalesStatus.TARJOUS.value,
            },
        )

        sales_dataframes = await gather(
            (
                (self.fetch_single_sale, models.ProjectOutputModel(**sale))
                for sale in sales
            ),
        )

        return pd.concat(sales_dataframes, ignore_index=True).convert_dtypes()

    async def fetch_single_sale(self, sale: models.ProjectOutputModel) -> pd.DataFrame:
        can_calculate_value = True

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
        elif sale.expectedOrderDate < arrow.utcnow().date():
            self._invalid_sales["Arvioitu tilauspäivä on menneisyydessä"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )
            can_calculate_value = False

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

        if sale.deadline is None:
            self._invalid_sales["Deadline puuttuu"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )

        if sale.keywords is None:
            self._invalid_sales["Avainsanat puuttuvat"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )

        phases = [
            models.PhaseModelWithHierarchyInfo(**phase_json)
            for phase_json in await self._client.get_all(
                f"projects/{sale.guid}/phaseswithhierarchy"
            )
        ]

        expected_workhours = []

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
                    expected_workhours.append(
                        {
                            "value": phase.workHoursEstimate * sale.probability / 100.0,
                            "user": sale.projectOwner.guid,
                            "start_date": arrow.get(phase.startDate).datetime,
                            "end_date": arrow.get(phase.deadline).datetime,
                            "project": phase.project.guid,
                            "phase": phase.guid,
                            "sold_by": sale.salesPerson.guid,
                            "productive": not sale.isInternal,
                            "id": "saleswork",
                            "internal_guid": phase.guid,
                        }
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

        MINIMUM_SUM_EPSILON = 0.5
        if sum(exp["value"] for exp in expected_workhours) < MINIMUM_SUM_EPSILON:
            self._invalid_sales["Työmääräarvio puuttuu"].append(
                {
                    "name": sale.name,
                    "soldby": sale.salesPerson.firstName,
                    "owner": sale.projectOwner.firstName,
                    "guid": sale.guid,
                }
            )

        expected_value_df = pd.DataFrame(
            {
                "user": pd.Series(dtype=str),
                "id": pd.Series(dtype=str),
                "project": pd.Series(dtype=str),
                "date": pd.Series(dtype="datetime64[ns, utc]"),
                "value": pd.Series(dtype=float),
                "internal_guid": pd.Series(dtype=str),
            }
        )

        if can_calculate_value:
            expected_value_df = pd.DataFrame(
                [
                    {
                        "user": sale.projectOwner.guid,
                        "id": "salesvalue",
                        "project": sale.guid,
                        "sold_by": sale.salesPerson.guid,
                        "date": pd.Timestamp(sale.expectedOrderDate, tz="utc"),
                        "value": sale.expectedValue.amount,
                        "internal_guid": sale.guid,
                    }
                ]
            )

        expected_work_df = pd.DataFrame(expected_workhours)

        return pd.concat(
            [expected_value_df, expected_work_df], ignore_index=True
        ).convert_dtypes()

    def get_invalid_sales(self) -> pd.DataFrame:
        result = pd.DataFrame(
            [{**v, "id": k} for k, lst in self._invalid_sales.items() for v in lst]
        )
        result["_id"] = result.apply(
            lambda x: get_hash((x["id"], x["guid"], x["phase"])), axis=1
        )
        result["inserted"] = pd.Timestamp(arrow.utcnow().datetime)
        return result.convert_dtypes()

    ###########################
    # Fetching billing        #
    ###########################

    async def fetch_billing(self, span: DateRange) -> pd.DataFrame:
        projects_mapping = await self.fetch_projects_with_cache()

        span_past, span_future = span.cut(arrow.utcnow())
        awaitables = []

        if span_past:
            awaitables += [(self.fetch_realized_billing, span_past)]

        if span_future:
            awaitables += [(self.fetch_forecasted_billing, span_future)]

        result = pd.concat(await gather(awaitables), ignore_index=True)
        result["user"] = result["project"].apply(
            lambda x: projects_mapping[x].projectOwner.guid
            if x in projects_mapping
            else "CACHE_MISS"
        )
        result["forecast_date"] = arrow.utcnow().floor("day").datetime
        result["_id"] = result.apply(
            lambda x: get_hash(
                (x.get("internal_guid"), x.get("id"), x.get("forecast_date"))
            ),
            axis=1,
        )

        return result.convert_dtypes()

    async def fetch_realized_billing(self, span: DateRange) -> pd.DataFrame:
        all_invoices = [
            models.InvoiceOutputModel(**invoice_json)
            for invoice_json in await self._client.get_all(
                "invoices",
                {**span, "projectBusinessUnitGuids": self.businessunits},
            )
        ]

        billing_df = pd.DataFrame(
            [
                {
                    "value": invoice.totalExcludingTax.amount,
                    "project": invoice.projects[0].guid,
                    "internal_guid": invoice.guid,
                    "date": pd.Timestamp(invoice.date, tz="utc"),
                    "status": invoice.status.guid,
                    "id": "billing",
                }
                for invoice in all_invoices
            ]
        )

        return billing_df.convert_dtypes()

    async def fetch_forecasted_billing(self, span: DateRange) -> pd.DataFrame:
        all_projects = (await self.fetch_projects_with_cache()).values()

        forecasts: list[models.ProjectForecastOutputModel] = sum(
            await gather(
                (self.fetch_project_forecasts, project, span)
                for project in all_projects
            ),
            start=[],
        )

        result = pd.DataFrame(
            [
                {
                    "id": "billing",
                    "internal_guid": forecast.guid,
                    "start_date": pd.Timestamp(
                        arrow.get(forecast.year, forecast.month, 1)
                        .floor("month")
                        .datetime
                    ),
                    "end_date": pd.Timestamp(
                        arrow.get(forecast.year, forecast.month, 1)
                        .ceil("month")
                        .datetime
                    ),
                    "project": forecast.project.guid,
                    "value": forecast.billingForecast.amount
                    if forecast.billingForecast is not None
                    else 0.0,
                    "billing": forecast.billingForecast.amount
                    if forecast.billingForecast is not None
                    else 0.0,
                    "expense": forecast.expenseForecast.amount
                    if forecast.expenseForecast is not None
                    else 0.0,
                    "revenue": forecast.revenueForecast.amount
                    if forecast.revenueForecast is not None
                    else 0.0,
                    "labor_expense": forecast.laborExpenseForecast.amount
                    if forecast.laborExpenseForecast is not None
                    else 0.0,
                }
                for forecast in forecasts
            ]
        )

        forecast_sum = result[["billing", "expense", "revenue", "labor_expense"]].sum(
            axis=1
        )
        return result[(forecast_sum < 0) | (forecast_sum > 0)]

    async def fetch_project_forecasts(
        self, project: models.ProjectOutputModel, span: DateRange
    ) -> typing.Iterable[models.ProjectForecastOutputModel]:
        """
        Get all the forecasts in span for one project.
        """
        return [
            models.ProjectForecastOutputModel(**forecast_json)
            for forecast_json in await self._client.get_all(
                f"projects/{project.guid}/projectforecasts", {**span}
            )
            if not project.isClosed
            and not project.isInternal
            and project.lastUpdatedDateTime > span.start.shift(months=-4).datetime
        ]

    ###########################
    # Fetching sales          #
    ###########################

    async def fetch_forecasted_salesvalue(self, span: DateRange) -> pd.DataFrame:
        saleshours = await self.fetch_sales()
        # TODO: span
        return saleshours[saleshours.id == "salesvalue"].drop(
            ["start_date", "end_date", "phase", "productive"], axis=1
        )

    async def fetch_realized_salesvalue(self, span: DateRange) -> pd.DataFrame:
        result = pd.DataFrame(
            [
                {
                    "value": project.expectedValue.amount,
                    "project": project.guid,
                    "date": pd.Timestamp(project.expectedOrderDate, tz="utc"),
                    "user": project.projectOwner.guid,
                    "internal_guid": project.guid,
                    "sold_by": project.salesPerson.guid,
                    "id": "salesvalue",
                }
                for project in (await self.fetch_projects_with_cache()).values()
                if span.contains(arrow.get(project.expectedOrderDate.isoformat()))
            ]
        )

        return result.convert_dtypes()

    async def fetch_salesvalue(self, span: DateRange) -> pd.DataFrame:
        span_past, span_future = span.cut(arrow.utcnow())

        awaitables = []

        if span_past:
            awaitables += [(self.fetch_realized_salesvalue, span_past)]

        if span_future:
            awaitables += [(self.fetch_forecasted_salesvalue, span_future)]

        result = pd.concat(await gather(awaitables), ignore_index=True)
        result["forecast_date"] = arrow.utcnow().floor("day").datetime
        result["_id"] = result.apply(
            lambda x: get_hash(
                (x.get("internal_guid"), x.get("id"), x.get("forecast_date"))
            ),
            axis=1,
        )

        return result.convert_dtypes()

    async def fetch_projects_with_cache(self) -> dict[str, models.ProjectOutputModel]:
        if self._projects_cache is not None and (
            time.monotonic() - self._projects_cache_refresh_time
            < PROJECTS_CACHE_REFRESH_AFTER_SECONDS
        ):
            return self._sales_cache

        projects_json = await self._client.get_all(
            "projects",
            {
                "businessUnitGuids": self.businessunits,
                "salesStatusTypeGuids": SalesStatus.TILAUS.value,
            },
        )

        self._projects_cache = {
            project_json["guid"]: models.ProjectOutputModel(**project_json)
            for project_json in projects_json
        }

        return self._projects_cache

    ###########################
    # Utility                 #
    ###########################

    async def lookup_usernames(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    async def lookup_businessunits(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()
