from datetime import timedelta

import altair as alt
import pandas as pd

from src.database import Base
from src.severa.fetch import Fetcher

FI_LOCALE_JSON = {
    "dateTime": "%A, %-d. %Bta %Y klo %X",
    "date": "%-d.%-m.%Y",
    "time": "%H:%M:%S",
    "periods": ["a.m.", "p.m."],
    "days": [
        "sunnuntai",
        "maanantai",
        "tiistai",
        "keskiviikko",
        "torstai",
        "perjantai",
        "lauantai",
    ],
    "shortDays": ["Su", "Ma", "Ti", "Ke", "To", "Pe", "La"],
    "months": [
        "tammikuu",
        "helmikuu",
        "maaliskuu",
        "huhtikuu",
        "toukokuu",
        "kesäkuu",
        "heinäkuu",
        "elokuu",
        "syyskuu",
        "lokakuu",
        "marraskuu",
        "joulukuu",
    ],
    "shortMonths": [
        "Tammi",
        "Helmi",
        "Maalis",
        "Huhti",
        "Touko",
        "Kesä",
        "Heinä",
        "Elo",
        "Syys",
        "Loka",
        "Marras",
        "Joulu",
    ],
}


class ChartGroup:
    def __init__(self):
        alt.themes.register("treb", self.treb)
        alt.themes.enable("treb")
        alt.data_transformers.disable_max_rows()
        alt.renderers.set_embed_options(actions=False, timeFormatLocale=FI_LOCALE_JSON)

    def treb(self):
        font = "Trebuchet MS"

        return {
            "config": {
                "title": {"font": font, "subtitleFont": font},
                "axis": {"labelFont": font, "titleFont": font},
                "header": {"labelFont": font, "titleFont": font},
                "legend": {"labelFont": font, "titleFont": font},
            }
        }

    def prepare_allocation_data(
        self, data: pd.DataFrame, spanmin: int, spanmax: int
    ) -> pd.DataFrame:
        grouped = (
            data.groupby(["date", "forecast-date", "type"])["value"].sum().reset_index()
        )
        grouped["span"] = (grouped["forecast-date"] - grouped["date"]).map(
            lambda x: x.days
        )

        grouped_pivoted = grouped[
            (grouped["span"] >= spanmin) & (grouped["span"] <= spanmax)
        ]
        grouped_pivoted = grouped_pivoted.pivot_table(
            columns="type", values="value", index=["date", "forecast-date", "span"]
        ).reset_index()
        grouped_pivoted = grouped_pivoted.fillna({"external": 0, "internal": 0}).dropna(
            subset=["max"]
        )
        grouped_pivoted["total"] = (
            grouped_pivoted["internal"] + grouped_pivoted["external"]
        )
        grouped_pivoted["unallocated"] = (
            grouped_pivoted["max"] - grouped_pivoted["total"]
        )
        grouped_pivoted["billing-rate"] = (
            grouped_pivoted["external"] / grouped_pivoted["total"]
        )
        grouped_pivoted["allocation-rate"] = (
            grouped_pivoted["total"] / grouped_pivoted["max"]
        )
        grouped_melted = grouped_pivoted.melt(
            id_vars=["date", "span"],
            value_vars=["external", "internal", "max", "unallocated"],
        )
        grouped_merged = grouped_melted.merge(
            grouped_pivoted.drop(["external", "internal"], axis=1), on=["date", "span"]
        )
        grouped_merged["type"] = (
            grouped_merged["type"]
            .astype("category")
            .cat.rename_categories(
                {
                    "external": "Projektityö",
                    "internal": "Sisäinen työ",
                    "max": "Maksimi",
                    "unallocated": "Allokoimaton",
                }
            )
        )

        return grouped_merged

    async def allocated_hours(self, data: pd.DataFrame) -> alt.Chart:
        spanmin = 7
        spanmax = 360

        slider = alt.binding_range(
            min=spanmin, max=spanmax, step=1, name="Ennusteen pituus (vrk):  "
        )
        op_span = alt.param(value=30, bind=slider)

        base = alt.Chart(
            self.prepare_allocation_data(data, spanmin, spanmax)
        ).transform_filter((alt.datum.span >= 0) & (alt.datum.span <= op_span))

        chart_base = (
            base.transform_calculate(
                order="{'Projekityö':0, 'Sisäinen työ':1, 'Maksimi':3, 'Allokoimaton':2}[datum.variable]"
            )
            .encode(
                x=alt.X("monthdate(date):T").axis(title="Päiväys"),
                y=alt.Y("sum(value):Q").axis(title="Allokoitu tuntimäärä (h)"),
                color=alt.Color("type:N", title="Tuntilaji").scale(
                    scheme="category20c"
                ),
                order="order:O",
                tooltip=[
                    alt.Tooltip("sum(value):Q", title="Tuntimäärä (h)", format=".1f"),
                    alt.Tooltip("type:N", title="Tyyppi"),
                    alt.Tooltip(
                        "sum(total):Q", title="Allokoinnit yhteensä (h)", format=".1f"
                    ),
                    alt.Tooltip(
                        "sum(unallocated):Q", title="Allokoimatonta (h)", format=".1f"
                    ),
                    alt.Tooltip(
                        "sum(max):Q", title="Allokoinnit korkeintaan (h)", format=".1f"
                    ),
                    alt.Tooltip(
                        "mean(billing-rate):Q", title="Laskutusaste", format=".1%"
                    ),
                    alt.Tooltip(
                        "mean(allocation-rate):Q", title="Resursointiaste", format=".1%"
                    ),
                ],
            )
            .properties(width="container")
        )

        allocations_per_type = (
            chart_base.mark_area(
                opacity=1.0,
                point=alt.OverlayMarkDef(filled=False, fill="white", size=100),
            )
            .transform_filter(
                (alt.datum.type == "Sisäinen työ") | (alt.datum.type == "Projektityö")
            )
            .properties(title="Tuorein resursointiennuste henkilöittäin")
        )

        maximum_allocations = chart_base.mark_line(
            point=alt.OverlayMarkDef(filled=True, size=100), strokeDash=[4, 4]
        ).transform_filter(alt.datum.type == "Maksimi")

        normalized_allocations_per_type = (
            chart_base.mark_area()
            .encode(
                y=alt.Y("sum(value):Q", title="Allokointi (%)").stack(  # noqa: PD013
                    "normalize"
                )
            )
            .transform_filter(
                alt.FieldOneOfPredicate("type", ["Projektityö", "Sisäinen työ"])
            )
        )

        rule = (
            alt.Chart()
            .mark_rule(color="white", strokeDash=[4, 4])
            .encode(y=alt.datum(0.7))
        )
        rule_text = (
            base.transform_aggregate(mindate="min(date):T")
            .mark_text(baseline="top", dy=5, dx=5, align="left", color="white")
            .encode(
                y=alt.datum(0.7),
                x="monthdate(mindate):T",
                text=alt.datum("Laskutusastetavoite 70%"),
            )
        )

        return (
            (
                (allocations_per_type + maximum_allocations).properties(
                    height=260,
                )
                & (normalized_allocations_per_type + rule + rule_text).properties(
                    height=80
                )
            )
            .add_params(op_span)
            .interactive()
        )

    async def user_allocations(self, data: pd.DataFrame) -> alt.Chart:
        users = await Fetcher().users()
        users_df = pd.DataFrame([{"user": u.guid, "name": u.firstName} for u in users])

        today = data["date"].max()
        most_recent = data[(data["date"] == today)]
        most_recent = most_recent[
            most_recent["forecast-date"] < today + timedelta(days=540)
        ]

        source = (
            most_recent[(most_recent["type"] != "max")]
            .groupby(["forecast-date", "user"])["value"]
            .sum()
            .reset_index()
        )

        max_hours = (
            most_recent[most_recent["type"] == "max"]
            .groupby(["forecast-date", "user"])["value"]
            .sum()
            .reset_index()
        )
        max_hours["max"] = max_hours["value"]
        source = source.merge(
            max_hours.drop("value", axis=1), on=["forecast-date", "user"], how="outer"
        ).merge(users_df, on="user")

        source["week"] = source["forecast-date"].dt.isocalendar().week
        source["month"] = source["forecast-date"].dt.month
        source["year"] = source["forecast-date"].dt.isocalendar().year

        brush = alt.selection_interval(encodings=["x"])
        selected_user = alt.selection_point(encodings=["color"], on="mouseover")

        base = alt.Chart(source.convert_dtypes()).properties(
            width="container", height=300
        )

        upper = (
            base.transform_aggregate(
                date="min(forecast-date):T",
                weekvalue="sum(value):Q",
                weekmax="sum(max):Q",
                name="min(name):N",
                groupby=["week", "year", "name"],
            )
            .transform_joinaggregate(
                monthvalue="sum(weekvalue):Q",
                monthmax="sum(weekmax):Q",
                month="min(date):T",
                groupby=["year", "month", "name"],
            )
            .mark_area(interpolate="step-after")
            .encode(
                x=alt.X("date:T").scale(domain=brush).axis(title="Päiväys"),
                y=alt.Y("weekvalue:Q", scale=alt.Scale(domain=[0, 350])).axis(
                    title="Allokoitu tuntimäärä (h/vko)"
                ),
                color=alt.Color("name:N", title="Nimi"),
                opacity=alt.condition(selected_user, alt.value(1), alt.value(0.5)),
                tooltip=[
                    alt.Tooltip("name:N", title="Nimi"),
                    alt.Tooltip(
                        "date:T",
                        title="Päiväys",
                        format="vko %V / %Y (%-d.%-m.%Y)",
                    ),
                    alt.Tooltip("weekmax:Q", title="Maksimi", format=".1f"),
                    alt.Tooltip("weekvalue:Q", title="h/vko", format=".1f"),
                    alt.Tooltip("date:T", title="KK", format="%B"),
                    alt.Tooltip("monthvalue:Q", title="h/kk", format=".1f"),
                ],
            )
            .add_params(selected_user)
        ).properties(title="Tuorein resursointiennuste henkilöittäin")

        max_line = (
            base.transform_aggregate(
                date="min(forecast-date):T",
                weekmax="sum(max):Q",
                groupby=["week", "year"],
            )
            .mark_line(interpolate="step-after", strokeDash=[4, 4])
            .encode(x=alt.X("date:T").scale(domain=brush), y="weekmax:Q")
        )

        lower = (
            base.mark_area(interpolate="step-after")
            .encode(
                x=alt.X("forecast-date:T").axis(title="Päiväys"),
                y=alt.Y("sum(value):Q", scale=alt.Scale(domain=[0, 30])).axis(
                    title="Allokoitu tuntimäärä (h/vrk)"
                ),
            )
            .properties(height=60)
            .add_params(brush)
        )

        return (upper + max_line) & lower

    async def get_charts(self):
        data = Base("kpi-dev", "allocations").find()

        return [
            await self.allocated_hours(data),
            await self.user_allocations(data),
        ], len(data)
