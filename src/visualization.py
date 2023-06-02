import altair as alt
import pandas as pd

from src.database import Base
from src.severa.fetch import Fetcher


class ChartGroup:
    def __init__(self, span: int):
        self._span = span

        alt.themes.register("treb", self.treb)
        alt.themes.enable("treb")
        alt.renderers.set_embed_options(actions=False)
        alt.data_transformers.disable_max_rows()

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

    async def allocated_hours(self, data: pd.DataFrame) -> alt.Chart:
        grouped = (
            data.groupby(["date", "forecast-date", "type"])["value"].sum().reset_index()
        )
        grouped["span"] = (grouped["forecast-date"] - grouped["date"]).map(
            lambda x: x.days
        )

        grouped_pivoted = grouped[grouped["span"] > 0]
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

        slider = alt.binding_range(
            min=7, max=360, step=1, name="Ennusteen pituus (vrk):  "
        )
        op_span = alt.param(value=30, bind=slider)

        base = alt.Chart(grouped_merged).transform_filter(
            (alt.datum.span >= 0) & (alt.datum.span <= op_span)
        )

        rule = (
            alt.Chart()
            .mark_rule(color="white", strokeDash=[4, 4])
            .encode(y=alt.datum(0.7))
        )
        rule_text = (
            base.transform_aggregate(mindate="min(date)")
            .mark_text(baseline="top", dy=5, dx=5, align="left", color="white")
            .encode(
                y=alt.datum(0.7),
                x="monthdate(mindate):T",
                text=alt.datum("Laskutusastetavoite 70%"),
            )
        )

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

        allocations_per_type = chart_base.mark_area(
            opacity=1.0, point=alt.OverlayMarkDef(filled=False, fill="white", size=100)
        ).transform_filter(
            (alt.datum.type == "Sisäinen työ") | (alt.datum.type == "Projektityö")
        )

        maximum_allocations = chart_base.mark_line(
            point=alt.OverlayMarkDef(filled=True, size=100), strokeDash=[4, 4]
        ).transform_filter(alt.datum.type == "Maksimi")

        normalized_allocations_per_type = (
            chart_base.mark_area()
            .encode(
                y=alt.Y("sum(value):Q", title="Allokointi (%)").stack("normalize")
            )  # noqa: PD013
            .transform_filter(
                alt.FieldOneOfPredicate("type", ["Projektityö", "Sisäinen työ"])
            )
        )

        return (
                (allocations_per_type + maximum_allocations).properties(
                    height=260,
                ) & (normalized_allocations_per_type + rule + rule_text).properties(
                    height=80
                )
            ).add_params(op_span).interactive()

    async def user_allocations(self, data: pd.DataFrame) -> alt.Chart:
        users = await Fetcher().users()
        users_df = pd.DataFrame([{"user": u.guid, "name": u.firstName} for u in users])

        source = (
            data[(data["date"] == data["date"].max()) & (data["type"] != "max")]
            .groupby(["forecast-date", "user"])["value"]
            .sum()
            .reset_index()
        ).merge(users_df, on="user")

        brush = alt.selection_interval(encodings=["x"])

        base = (
            alt.Chart(source)
            .encode(x="forecast-date:T", y="value:Q")
            .properties(width="container", height=200)
        )

        upper = base.mark_area().encode(
            x=alt.X("forecast-date:T").scale(domain=brush), color="name:N"
        )

        lower = (
            base.mark_area()
            .encode(y="sum(value):Q")
            .properties(height=60)
            .add_params(brush)
        )

        return upper & lower

    async def get_charts(self):
        data = Base("kpi-dev", "allocations").find()

        return [await self.allocated_hours(data), await self.user_allocations(data)]
