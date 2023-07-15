"use strict";

// Poor man's import...
dfd = window.dfd;
luxon = window.luxon;

const HEIGHT = 350;

const embedOpt = {
  mode: "vega-lite",
  actions: false,
  formatLocale: {
    currency: ["", "\u00a0€"],
    decimal: ".",
    thousands: "\u00a0 ",
    grouping: [3],
  },
  timeFormatLocale: {
    dateTime: "%A, %-d. %Bta %Y klo %X",
    date: "%-d.%-m.%Y",
    time: "%H:%M:%S",
    periods: ["a.m.", "p.m."],
    days: [
      "sunnuntai",
      "maanantai",
      "tiistai",
      "keskiviikko",
      "torstai",
      "perjantai",
      "lauantai",
    ],
    shortDays: ["Su", "Ma", "Ti", "Ke", "To", "Pe", "La"],
    months: [
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
    shortMonths: [
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
  },
};

async function refresh_sales_vega(
  data,
  div_class_to_embed,
  span,
  monthly_target_billing,
  target_billing
) {
  const vlSpec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    background: "rgba(0,0,0,0%)",
    data: { values: data, format: { type: "json" } },
    config: {
      view: { fill: "white", width: 1300 },
    },

    transform: [
      {
        filter: {
          field: "id",
          oneOf: ["salesvalue", "maximum"],
        },
      },
      { timeUnit: "yearmonthdate", field: "date", as: "datevalue" },
      { timeUnit: "yearmonth", field: "date", as: "monthvalue" },
      {
        impute: "value",
        key: "datevalue",
        value: 0,
        groupby: ["datevalue", "id"],
      },
      {
        filter: {
          field: "id",
          oneOf: ["salesvalue"],
        },
      },
      {
        aggregate: [
          {
            op: "sum",
            field: "value",
            as: "value",
          },
        ],
        groupby: ["datevalue"],
      },
      
      { calculate: `${target_billing} / ${span}`, as: "target" },
      {
        window: [
          { op: "sum", field: "value", as: "w_value" },
          { op: "sum", field: "target", as: "w_target" },
        ],
        sort: [{ field: "datevalue", order: "ascending" }],
        ignorePeers: false,
        // groupby: ["first_name"],
        frame: [null, 0],
      },
      { calculate: "datum.w_value - datum.w_target", as: "w_diff" },
      { calculate: "min(datum.w_diff, 0)", as: "w_pos_diff" },
      { calculate: "max(datum.w_diff, 0)", as: "w_neg_diff" },
    ],

    vconcat: [
      {
        layer: [
          {
            height: HEIGHT,
            mark: { type: "area" },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonthdate",
                type: "temporal",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%-d.%-m.'), timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%B') : timeFormat(datum.value, '%u') == '1' ? 'vko ' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 30,
                  tickCount: "month",
                  labelOffset: 4,
                  labelPadding: -24,
                  labelBound: true,
                  gridDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [5, 5],
                  },
                  tickDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [5, 5],
                  },
                },
              },

              y: {
                field: "w_value",
                type: "quantitative",
                aggregate: "sum",
                axis: { title: "Myynti" },
                format: "$.2f",
              },
              color: { value: "#4c78a8" },
              opacity: { value: 0.3 },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Päiväys",
                  format: "%d.%m.%Y",
                  aggregate: "max",
                },
                {
                  field: "w_target",
                  type: "quantitative",
                  title: "Kumuloituva tavoite",
                  format: "$.2f",
                  aggregate: "sum",
                },
                {
                  field: "w_value",
                  type: "quantitative",
                  title: "Kumuloituva myynti",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          

          {
            height: HEIGHT,
            mark: { type: "line", strokeDash: [8, 4] },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: { field: "w_target", type: "quantitative" },
              color: {
                value: "#4c78a8",
              },
              size: {
                value: 2,
              },
            },
          },

          {
            height: HEIGHT,
            mark: { type: "area" },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: { field: "w_pos_diff", type: "quantitative" },
              color: {
                value: "#e45756",
              },
              opacity: {value: 0.8}
            },
          },

          {
            height: HEIGHT,
            mark: { type: "area" },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: { field: "w_neg_diff", type: "quantitative" },
              color: {
                value:  "#4c78a8",
              },
              opacity: {value: 0.9}
            },
          },

          {
            height: HEIGHT,
            mark: { type: "bar" },
            data: { values: data, format: { type: "json" } },
            transform: [ {
              filter: {
                field: "id",
                oneOf: [
                  "salesvalue",
                ],
              },
            }],
            encoding: {
              x: { field: "date", type: "temporal" },
              y: { field: "value", type: "quantitative" },
              color: {
                value: "#4c78a8",
              },
              tooltip: [
                {
                  field: "value",
                  type: "quantitative",
                  title: "Myyntityön arvo",
                  format: "$.2f",
                },
                {
                  field: "project",
                  type: "nominal",
                  title: "Myyntityö",
                },
                {
                  field: "sold_by",
                  type: "nominal",
                  title: "Myyjä",
                },
              ]
            },
          },

          {
            height: HEIGHT,
            data: { values: { dummy: "dummy" } },
            transform: [
              {
                calculate: "now()",
                as: "current_time",
              },
            ],
            mark: { type: "rule", strokeDash: [8, 4] },

            encoding: {
              x: { field: "current_time", type: "temporal" },
              color: {
                value: "#F3AA60",
              },
              size: {
                value: 3,
              },
            },
          },
        ],
      },
    ],
  };

  vegaEmbed(div_class_to_embed, vlSpec, embedOpt);
}

async function refresh_salesmargin_vega(
  data,
  div_class_to_embed,
  span,
  monthly_target_billing,
  target_billing
) {
  console.log(
    `salesmargin refresh: ${div_class_to_embed}, ${span}, ${monthly_target_billing}, ${target_billing}`
  );
  console.log(
    `salesmargin refresh: ${typeof div_class_to_embed}, ${typeof span}, ${typeof monthly_target_billing}, ${typeof target_billing}`
  );
  console.log(data);

  const vlSpec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    background: "rgba(0,0,0,0%)",
    data: { values: data, format: { type: "json" } },
    config: {
      view: { fill: "white", width: 1300 },
    },

    transform: [
      {
        filter: {
          field: "id",
          oneOf: [
            "billing",
            "hour_cost",
            "maximum",
            "workhours",
            "absences",
            "saleswork",
          ],
        },
      },
      { timeUnit: "yearmonthdate", field: "date", as: "datevalue" },
      {
        calculate:
          // "datum.hour_cost * if(toDate(datum.date) < now(), datum.workhours + datum.absences + datum.saleswork, 0)",
          "if(datum.date <= now(), 1, 0)",
        as: "is_past",
      },
      {
        pivot: "id",
        value: "value",
        groupby: ["datevalue", "first_name", "is_past"],
        op: "sum",
      },
      {
        calculate:
          "datum.hour_cost * if(datum.is_part, datum.workhours + datum.absences + datum.saleswork, datum.maximum)",
        as: "cost",
      },
      {
        window: [
          { op: "sum", field: "billing", as: "w_billing" },
          { op: "sum", field: "cost", as: "w_cost" },
          { op: "sum", field: "maximum", as: "w_maximum" },
          { op: "sum", field: "workhours", as: "w_workhours" },
          { op: "sum", field: "absences", as: "w_absences" },
          { op: "sum", field: "saleswork", as: "w_saleswork" },
        ],
        sort: [{ field: "datevalue", order: "ascending" }],
        ignorePeers: false,
        groupby: ["first_name"],
        frame: [span, 0],
      },
      { extent: "w_billing", param: "w_billing_extent" },
    ],

    vconcat: [
      {
        layer: [
          {
            height: HEIGHT,
            mark: { type: "bar", width: { band: 0.9 } },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonth",
                type: "temporal",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%-d.%-m.'), timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%B') : timeFormat(datum.value, '%u') == '1' ? 'vko ' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 30,
                  tickCount: "month",
                  labelOffset: 4,
                  labelPadding: -24,
                  labelBound: true,
                  gridDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [5, 5],
                  },
                  tickDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [5, 5],
                  },
                },
              },

              y: {
                field: "billing",
                type: "quantitative",
                aggregate: "sum",
                axis: { title: "Kate" },
                format: "$.2f",
              },
              //color: {field: "first_name", type: "ordinal"},
              color: { value: "#4c78a8" },
              opacity: { value: 0.3 },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Ajanjakso",
                  format: "%B",
                  aggregate: "max",
                },
                {
                  field: "billing",
                  type: "quantitative",
                  title: "Laskutus",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            transform: [{ calculate: "-datum.cost", as: "negative_cost" }],

            mark: { type: "bar", width: { band: 0.9 } },

            encoding: {
              x: {
                field: "datevalue",
                type: "temporal",
                timeUnit: "yearmonth",
              },
              y: {
                field: "negative_cost",
                type: "quantitative",
                aggregate: "sum",
              },
              color: { value: "#e45756" },
              opacity: { value: 0.3 },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Ajanjakso",
                  format: "%B",
                  aggregate: "max",
                },
                {
                  field: "negative_cost",
                  type: "quantitative",
                  title: "Kulut",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            transform: [
              {
                calculate: "datum.billing - datum.cost",
                as: "salesmargin",
              },
            ],

            mark: { type: "bar", width: { band: 0.9 } },

            encoding: {
              x: {
                field: "datevalue",
                type: "temporal",
                timeUnit: "yearmonth",
              },
              y: {
                field: "salesmargin",
                type: "quantitative",
                aggregate: "sum",
              },
              color: {
                condition: {
                  test: { field: "salesmargin", aggregate: "sum", lte: 0 },
                  value: "#e45756",
                },
                value: "#4c78a8",
              },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Ajanjakso",
                  format: "%B",
                  aggregate: "max",
                },
                {
                  field: "salesmargin",
                  type: "quantitative",
                  title: "Kate",
                  format: "+$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            data: { values: { dummy: "dummy" } },
            transform: [
              {
                calculate: "now()",
                as: "current_time",
              },
            ],
            mark: { type: "rule", strokeDash: [8, 4] },

            encoding: {
              x: { field: "current_time", type: "temporal" },
              color: {
                value: "#F3AA60",
              },
              size: {
                value: 3,
              },
            },
          },

          {
            height: HEIGHT,
            data: { values: { y: monthly_target_billing } },
            mark: { type: "rule", strokeDash: [8, 4] },
            encoding: {
              y: { field: "y", type: "quantitative" },
              color: {
                value: "black",
              },
              size: {
                value: 0.5,
              },
            },
          },
        ],
      },

      {
        layer: [
          {
            height: HEIGHT,
            mark: { type: "area" },
            encoding: {
              x: {
                field: "datevalue",
                type: "temporal",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%b') : '', timeFormat(datum.value, '%u') == '1' ? 'v' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 13,
                  tickCount: "date",
                  labelOffset: 3,
                  labelPadding: -11,
                  labelBound: true,
                  labelOverlap: false,
                  gridDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [2, 6],
                  },
                  gridOpacity: {
                    condition: {
                      test: {
                        or: [
                          { field: "value", timeUnit: "day", equal: "Monday" },
                          { field: "value", timeUnit: "date", equal: 1 },
                        ],
                      },
                      value: 1,
                    },
                    value: 0,
                  },
                  tickDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [2, 6],
                  },
                  tickOpacity: {
                    condition: {
                      test: {
                        or: [{ field: "value", timeUnit: "date", equal: 1 }],
                      },
                      value: 1,
                    },
                    value: 0,
                  },
                },
              },

              y: {
                field: "w_billing",
                type: "quantitative",
                aggregate: "sum",
                axis: { title: "Kate" },
              },
              color: { value: "#4c78a8" },
              opacity: { value: 0.3 },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Jakson päätöspäivä",
                  format: "%d.%m.%Y",
                  aggregate: "max",
                },
                {
                  field: "w_billing",
                  type: "quantitative",
                  title: "Laskutus",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            transform: [{ calculate: "-datum.w_cost", as: "w_negative_cost" }],

            mark: { type: "area" },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: {
                field: "w_negative_cost",
                type: "quantitative",
                aggregate: "sum",
              },
              color: { value: "#e45756" },
              opacity: { value: 0.3 },
              // color: { field: "user", type: "nominal", }
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Jakson päätöspäivä",
                  format: "%d.%m.%Y",
                  aggregate: "max",
                },
                {
                  field: "w_negative_cost",
                  type: "quantitative",
                  title: "Kulut",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            // Positive salesmargin
            transform: [
              {
                aggregate: [
                  {
                    op: "sum",
                    field: "w_billing",
                    as: "w_billing",
                  },
                  {
                    op: "sum",
                    field: "w_cost",
                    as: "w_cost",
                  },
                ],
                groupby: ["datevalue"],
              },
              {
                calculate: "max(datum.w_billing-datum.w_cost, 0)",
                as: "w_salesmargin_p",
              },
            ],

            mark: { type: "area" },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: {
                field: "w_salesmargin_p",
                type: "quantitative",
                aggregate: "sum",
              },
              color: { value: "#4c78a8" },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Jakson päätöspäivä",
                  format: "%d.%m.%Y",
                  aggregate: "max",
                },
                {
                  field: "w_salesmargin_p",
                  type: "quantitative",
                  title: "Kate",
                  format: "+$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            // Negative salesmargin
            transform: [
              {
                aggregate: [
                  {
                    op: "sum",
                    field: "w_billing",
                    as: "w_billing",
                  },
                  {
                    op: "sum",
                    field: "w_cost",
                    as: "w_cost",
                  },
                ],
                groupby: ["datevalue"],
              },
              {
                calculate: "min(datum.w_billing-datum.w_cost, 0)",
                as: "w_salesmargin_n",
              },
            ],

            mark: { type: "area" },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: {
                field: "w_salesmargin_n",
                type: "quantitative",
                aggregate: "sum",
              },
              color: { value: "#e45756" },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Jakson päätöspäivä",
                  format: "%d.%m.%Y",
                  aggregate: "max",
                },
                {
                  field: "w_salesmargin_n",
                  type: "quantitative",
                  title: "Kate",
                  format: "+$.2f",
                  aggregate: "sum",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            data: { values: { dummy: "dummy" } },
            transform: [
              {
                calculate: "now()",
                as: "current_time",
              },
            ],
            mark: { type: "rule", strokeDash: [5, 5] },

            encoding: {
              x: { field: "current_time", type: "temporal" },
              color: {
                value: "#F3AA60",
              },
              size: {
                value: 3,
              },
            },
          },

          {
            data: { values: { y: target_billing } },
            mark: { type: "rule", strokeDash: [8, 4] },
            encoding: {
              y: {
                field: "y",
                type: "quantitative",
              },
              color: {
                value: "black",
              },
              size: {
                value: 0.5,
              },
            },
          },
        ],
      },
    ],
  };

  vegaEmbed(div_class_to_embed, vlSpec, embedOpt);
}

async function refresh_hours_vega(
  data,
  div_class_to_embed,
  span,
  monthly_target_billing,
  target_billing
) {
  console.log(
    `salesmargin refresh: ${div_class_to_embed}, ${span}, ${monthly_target_billing}, ${target_billing}`
  );
  console.log(data);

  const vlSpec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    background: "rgba(0,0,0,0%)",
    data: { values: data, format: { type: "json" } },
    config: {
      view: { fill: "white", width: 1300 },
    },
    width: 1300,
    transform: [
      {
        filter: {
          field: "id",
          oneOf: ["maximum", "workhours", "absences", "saleswork"],
        },
      },
      { timeUnit: "yearmonthdate", field: "date", as: "datevalue" },
      { timeUnit: "yearmonth", field: "date", as: "monthvalue" },
      {
        calculate: "if(datum.date > now(), true, false)",
        as: "is_forecast",
      },
      {
        calculate: "if(datum.productive === false, false, true)",
        as: "is_productive",
      },
      {
        calculate: "if(datum.id == 'workhours', datum.value, 0)",
        as: "workhours",
      },
      {
        calculate: "if(datum.id != 'maximum', datum.value, 0)",
        as: "hours",
      },
      {
        calculate:
          "if(datum.id == 'workhours' && datum.productive, datum.value, 0)",
        as: "productive_workhours",
      },
      {
        calculate: "if(datum.id == 'absences', datum.value, 0)",
        as: "absences",
      },
      {
        calculate: "if(datum.id == 'saleswork', datum.value, 0)",
        as: "saleswork",
      },
      {
        calculate: "if(datum.id == 'maximum', datum.value, 0)",
        as: "maximum",
      },
      {
        calculate:
          "if(datum.id == 'workhours', if(datum.productive, 'workhours_productive', 'workhours_unproductive'), datum.id)",
        as: "id",
      },
      {
        window: [
          { op: "sum", field: "value", as: "w_value" },
          { op: "sum", field: "maximum", as: "w_maximum" },
          { op: "sum", field: "workhours", as: "w_workhours" },
          {
            op: "sum",
            field: "productive_workhours",
            as: "w_productive_workhours",
          },
          { op: "sum", field: "absences", as: "w_absences" },
          { op: "sum", field: "saleswork", as: "w_saleswork" },
        ],
        sort: [{ field: "datevalue", order: "ascending" }],
        ignorePeers: false,
        groupby: ["user", "id"],
        frame: [span, 0],
      },
    ],

    vconcat: [
      {
        layer: [
          {
            height: HEIGHT,
            transform: [
              {
                joinaggregate: [
                  {
                    op: "sum",
                    field: "maximum",
                    as: "total_maximum",
                  },
                  {
                    op: "sum",
                    field: "hours",
                    as: "total_hours",
                  },
                  {
                    op: "sum",
                    field: "workhours",
                    as: "total_workhours",
                  },
                  {
                    op: "sum",
                    field: "productive_workhours",
                    as: "total_productive_workhours",
                  },
                  {
                    op: "sum",
                    field: "absences",
                    as: "total_absences",
                  },
                  {
                    op: "sum",
                    field: "saleswork",
                    as: "total_saleswork",
                  },
                ],
                groupby: ["monthvalue"],
              },
              { filter: "datum.id != 'maximum'" },
            ],
            mark: { type: "bar", width: { band: 0.9 } },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonth",
                type: "temporal",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%-d.%-m.'), timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%B') : timeFormat(datum.value, '%u') == '1' ? 'vko ' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 30,
                  tickCount: "month",
                  labelOffset: 4,
                  labelPadding: -24,
                  labelBound: true,
                  gridDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [5, 5],
                  },
                  tickDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [5, 5],
                  },
                },
              },

              y: {
                field: "value",
                type: "quantitative",
                aggregate: "sum",
                title: "Tuntikirjauksia",
              },
              color: { field: "id", type: "nominal" },
              // opacity: { field: "is_productive", sort: "ascending" },
              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Ajanjakso",
                  format: "%B",
                  aggregate: "max",
                },
                {
                  field: "total_maximum",
                  type: "quantitative",
                  title: "Työsop. mukainen maksimi",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "total_hours",
                  type: "quantitative",
                  title: "Tunteja yhteensä",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "total_workhours",
                  type: "quantitative",
                  title: "Työtunteja",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "total_productive_workhours",
                  type: "quantitative",
                  title: "Joista tuottavia",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "total_saleswork",
                  type: "quantitative",
                  title: "Tarjottua työtä",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "total_absences",
                  type: "quantitative",
                  title: "Poissaoloja",
                  format: ".1f",
                  aggregate: "max",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            transform: [{ filter: "datum.id == 'maximum'" }],
            mark: {
              type: "line",
              point: true,
              interpolate: "step-after",
              strokeDash: [8, 4],
              // xOffset: 75,
            },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonth",
                type: "temporal",
              },
              y: { field: "value", type: "quantitative", aggregate: "sum" },
              color: { value: "#1D5B79" },
            },
          },

          {
            height: HEIGHT,
            data: { values: { dummy: "dummy" } },
            transform: [
              {
                calculate: "now()",
                as: "current_time",
              },
            ],
            mark: { type: "rule", strokeDash: [8, 4] },

            encoding: {
              x: { field: "current_time", type: "temporal" },
              color: {
                value: "#F3AA60",
              },
              size: {
                value: 3,
              },
            },
          },
        ],
      },
      // VE 2
      {
        layer: [
          {
            height: HEIGHT,
            transform: [
              {
                impute: "value",
                key: "datevalue",
                value: 0,
                groupby: ["datevalue", "id"],
              },
              {
                aggregate: [
                  {
                    op: "sum",
                    field: "maximum",
                    as: "maximum",
                  },
                  {
                    op: "sum",
                    field: "hours",
                    as: "hours",
                  },
                  {
                    op: "sum",
                    field: "value",
                    as: "agg_value",
                  },
                  {
                    op: "sum",
                    field: "workhours",
                    as: "workhours",
                  },
                  {
                    op: "sum",
                    field: "productive_workhours",
                    as: "productive_workhours",
                  },
                  {
                    op: "sum",
                    field: "absences",
                    as: "absences",
                  },
                  {
                    op: "sum",
                    field: "saleswork",
                    as: "saleswork",
                  },
                ],
                groupby: ["datevalue", "id"],
              },
              {
                joinaggregate: [
                  {
                    op: "sum",
                    field: "maximum",
                    as: "total_maximum",
                  },

                  {
                    op: "sum",
                    field: "hours",
                    as: "total_hours",
                  },
                  {
                    op: "sum",
                    field: "workhours",
                    as: "total_workhours",
                  },
                  {
                    op: "sum",
                    field: "productive_workhours",
                    as: "total_productive_workhours",
                  },
                  {
                    op: "sum",
                    field: "absences",
                    as: "total_absences",
                  },
                  {
                    op: "sum",
                    field: "saleswork",
                    as: "total_saleswork",
                  },
                ],
                groupby: ["datevalue"],
              },
              { filter: "datum.id != 'maximum'" },
              {
                sort: [{ field: "datevalue" }],
                window: [
                  {
                    op: "sum",
                    field: "agg_value",
                    as: "w_value",
                  },
                  {
                    op: "sum",
                    field: "total_hours",
                    as: "w_hours",
                  },
                  {
                    op: "sum",
                    field: "total_maximum",
                    as: "w_maximum",
                  },
                  {
                    op: "sum",
                    field: "total_workhours",
                    as: "w_workhours",
                  },
                  {
                    op: "sum",
                    field: "total_productive_workhours",
                    as: "w_productive_workhours",
                  },
                  {
                    op: "sum",
                    field: "total_saleswork",
                    as: "w_saleswork",
                  },
                  {
                    op: "sum",
                    field: "total_absences",
                    as: "w_absences",
                  },
                ],
                frame: [span, 0],
                groupby: ["id"],
              },
            ],
            mark: {
              type: "area",
              // point: true,
              tooltip: true,
            },
            encoding: {
              x: {
                field: "datevalue",
                type: "temporal",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%b') : '', timeFormat(datum.value, '%u') == '1' ? 'v' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 13,
                  tickCount: "date",
                  labelOffset: 3,
                  labelPadding: -11,
                  labelBound: true,
                  labelOverlap: false,
                  gridDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [2, 6],
                  },
                  gridOpacity: {
                    condition: {
                      test: {
                        or: [
                          { field: "value", timeUnit: "day", equal: "Monday" },
                          { field: "value", timeUnit: "date", equal: 1 },
                        ],
                      },
                      value: 1,
                    },
                    value: 0,
                  },
                  tickDash: {
                    condition: {
                      test: { field: "value", timeUnit: "date", equal: 1 },
                      value: [],
                    },
                    value: [2, 6],
                  },
                  tickOpacity: {
                    condition: {
                      test: {
                        or: [{ field: "value", timeUnit: "date", equal: 1 }],
                      },
                      value: 1,
                    },
                    value: 0,
                  },
                },
              },
              y: {
                field: "w_value",
                type: "quantitative",
                aggregate: "sum",
                title: "Tuntikirjauksia",
              },
              color: {
                field: "id",
                type: "nominal",
                scale: {
                  domain: [
                    "workhours_productive",
                    "workhours_unproductive",
                    "saleswork",
                    "absences",
                  ],
                  range: ["#EF6262", "#f59e8c", "#1D5B79", "#468B97"], // "#1D5B79"
                },
                legend: {
                  labelExpr:
                    "datum.label == 'workhours_productive' ? 'Asiakastyö' : datum.label == 'workhours_unproductive' ? 'Sisäinen työ' : datum.label == 'saleswork' ? 'Tarjottu työ' : datum.label == 'absences' ? 'Poissaolot' : 'xxx'",
                },
                title: "Tuntikirjaukset",
              },

              tooltip: [
                {
                  field: "datevalue",
                  type: "temporal",
                  title: "Jakson päätöspäivä",
                  format: "%d.%m.%Y",
                  aggregate: "max",
                },
                {
                  field: "w_maximum",
                  type: "quantitative",
                  title: "Työsop. mukainen maksimi",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "w_hours",
                  type: "quantitative",
                  title: "Tunteja yhteensä",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "w_workhours",
                  type: "quantitative",
                  title: "Työtunteja",
                  format: ".1f",
                  aggregate: "sum",
                },
                {
                  field: "w_productive_workhours",
                  type: "quantitative",
                  title: "Joista tuottavia",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "w_saleswork",
                  type: "quantitative",
                  title: "Tarjottua työtä",
                  format: ".1f",
                  aggregate: "max",
                },
                {
                  field: "w_absences",
                  type: "quantitative",
                  title: "Poissaoloja",
                  format: ".1f",
                  aggregate: "max",
                },
              ],
            },
          },

          {
            height: HEIGHT,
            transform: [
              { filter: "datum.id == 'maximum'" },
              {
                impute: "value",
                key: "datevalue",
                value: 0,
                groupby: ["datevalue", "id"],
              },
              {
                aggregate: [
                  {
                    op: "sum",
                    field: "value",
                    as: "agg_value",
                  },
                ],
                groupby: ["datevalue", "id"],
              },
              {
                sort: [{ field: "datevalue" }],
                window: [
                  {
                    op: "sum",
                    field: "agg_value",
                    as: "w_value",
                  },
                ],
                frame: [span, 0],
                groupby: ["id"],
              },
            ],
            mark: {
              type: "line",
              strokeDash: [8, 4],
            },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonthdate",
                type: "temporal",
              },
              y: { field: "w_value", type: "quantitative", aggregate: "sum" },
              color: { value: "#1D5B79" },
            },
          },

          {
            height: HEIGHT,
            data: { values: { dummy: "dummy" } },
            transform: [
              {
                calculate: "now()",
                as: "current_time",
              },
            ],
            mark: { type: "rule", strokeDash: [8, 4] },

            encoding: {
              x: { field: "current_time", type: "temporal" },
              color: {
                value: "#F3AA60",
              },
              size: {
                value: 3,
              },
            },
          },
        ],
      },
    ],
  };

  vegaEmbed(div_class_to_embed, vlSpec, embedOpt);
}

async function refresh_all_vega(data) {
  const AVERAGE_DAYS_IN_MONTH = 30.4368499;
  const span = Number(document.getElementById("span").value) - 1;
  const monthly_target_billing = Number(
    document.getElementById("billing_target").value
  );
  const target_billing =
    (monthly_target_billing / AVERAGE_DAYS_IN_MONTH) * span;

  document.getElementById("billing_target_span").value = target_billing;

  data.forEach(async (element) => {
    await element.refresh_func(
      element.data,
      element.div,
      span,
      monthly_target_billing,
      target_billing
    );
  });
}

export async function main() {
  console.log("loading");
  const content =
    '<div class="header"><h2>Laskutus ja kate</h2></div><div id="salesmargin"></div><div class="header"><h2>Tuntikirjaukset ja työmäärä</h2></div><div id="hours"></div><h2>Myynti</h2><div id="sales"></div>';
  const spinner = '<div class="loader-parent"><div class="loader"></div></div>';

  document.getElementById("main").innerHTML = spinner;

  const t0 = new Date().getTime();

  const start = document.getElementById("start").value;
  const end = document.getElementById("end").value;

  const data_url = `/kpi/totals?start=${start}&end=${end}`;

  const response = await fetch(data_url);
  const json = await response.json();

  const t1 = new Date().getTime();

  const data = [
    {
      data: json,
      div: "#salesmargin",
      refresh_func: refresh_salesmargin_vega,
    },
    {
      data: json,
      div: "#hours",
      refresh_func: refresh_hours_vega,
    },
    { data: json, div: "#sales", refresh_func: refresh_sales_vega },
  ];

  document.getElementById("info").innerHTML = `${json.length} riviä dataa, ${(
    (t1 - t0) /
    1000.0
  ).toFixed(2)}s`;

  document.querySelectorAll("input").forEach((input) => {
    input.replaceWith(input.cloneNode(true));
  });

  document.querySelectorAll("input").forEach((input) => {
    input.addEventListener("change", () => {
      refresh_all_vega(data);
    });
  });

  document.getElementById("main").innerHTML = content;

  await refresh_all_vega(data);

  const button = document.getElementById("reload");
  button.replaceWith(button.cloneNode(true));
  document.getElementById("reload").onclick = main;
}

// Initialization
window.addEventListener("load", main);

document.getElementById("start").value = luxon.DateTime.now()
  .minus({ months: 4 })
  .startOf("month")
  .toISODate();
document.getElementById("end").value = luxon.DateTime.now()
  .plus({ months: 3 })
  .endOf("month")
  .toISODate();
