const embedOpt = {
  mode: "vega-lite",
  actions: false,
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
      view: { fill: "white", width: 1000 },
    },

    transform: [
      { filter: { param: "user_selection" } },
      { timeUnit: "yearmonthdate", field: "date", as: "datevalue" },
      {
        pivot: "id",
        value: "value",
        groupby: ["datevalue", "user"],
        op: "sum",
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
        groupby: ["user"],
        frame: [span, 0],
      },
      { extent: "w_billing", param: "w_billing_extent" },
    ],

    vconcat: [
      {
        layer: [
          {
            params: [
              {
                name: "user_selection",
                select: { type: "point", fields: ["user"] },
                bind: {
                  input: "select",
                  options: [null, "0d100bc1-8376-7ac5-7bbb-055cdc20497d"],
                },
              },
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
                field: "billing",
                type: "quantitative",
                aggregate: "sum",
                axis: { title: "Kate" },
              },
              color: { value: "#4c78a8" },
              opacity: { value: 0.3 },
            },
          },

          {
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
            },
          },

          {
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
            },
          },

          {
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
                value: "black",
              },
              size: {
                value: 1,
              },
            },
          },

          {
            data: { values: { y: monthly_target_billing } },
            mark: { type: "rule", strokeDash: [5, 5] },
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
            mark: "area",
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
            },
          },

          {
            transform: [{ calculate: "-datum.w_cost", as: "w_negative_cost" }],

            mark: "area",

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
            },
          },

          {
            transform: [
              {
                calculate: "datum.w_billing-datum.w_cost",
                as: "w_salesmargin",
              },
            ],

            mark: { type: "bar", size: 4 },

            encoding: {
              x: { field: "datevalue", type: "temporal" },
              y: {
                field: "w_salesmargin",
                type: "quantitative",
                aggregate: "sum",
              },
              color: {
                condition: {
                  test: { field: "w_salesmargin", lte: 0, aggregate: "sum" },
                  value: "#e45756",
                },
                value: "#4c78a8",
              },
            },
          },

          {
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
                value: "black",
              },
              size: {
                value: 0.5,
              },
            },
          },

          {
            data: { values: { y: target_billing } },
            mark: { type: "rule", strokeDash: [5, 5] },
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
      view: { fill: "white", width: 1000 },
    },

    transform: [
      { timeUnit: "yearmonthdate", field: "date", as: "datevalue" },
      //   {
      //     pivot: "id",
      //     value: "value",
      //     groupby: ["datevalue", "user", "productive"],
      //     op: "sum",
      //   },
      //   {
      //     fold: ["maximum", "workhours", "saleswork", "absences"],
      //     as: ["id", "value"]
      //   },
      {
        impute: "value",
        key: "datevalue",
        value: 0,
        groupby: ["user", "id", "productive"],
      },
      {
        window: [{ op: "sum", field: "value", as: "w_value" }],
        sort: [{ field: "datevalue", order: "ascending" }],
        ignorePeers: false,
        groupby: ["user", "id", "productive"],
        frame: [span, 0],
      },
      //   { extent: "w_billing", param: "w_billing_extent" },
    ],

    vconcat: [
      {
        layer: [
          {
            transform: [{ filter: "datum.id != 'maximum'" }],
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

              y: { field: "value", type: "quantitative", aggregate: "sum" },
              color: { field: "id", type: "nominal" },
              opacity: { field: "productive", sort: "decending" },
            },
          },

          {
            transform: [{ filter: "datum.id == 'maximum'" }],
            mark: {
              type: "line",
              interpolate: "step",
              strokeDash: [8, 8],
              xOffset: 75,
            },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonth",
                type: "temporal",
              },
              y: { field: "value", type: "quantitative", aggregate: "sum" },
            },
          },
        ],
      },

      {
        layer: [
          {
            transform: [{ filter: "datum.id != 'maximum'" }],
            mark: { type: "area", width: { band: 0.9 } },
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

              y: { field: "w_value", type: "quantitative", aggregate: "sum" },
              color: { field: "id", type: "nominal" },
              opacity: { field: "productive", sort: "decending" },
            },
          },

          {
            transform: [{ filter: "datum.id == 'maximum'" }],
            mark: {
              type: "line",
              interpolate: "step",
              strokeDash: [8, 8],
              //xOffset: 75,
            },
            encoding: {
              x: {
                field: "datevalue",
                timeUnit: "yearmonthdate",
                type: "temporal",
              },
              y: { field: "w_value", type: "quantitative", aggregate: "sum" },
            },
          },
        ],
      },
    ],
  };

  vegaEmbed(div_class_to_embed, vlSpec, embedOpt);
}

async function refresh_all_vega(data) {
  const span = Number(document.getElementById("span").value);
  const monthly_target_billing = Number(
    document.getElementById("billing_target").value
  );
  const target_billing = (monthly_target_billing / 30.4368499) * span;

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

async function init(base_url, kpi) {
  //const url = `${base_url}kpi/get/${kpi}`;
  const data_url = "../static/tmp.json";

  const [salesmargin_json, hours_json] = await Promise.all([
    fetch(data_url),
    fetch("hours.json"),
  ]);

  const data = [
    {
      data: await salesmargin_json.json(),
      div: "#salesmargin",
      refresh_func: refresh_salesmargin_vega,
    },
    {
      data: await hours_json.json(),
      div: "#hours",
      refresh_func: refresh_hours_vega,
    },
  ];

  const filter_inputs = document.querySelectorAll("input");

  filter_inputs.forEach((input) => {
    input.addEventListener("change", () => {
      refresh_all_vega(data);
    });
  });

  await refresh_all_vega(data);
}
