"use strict";

// Poor man's import...
dfd = window.dfd;
luxon = window.luxon;

const HEIGHT = 350;

const CONFIG = {
  titleFontSize: 17,
  titleFontWeight: "normal",
  titleFontStyle: "normal",
  titleColor: "#333",
  titlePadding: 30,
};

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

async function refresh_history_vega(
  data_forecast,
  data_realized,
  div_class_to_embed,
  span,
  monthly_target_billing,
  target_billing
) {
  // The following is asinine bs
  const datetimes = data_forecast.map((elem) =>
    luxon.DateTime.fromISO(elem.forecast_date)
  );

  // Too many values for min(...datetimes) spreading
  const min_ts = datetimes.reduce((min, val) => (val < min ? val : min)); // luxon.DateTime.min(...datetimes);
  const max_ts = datetimes.reduce((min, val) => (val < min ? val : min)); // luxon.DateTime.max(...datetimes);

  const dur = luxon.Duration.fromObject({ days: 1 });

  let timestamps_for_impute = [];

  for (let i = min_ts; i <= max_ts; i = i.plus(dur)) {
    timestamps_for_impute.push(i.ts);
  }

  console.log(timestamps_for_impute);

  const min_date_span = min_ts.plus(luxon.Duration.fromObject({ days: span }));

  ///////

  const retro_metro0 = [
    "#ea5545",
    "#f46a9b",
    "#ef9b20",
    "#edbf33",
    "#ede15b",
    "#bdcf32",
    "#87bc45",
    "#27aeef",
    "#b33dc6",
  ];

  const retro_metro2 = [
    "#fd7f6f",
    "#7eb0d5",
    "#b2e061",
    "#bd7ebe",
    "#ffb55a",
    "#ffee65",
    "#beb9db",
    "#fdcce5",
    "#8bd3c7",
  ];
  const retro_metro3 = [
    "#54bebe",
    "#76c8c8",
    "#98d1d1",
    "#badbdb",
    "#dedad2",
    "#e4bcad",
    "#df979e",
    "#d7658b",
    "#c80064",
  ];
  const color_scheme = "category20c";
  const vlSpec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    background: "rgba(0,0,0,0%)",
    data: { values: data_forecast, format: { type: "json" } },
    config: {
      view: { fill: "white", width: 1300 },
    },

    transform: [
      { timeUnit: "yearmonthdate", field: "date", as: "date" },
      {
        timeUnit: "yearmonthdate",
        field: "forecast_date",
        as: "forecast_datevalue",
      },
    ],

    vconcat: [
      {
        layer: [
          {
            params: [
              {
                name: "user-selection",
                select: { type: "point", fields: ["first_name"] },
                bind: "legend",
              },
            ],
            transform: [
              {
                filter: {
                  field: "forecast_length",
                  lte: span,
                },
              },
              {
                aggregate: [
                  {
                    op: "sum",
                    field: "value",
                    as: "w_value",
                  },
                ],
                groupby: ["forecast_datevalue", "first_name"],
              },
              //   { calculate: `timeOffset('date', datum.forecast_date, ${span})`, as: "forecast_span_end_date" }
              {
                calculate: `timeOffset('date', datum.forecast_datevalue, ${span})`,
                as: "new_date",
              },
              {
                calculate: `datum.new_date <= toDate(now())`,
                as: "is_in_range",
              },
              {
                filter: {
                  field: "is_in_range",
                  equal: true,
                },
              },
              {
                joinaggregate: [
                  {
                    op: "sum",
                    field: "w_value",
                    as: "total_value",
                  },
                ],
                groupby: ["new_date"],
              },
              //   { window: [
              //     { op: "sum", field: "value", as: "w_value" },
              //   ],},
              //   {
              //     window: [
              //       { op: "sum", field: "value", as: "w_value" },
              //       { op: "sum", field: "target", as: "w_target" },
              //     ],
              //     sort: [{ field: "datevalue", order: "ascending" }],
              //     ignorePeers: false,
              //     groupby: ["forecast_date", "first_name"],
              //     frame: [null, 0],
              //   },
            ],
            height: HEIGHT,
            mark: { type: "area", tooltip: true },
            encoding: {
              x: {
                field: "new_date",
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
                axis: {
                  title: `Laskutus + ennuste, juokseva ${span + 1} vrk`,
                  titleFontSize: CONFIG.titleFontSize,
                  titleFontWeight: CONFIG.titleFontWeight,
                  titleFontStyle: CONFIG.titleFontStyle,
                  titleColor: CONFIG.titleColor,
                  titlePadding: CONFIG.titlePadding,
                },
              },
              color: {
                condition: {
                  param: "user-selection",
                  field: "first_name",
                  type: "nominal",
                  title: "Projektipäällikkö",
                  scale: {
                    scheme: color_scheme,
                  },
                },
                value: "#bbb",
                title: "Projektipäällikkö",
                scale: {
                  scheme: color_scheme,
                },
              },
              opacity: {
                condition: {
                  param: "user-selection",
                  value: 1.0,
                },
                value: 0.5,
              },
              tooltip: [
                {
                  field: "forecast_datevalue",
                  type: "temporal",
                  title: "Aikavälin alku",
                  format: "%d.%m.%Y",
                },
                {
                  field: "new_date",
                  type: "temporal",
                  title: "Aikavälin loppu",
                  format: "%d.%m.%Y",
                },
                {
                  field: "first_name",
                  type: "nominal",
                  title: "Projekipäällikkö",
                },
                {
                  field: "w_value",
                  type: "quantitative",
                  title: "Laskutusennuste",
                  format: "$.2f",
                  aggregate: "sum",
                },
                {
                  field: "total_value",
                  type: "quantitative",
                  title: "Laskutusennuste, yhteensä",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            }, // encoding
          },

          {
            data: { values: data_realized, format: { type: "json" } },
            transform: [
              {
                impute: "value",
                key: "date",
                value: 0,
                groupby: ["first_name"],
                keyvals: timestamps_for_impute,
              },
              {
                aggregate: [
                  {
                    op: "sum",
                    field: "value",
                    as: "value",
                  },
                ],
                groupby: ["date", "first_name"],
              },
              {
                window: [{ op: "sum", field: "value", as: "w_value" }],
                sort: [{ field: "date", order: "ascending" }],
                ignorePeers: false,
                groupby: ["first_name"],
                frame: [span, 0],
              },
              {
                calculate: `datum.date >= toDate(${min_date_span.ts}) && datum.date <= toDate(now())`,
                as: "is_in_range",
              },
              {
                filter: {
                  field: "is_in_range",
                  equal: true,
                },
              },
              {
                filter: {
                  param: "user-selection",
                },
              },
            ],
            height: HEIGHT,
            mark: { type: "line", point: true, tooltip: true },
            encoding: {
              x: {
                field: "date",
                timeUnit: "yearmonthdate",
                type: "temporal",
              },
              y: { field: "w_value", type: "quantitative", aggregate: "sum" },
              tooltip: [
                {
                  field: "date",
                  type: "temporal",
                  title: "Aikavälin loppu",
                  format: "%d.%m.%Y",
                },
                {
                  field: "w_value",
                  type: "quantitative",
                  title: "Toteutunut laskutus",
                  format: "$.2f",
                  aggregate: "sum",
                },
              ],
            }, // encoding
          },
        ], // layer
      },
    ], // vconcat
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
      element.data_forecast,
      element.data_realized,
      element.div,
      span,
      monthly_target_billing,
      target_billing
    );
  });
}

export async function main() {
  const content =
    '<div class="header"><h2>Laskutusennusteet ja niiden toteutuminen</h2></div><div id="history"></div>';
  const spinner = '<div class="loader-parent"><div class="loader"></div></div>';

  document.getElementById("main").innerHTML = spinner;

  const t0 = new Date().getTime();

  const start = document.getElementById("start").value;
  const end = document.getElementById("end").value;

  const forecast_data_url = `/kpi/billing_history?start=${start}&end=${end}`;
  //   const forecast_data_url = "/static/temp_history.json";
  const realized_data_url = "/kpi/billing";

  const [response_forecasts, response_realized] = await Promise.all([
    fetch(forecast_data_url),
    fetch(realized_data_url),
  ]);

  const [forecast_json, realized_json] = await Promise.all([
    response_forecasts.json(),
    response_realized.json(),
  ]);

  const t1 = new Date().getTime();

  const data = [
    {
      data_forecast: forecast_json,
      data_realized: realized_json,
      div: "#history",
      refresh_func: refresh_history_vega,
    },
  ];

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

  const t2 = new Date().getTime();
  const delta1 = (t1 - t0) / 1000.0;
  const delta2 = (t2 - t1) / 1000.0;
  const delta_total = (t2 - t0) / 1000.0;

  document.getElementById("info").innerHTML = `${
    forecast_json.length + realized_json.length
  } riviä dataa, haku ${delta1.toFixed(2)}s, käsittely ${delta2.toFixed(
    2
  )}s, yhteensä ${delta_total.toFixed(2)}s`;
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
