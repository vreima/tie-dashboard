function lerp(start, end, amt) {
  return (1 - amt) * start + amt * end;
}

function threepoint_lerp(start, mid, end, amt) {
  return amt < 0.5
    ? lerp(start, mid, amt / 0.5)
    : lerp(mid, end, (amt - 0.5) / 0.5);
}

function gradient(x, y, alpha) {
  ratio = (x + y) / 2.0;

  hue = threepoint_lerp(94, 60, 19, ratio);
  s = threepoint_lerp(54, 100, 96, ratio);
  l = threepoint_lerp(59, 87, 67, ratio);

  // console.log(`${ratio} :: hsla(${Math.round(hue)}, ${Math.round(s)}%, ${Math.round(l)}%, ${alpha}%)`)

  return `hsla(${Math.round(hue)}, ${Math.round(s)}%, ${Math.round(
    l
  )}%, ${alpha}%)`;
}

function reset_canvas() {
  const c = document.getElementById("area");
  const w = c.offsetWidth;
  const h = c.offsetHeight;

  const ctx = c.getContext("2d");

  ctx.clearRect(0, 0, w, h);

  // Background
  let h_gradient = ctx.createLinearGradient(0, h, w, 0);
  h_gradient.addColorStop(0, gradient(0, 0, 30));
  h_gradient.addColorStop(0.5, gradient(0.5, 0.5, 30));
  h_gradient.addColorStop(1, gradient(1, 1, 30));

  ctx.fillStyle = h_gradient;
  ctx.fillRect(0, 0, w, h);

  // Reset
  ctx.setLineDash([]);
  ctx.strokeStyle = "rgb(50, 50, 50)";

  return ctx;
}

async function update(event) {
  if (finished) return;

  const x = event.offsetX;
  const y = event.offsetY;

  reset_canvas();

  const c = document.getElementById("area");
  const w = c.offsetWidth;
  const h = c.offsetHeight;

  const ctx = c.getContext("2d");

  ctx.lineWidth = 2;
  ctx.lineCap = "round";
  ctx.setLineDash([8, 16]);
  ctx.strokeStyle = gradient(x / w, 1 - y / h, 100);

  ctx.beginPath();
  ctx.moveTo(x, 0);
  ctx.lineTo(x, h);
  ctx.moveTo(0, y);
  ctx.lineTo(w, y);
  ctx.stroke();
}

function cross(ctx, x, y, cross_size) {
  ctx.beginPath();
  ctx.moveTo(x - cross_size, y - cross_size);
  ctx.lineTo(x + cross_size, y + cross_size);
  ctx.moveTo(x - cross_size, y + cross_size);
  ctx.lineTo(x + cross_size, y - cross_size);
  ctx.stroke();
}

function dot(ctx, x, y, dot_size, color) {
  ctx.fillStyle = color;
  ctx.strokeStyle = "rgba(255, 255, 255, 100%)";
  ctx.lineWidth = 1;

  ctx.beginPath();
  ctx.arc(x, y, dot_size, 0, 2 * Math.PI);
  ctx.fill();
  ctx.stroke();
}

async function myFunction(event, hostname, user_name) {
  if (finished) return;

  finished = true;

  const x = event.offsetX;
  const y = event.offsetY;

  reset_canvas();

  const c = document.getElementById("area");
  const w = c.offsetWidth;
  const h = c.offsetHeight;

  const cross_size = 10;

  const ctx = c.getContext("2d");

  ctx.lineCap = "round";

  ctx.strokeStyle = "rgba(0, 0, 0, 70%)";
  ctx.lineWidth = 12;
  cross(ctx, x, y, cross_size);

  ctx.strokeStyle = gradient(x / w, 1 - y / h, 100);
  ctx.lineWidth = 7;
  cross(ctx, x, y, cross_size);

  const scaled_x = x / w;
  const scaled_y = 1.0 - y / h;

  const response = await fetch(
    `/kiire/save/${user_name}?x=${scaled_x}&y=${scaled_y}`
  );
  const jsonData = await response.json();

  console.log(jsonData);
}

// async function fetch_pressure(hostname, offset) {
//   const c = document.getElementById("area");
//   const w = c.offsetWidth;
//   const h = c.offsetHeight;
//   const ctx = reset_canvas();

//   let response;

//   try {
//     response = await fetch(`${hostname}pressure/?offset=${offset}`);
//   } catch (error) {
//     document.getElementById(
//       "vis"
//     ).innerText = `Error fetching $${hostname}pressure/?offset=${offset}: ${error}`;
//     return;
//   }

//   const jsonData = await response.json();

//   ctx.lineCap = "round";
//   ctx.strokeStyle = "rgba(0, 0, 0, 50%)";
//   ctx.lineWidth = 2;

//   mean_x = 0;
//   mean_y = 0;

//   jsonData.forEach((element) => {
//     cross(ctx, element.x * w, (1 - element.y) * h, 5);
//     mean_x += element.x;
//     mean_y += element.y;
//   });

//   mean_x /= jsonData.length;
//   mean_y /= jsonData.length;

//   mx = mean_x * w;
//   my = (1 - mean_y) * h;

//   ctx.strokeStyle = "rgba(0, 0, 0, 100%)";
//   ctx.lineWidth = 3;
//   cross(ctx, mx, my, 10);

//   ctx.font = "18px Lato, sans-serif";
//   ctx.fillStyle = "black";
//   ctx.fillText("keskiarvo", mx + 20, my + 5);
// }

function dateToString(date) {
  return `${date.getFullYear()}-${(date.getMonth() + 1)
    .toString()
    .padStart(2, "0")}-${date.getDate().toString().padStart(2, "0")}`;
}

let global_data = null;
let global_hostname = "http://127.0.0.1:8000/";

async function init_vega(hostname) {
  global_hostname = hostname;

  const today = new Date();
  const start = new Date(2023, 5, 1);
  // const weekAgo = new Date(today.valueOf() - 1000 * 60 * 60 * 24 * 7);

  document.getElementById("start-date").value = dateToString(start);
  document.getElementById("end-date").value = dateToString(today);

  await refresh_data(null);

  const filter_inputs = document.querySelectorAll("fieldset#filters input");

  filter_inputs.forEach((input) => {
    input.addEventListener("change", refresh_data);
  });
}

async function init(hostname) {
  reset_canvas();

  global_hostname = hostname;

  const filter_inputs = document.querySelectorAll("fieldset#filters input");

  filter_inputs.forEach((input) => {
    input.addEventListener("change", refresh_data);
  });

  const graphics_inputs = document.querySelectorAll("fieldset#graphics input");

  graphics_inputs.forEach((input) => {
    input.addEventListener("change", refresh_graphics);
  });

  const today = new Date();
  const weekAgo = new Date(today.valueOf() - 1000 * 60 * 60 * 24 * 7);

  document.getElementById("start-date").value = dateToString(weekAgo);
  document.getElementById("end-date").value = dateToString(today);

  await refresh_data(null);
  await refresh_graphics(null);
}

async function refresh_data(event) {
  const startDate = document.getElementById("start-date").value;
  const endDate = document.getElementById("end-date").value;
  const userFilter = document.getElementById("user-filter").value;
  // const unitFilter = document.getElementById("businessunit-filter").value;

  try {
    global_data = await fetch_pressure_data(
      global_hostname,
      startDate,
      endDate,
      userFilter,
      null // unitFilter
    );

    await refresh_vega(global_data);
  } catch (error) {
    document.getElementById("vis").innerText = `${error}`;
    return;
  }
}

async function refresh_vega(data) {
  // Assign the specification to a local variable vlSpec.
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
  const vlSpec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",

    params: [
      {
        name: "toggle",
        bind: {
          input: "checkbox",
          name: "Henkilönmukainen väritys ja keskiarvot pois",
        },
      },
      {
        name: "trails",
        bind: {
          input: "checkbox",
          name: "Näytä hännät",
        },
      },
      { name: "fontSize", value: 14 },
    ],

    background: "rgba(0,0,0,0%)",

    data: {
      values: data,
    },
    vconcat: [
      {
        layer: [
          {
            height: 700,
            title: "Kiirekysely - tulokset",
            transform: [
              {
                timeUnit: "yearmonthdatehoursminutes",
                field: "date",
                as: "timestamp",
              },
              {
                joinaggregate: [
                  { op: "min", field: "timestamp", as: "first" },
                  { op: "max", field: "timestamp", as: "last" },
                ],
                groupby: ["user"],
              },
              {
                calculate:
                  "(datum.last - datum.timestamp)/(datum.last - datum.first)",
                as: "oldness",
              },
              { filter: { param: "brush" } },
            ],
            mark: {
              type: "trail",
            },
            params: [
              {
                name: "user-selection",
                select: { type: "point", fields: ["user"] },
                bind: "legend",
              },
            ],
            encoding: {
              y: {
                field: "y",
                type: "quantitative",
                scale: { domain: [0.0, 1.0] },
                axis: {
                  title: "Kiireen tuntu",
                  labels: false,
                  ticks: false,
                },
              },
              x: {
                field: "x",
                type: "quantitative",
                scale: { domain: [0.0, 1.0] },
                axis: {
                  title: "Kiireen määrä",
                  labels: false,
                  ticks: false,
                },
                sort: { field: "oldness" },
              },
              size: {
                field: "oldness",
                type: "quantitative",
                scale: { range: [10, 1] },
                legend: null,
              },
              color: {
                condition: {
                  test: {
                    and: [
                      { not: { param: "toggle" } },
                      { param: "user-selection" },
                    ],
                  },
                  field: "user",
                  type: "nominal",
                },
                value: "#4682b4",
                legend: null,
              },
              opacity: {
                condition: {
                  test: {
                    and: [{ param: "trails" }, { param: "user-selection" }],
                  },
                  value: 0.7,
                },
                value: 0,
              },
            },
          },

          {
            transform: [{ filter: { param: "brush" } }],
            mark: {
              type: "circle",
            },

            encoding: {
              y: {
                field: "y",
                type: "quantitative",
                scale: { domain: [0.0, 1.0] },
              },
              x: {
                field: "x",
                type: "quantitative",
                scale: { domain: [0.0, 1.0] },
              },
              size: { condition: { param: "trails", value: 1 }, value: 100 },
              color: {
                condition: {
                  test: {
                    and: [
                      { not: { param: "toggle" } },
                      { param: "user-selection" },
                    ],
                  },
                  field: "user",
                  type: "nominal",
                },
                value: "#4682b4",
              },
              opacity: {
                condition: {
                  param: "user-selection",
                  value: 1,
                },
                value: 0.1,
              },
              tooltip: [
                { field: "user", type: "nominal", title: "Käyttäjä" },
                {
                  field: "x",
                  type: "quantitative",
                  title: "Kiireen määrä",
                  format: ".0%",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "Kiireen tuntu",
                  format: ".0%",
                },
                {
                  field: "date",
                  type: "temporal",
                  title: "Päiväys",
                  formatType: "time",
                  format: "%-d.%-m.%Y %H:%M",
                },
              ],
            },
          },

          {
            transform: [
              {
                filter: {
                  and: [{ param: "brush" }, { not: { param: "toggle" } }],
                },
              },
            ],
            mark: {
              type: "point",
              shape: "M-1,-1L1,1M-1,1L1-1",
            },
            encoding: {
              y: {
                aggregate: "mean",
                field: "y",
                type: "quantitative",
              },
              x: {
                aggregate: "mean",
                field: "x",
                type: "quantitative",
              },
              color: {
                field: "user",
                type: "nominal",
                legend: {
                  title: "Henkilö",
                  orient: "top-right",
                },
              },
              size: { value: 250 },
              opacity: {
                condition: {
                  param: "user-selection",
                  value: 1,
                },
                value: 0.1,
              },
              tooltip: [
                { field: "user", type: "nominal", title: "Käyttäjä" },
                {
                  field: "x",
                  type: "quantitative",
                  title: "Kiireen tuntu (avg)",
                  format: ".0%",
                  aggregate: "mean",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "Kiireen määrä (avg)",
                  format: ".0%",
                  aggregate: "mean",
                },
              ],
            },
          },

          {
            transform: [
              {
                filter: {
                  and: [{ param: "brush" }, { param: "user-selection" }],
                },
              },
            ],
            mark: {
              type: "rule",
              strokeDash: [5, 5],
              color: "grey",
              strokeWidth: 1,
            },
            encoding: {
              y: {
                aggregate: "mean",
                field: "y",
                type: "quantitative",
              },

              opacity: { value: 0.3 },
            },
          },

          {
            transform: [
              {
                filter: {
                  and: [{ param: "brush" }, { param: "user-selection" }],
                },
              },
            ],
            mark: {
              type: "rule",
              strokeDash: [5, 5],
              color: "grey",
              strokeWidth: 1,
            },
            encoding: {
              x: {
                aggregate: "mean",
                field: "x",
                type: "quantitative",
              },

              opacity: { value: 0.3 },
            },
          },
        ],
      },

      {
        height: 100,
        encoding: {
          y: {
            field: "y",
            type: "quantitative",
            scale: { domain: [0.0, 1.0] },
            axis: {
              title: "Kiireen tuntu",
              labels: false,
              ticks: false,
            },
          },
        },

        layer: [
          {
            mark: {
              type: "circle",
              color: "#e0e0e0",
              stroke: null,
              opacity: 1,
            },
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              x: { field: "date", type: "temporal" },
              color: {
                condition: {
                  test: { param: "brush", empty: false },
                  value: "#4682b4",
                },
                value: "#e0e0e0",
              },
              tooltip: null,
            },
            params: [
              {
                name: "brush",
                select: { type: "interval", encodings: ["x"] },
              },
            ],
          },

          {
            mark: {
              type: "errorband",
              extent: "ci",
              borders: true,
              borders: {
                opacity: 0.5,
                strokeDash: [6, 4],
              },
            },
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              x: {
                field: "date",
                timeUnit: "yearmonthdate",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%-d.%-m.'), timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%B') : timeFormat(datum.value, '%u') == '1' ? 'vko ' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 30,
                  tickCount: "day",
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
              tooltip: null,
            },
          },

          {
            mark: "line",
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              y: { field: "y", aggregate: "mean" },
              x: { field: "date", timeUnit: "yearmonthdate" },
            },
          },

          {
            mark: "circle",
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              y: { field: "y", aggregate: "mean" },
              x: { field: "date", timeUnit: "yearmonthdate" },
              tooltip: [
                {
                  field: "y",
                  type: "quantitative",
                  title: "Kiireen tuntu (avg)",
                  format: ".0%",
                  aggregate: "mean",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "Havaintojen lkm",
                  aggregate: "count",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "95% LV alaraja",
                  format: ".0%",
                  aggregate: "ci0",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "95% LV yläraja",
                  format: ".0%",
                  aggregate: "ci1",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "Minimi",
                  format: ".0%",
                  aggregate: "min",
                },
                {
                  field: "y",
                  type: "quantitative",
                  title: "Maksimi",
                  format: ".0%",
                  aggregate: "max",
                },
                {
                  field: "user",
                  type: "nominal",
                  title: "Korkein merkintä",
                  aggregate: { argmax: "y" },
                },
              ],
            },
          },
        ],
      },

      {
        height: 100,
        encoding: {
          y: {
            field: "x",
            type: "quantitative",
            scale: { domain: [0.0, 1.0] },
            axis: {
              title: "Kiireen määrä",
              labels: false,
              ticks: false,
            },
          },
        },

        layer: [
          {
            mark: {
              type: "circle",
              color: "#e0e0e0",
              stroke: null,
              opacity: 1,
            },
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              x: { field: "date", type: "temporal" },
              color: {
                condition: {
                  test: { param: "brush", empty: false },
                  value: "#4682b4",
                },
                value: "#e0e0e0",
              },
              tooltip: null,
            },
            params: [
              {
                name: "brush",
                select: { type: "interval", encodings: ["x"] },
              },
            ],
          },

          {
            mark: {
              type: "errorband",
              extent: "ci",
              borders: true,
              borders: {
                opacity: 0.5,
                strokeDash: [6, 4],
              },
            },
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              x: {
                field: "date",
                timeUnit: "yearmonthdate",
                axis: {
                  title: null,
                  grid: true,
                  labelAlign: "left",
                  labelExpr:
                    "[timeFormat(datum.value, '%-d.%-m.'), timeFormat(datum.value, '%d') == '01' ? timeFormat(datum.value, '%B') : timeFormat(datum.value, '%u') == '1' ? 'vko ' + timeFormat(datum.value, '%V') : '']",
                  tickSize: 30,
                  tickCount: "day",
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
              tooltip: null,
            },
          },

          {
            mark: "line",
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              y: { field: "x", aggregate: "mean" },
              x: { field: "date", timeUnit: "yearmonthdate" },
            },
          },

          {
            mark: "circle",
            transform: [{ filter: { param: "user-selection" } }],
            encoding: {
              y: { field: "x", aggregate: "mean" },
              x: { field: "date", timeUnit: "yearmonthdate" },
              tooltip: [
                {
                  field: "x",
                  type: "quantitative",
                  title: "Kiireen määrä (avg)",
                  format: ".0%",
                  aggregate: "mean",
                },
                {
                  field: "x",
                  type: "quantitative",
                  title: "Havaintojen lkm",
                  aggregate: "count",
                },
                {
                  field: "x",
                  type: "quantitative",
                  title: "95% LV alaraja",
                  format: ".0%",
                  aggregate: "ci0",
                },
                {
                  field: "x",
                  type: "quantitative",
                  title: "95% LV yläraja",
                  format: ".0%",
                  aggregate: "ci1",
                },
                {
                  field: "x",
                  type: "quantitative",
                  title: "Minimi",
                  format: ".0%",
                  aggregate: "min",
                },
                {
                  field: "x",
                  type: "quantitative",
                  title: "Maksimi",
                  format: ".0%",
                  aggregate: "max",
                },
                {
                  field: "user",
                  type: "nominal",
                  title: "Korkein merkintä",
                  aggregate: { argmax: "x" },
                },
              ],
            },
          },
        ],
      },
    ],
    config: {
      view: { fill: "white", cornerRadius: 7, width: 700 },
      title: {
        fontWeight: "normal",
        fontSize: { expr: "fontSize + 6" },
        font: "Lato, sans-serif",
      },
      axis: {
        grid: false,
        titleFontWeight: "normal",
        titleFontSize: { expr: "fontSize" },
        titleFont: "Lato, sans-serif",
      },
      header: {
        titleFontSize: { expr: "fontSize" },
        labelFontSize: { expr: "fontSize" },
        titleFont: "Lato, sans-serif",
        labelFont: "Lato, sans-serif",
      },
    },
  };

  // Embed the visualization in the container with id `vis`
  vegaEmbed("#vis", vlSpec, embedOpt);
}

async function refresh_graphics(event) {
  const showTotalMean = document.getElementById("show-total-mean").checked;
  const showUserMeans = document.getElementById("show-user-means").checked;
  const coloringOff = document.getElementById("off").checked;
  const coloringByUser = document.getElementById("user").checked;
  // const coloringByUnit = document.getElementById("businessunit").checked;

  // const colors = ["#e60049", "#0bb4ff", "#50e991", "#e6d800", "#9b19f5", "#ffa300", "#dc0ab4", "#b3d4ff", "#00bfa0"]; // Dutch Field
  // const colors = ["#ea5545", "#f46a9b", "#ef9b20", "#edbf33", "#ede15b", "#bdcf32", "#87bc45", "#27aeef", "#b33dc6"]: // Retro Metro
  const colors = [
    "#fd7f6f",
    "#7eb0d5",
    "#b2e061",
    "#bd7ebe",
    "#ffb55a",
    "#ffee65",
    "#beb9db",
    "#fdcce5",
    "#8bd3c7",
  ]; // Spring pastels

  reset_canvas();

  const c = document.getElementById("area");
  const w = c.offsetWidth;
  const h = c.offsetHeight;
  const ctx = reset_canvas();

  if (global_data !== null) {
    ctx.lineCap = "round";
    ctx.strokeStyle = "rgba(0, 0, 0, 100%)";
    ctx.fillStyle = "rgba(0, 0, 0, 100%)";
    ctx.lineWidth = 2;

    let mean_x = 0;
    let mean_y = 0;

    const coloursByUser = new Map();
    let i = 0;

    global_data.forEach((element) => {
      if (!coloursByUser.has(element.user)) {
        coloursByUser.set(element.user, colors[i++]);
      }
    });

    const means = new Map();

    global_data.forEach((element) => {
      dot(
        ctx,
        element.x * w,
        (1 - element.y) * h,
        5,
        coloringByUser ? coloursByUser.get(element.user) : colors[0]
      );

      mean_x += element.x;
      mean_y += element.y;

      user_mean = { x: 0.0, y: 0.0, n: 0 };
      if (means.has(element.user)) {
        user_mean = means.get(element.user);
      }

      means.set(element.user, {
        x: user_mean.x + element.x,
        y: user_mean.y + element.y,
        n: user_mean.n + 1,
      });
    });

    mean_x /= global_data.length;
    mean_y /= global_data.length;

    if (showUserMeans) {
      means.forEach((mean, name) => {
        const mx = (mean.x / mean.n) * w;
        const my = (1 - mean.y / mean.n) * h;
        ctx.strokeStyle = "rgba(0, 0, 0, 100%)";
        ctx.lineWidth = 3;
        dot(
          ctx,
          mx,
          my,
          7,
          coloringByUser ? coloursByUser.get(name) : colors[0]
        );
        ctx.font = "12px Lato, sans-serif";
        ctx.fillStyle = "black";
        ctx.fillText(name, mx + 20, my + 5);
      });
    }

    if (showTotalMean) {
      mx = mean_x * w;
      my = (1 - mean_y) * h;

      ctx.strokeStyle = "rgba(0, 0, 0, 100%)";
      ctx.lineWidth = 3;
      dot(ctx, mx, my, 10, colors[colors.length - 1]);

      ctx.font = "18px Lato, sans-serif";
      ctx.fillStyle = "black";
      ctx.fillText("keskiarvo", mx + 20, my + 5);
    }
  }
}

async function fetch_pressure_data(
  hostname,
  startDate,
  endDate,
  userFilter,
  unitFilter
) {
  // const url = `${hostname}pressure/?startDate=${startDate}&endDate=${endDate}&users=${userFilter}&businessunits=${unitFilter}`;

  const url = `/kiire/pressure.json?startDate=${startDate}&endDate=${endDate}&users=${userFilter}`;

  const response = await fetch(url);

  const jsonData = await response.json();

  return jsonData;
}
