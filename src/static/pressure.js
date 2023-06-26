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
    `https://${hostname}/pressure/save/${user_name}?x=${scaled_x}&y=${scaled_y}`
  );
  const jsonData = await response.json();

  console.log(jsonData);
}

async function fetch_pressure(hostname, offset) {
    const c = document.getElementById("area");
  const w = c.offsetWidth;
  const h = c.offsetHeight;
    const ctx = reset_canvas();

  const response = await fetch(`https://${hostname}/pressure/?offset=${offset}`);
  const jsonData = await response.json();

  ctx.lineCap = "round";
  ctx.strokeStyle = "rgba(0, 0, 0, 50%)";
  ctx.lineWidth = 2;

  mean_x = 0;
  mean_y = 0;

  jsonData.forEach(element => {
    cross(ctx, element.x * w, (1-element.y) * h, 5);
    mean_x += element.x;
    mean_y += element.y;
  });

  mean_x /= jsonData.length;
  mean_y /= jsonData.length;

  mx = mean_x * w;
  my = (1-mean_y) * h;

  ctx.strokeStyle = "rgba(0, 0, 0, 100%)";
  ctx.lineWidth = 3;
  cross(ctx, mx, my, 10);

  ctx.font = "18px Lato, sans-serif";
  ctx.fillStyle = "black";
  ctx.fillText("keskiarvo", mx+20, my + 5)
}
