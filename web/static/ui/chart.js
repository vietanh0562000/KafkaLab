import { useEffect, useRef, useState } from "preact/hooks";
import { html } from "./shell.js";

const H = 190, PAD = { l: 48, r: 12, t: 10, b: 24 };
const fmt = (n) => (n >= 1000 ? `${(n / 1000).toFixed(n >= 10000 ? 0 : 1)}k` : String(Math.round(n * 10) / 10));

function niceMax(v) {
  if (v <= 1) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  const m = v / p;
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
}

// LineChart draws one measure; a null point or null value breaks the line rather than inventing data.
export function LineChart({ title, unit, points, field }) {
  const ref = useRef(null);
  const [w, setW] = useState(560);
  const [hover, setHover] = useState(null);
  useEffect(() => {
    if (!ref.current) return undefined;
    const ro = new ResizeObserver(([e]) => setW(Math.max(240, e.contentRect.width)));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  const valid = points.filter((p) => p && p[field] !== null && p[field] !== undefined);
  const stamped = points.filter(Boolean);
  const latest = valid.length ? valid[valid.length - 1][field] : null;
  const x1 = stamped.length ? stamped[stamped.length - 1].at : Date.now();
  const x0 = Math.min(stamped.length ? stamped[0].at : x1, x1 - 60000);
  const yMax = niceMax(Math.max(1, ...valid.map((p) => p[field])));
  const X = (t) => PAD.l + ((t - x0) / (x1 - x0)) * (w - PAD.l - PAD.r);
  const Y = (v) => H - PAD.b - (v / yMax) * (H - PAD.t - PAD.b);

  let d = "", pen = false;
  for (const p of points) {
    if (!p || p[field] === null || p[field] === undefined) { pen = false; continue; }
    d += `${pen ? "L" : "M"}${X(p.at).toFixed(1)} ${Y(p[field]).toFixed(1)}`;
    pen = true;
  }
  const ticks = [0, 0.5, 1].map((f) => f * yMax);
  const span = (x1 - x0) / 60000;
  const xTicks = [0, 0.5, 1].map((f) => ({ at: x0 + f * (x1 - x0), label: f === 1 ? "bây giờ" : `-${(span * (1 - f)).toFixed(span < 3 ? 1 : 0)}m` }));

  const move = (e) => {
    if (!valid.length) return;
    const box = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - box.left;
    let best = valid[0];
    for (const p of valid) if (Math.abs(X(p.at) - px) < Math.abs(X(best.at) - px)) best = p;
    setHover(best);
  };
  const tipLeft = hover ? Math.min(X(hover.at) + 8, w - 150) : 0;
  return html`
    <div class="panel stack chart">
      <div class="between"><span class="caps">${title}</span>
        <span class="mono">${latest === null ? "?" : `${fmt(latest)} ${unit}`}</span></div>
      <div class="chart-box" ref=${ref}>
        <svg width=${w} height=${H} role="img" aria-label=${`${title}: ${latest === null ? "chưa có dữ liệu" : `${fmt(latest)} ${unit}`}`}
          onMouseMove=${move} onMouseLeave=${() => setHover(null)}>
          ${ticks.map((t) => html`<g key=${t}>
            <line class="grid-line" x1=${PAD.l} x2=${w - PAD.r} y1=${Y(t)} y2=${Y(t)} />
            <text class="axis" x=${PAD.l - 6} y=${Y(t) + 4} text-anchor="end">${fmt(t)}</text></g>`)}
          ${xTicks.map((t, i) => html`<text key=${i} class="axis" x=${X(t.at)} y=${H - 7} text-anchor=${i === 0 ? "start" : i === 2 ? "end" : "middle"}>${t.label}</text>`)}
          ${d && html`<path class="series" d=${d} />`}
          ${hover && html`<g><line class="cross" x1=${X(hover.at)} x2=${X(hover.at)} y1=${PAD.t} y2=${H - PAD.b} />
            <rect class="hover-dot" x=${X(hover.at) - 4} y=${Y(hover[field]) - 4} width="8" height="8" /></g>`}
        </svg>
        ${!valid.length && html`<div class="chart-empty muted small">Chưa có dữ liệu cho topic này</div>`}
        ${hover && html`<div class="chart-tip" style=${`left:${tipLeft}px`}>
          <b>${fmt(hover[field])} ${unit}</b><div class="muted">${new Date(hover.at).toLocaleTimeString("vi-VN")}</div></div>`}
      </div>
    </div>`;
}
