import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
import { html } from "./shell.js";
import { edgeColor, isDead, lagOf, partitionHealth } from "./logic/health.js";
import { layoutFlow, serviceView } from "./logic/layout.js";

// FNV-1a spread over the golden angle, so similar keys like xe-1 and xe-4 get distinct colours.
function hue(key) {
  let h = 2166136261;
  for (const c of key) h = Math.imul(h ^ c.codePointAt(0), 16777619);
  return Math.round(((h >>> 0) * 137.508) % 360);
}

const is = (sel, kind, id) => Boolean(sel && sel.kind === kind && String(sel.id) === String(id));
const num = (n) => (n === null || n === undefined ? "?" : Number(n).toLocaleString("en-US"));

function replicaClass(r, p, byId) {
  const inIsr = p.isr.includes(r), leader = r === p.leader, dead = isDead(byId.get(r));
  if (dead) return leader ? "replica leader dead-leader" : inIsr ? "replica dead-isr" : "replica dead";
  return leader ? "replica leader" : inIsr ? "replica" : "replica out";
}

function replicaTitle(r, p, byId) {
  const inIsr = p.isr.includes(r), leader = r === p.leader;
  const state = isDead(byId.get(r))
    ? leader ? "đã tắt nhưng vẫn là leader: controller chưa chọn leader mới"
      : inIsr ? "đã tắt nhưng vẫn trong ISR: acks=all phải đợi nó" : "đã tắt, đã bị loại khỏi ISR"
    : leader ? "leader" : inIsr ? "trong ISR" : "ngoài ISR (đang đuổi theo leader)";
  return `broker ${r} · ${state}`;
}

function Lane({ p, view, health, selection, onSelect }) {
  const byId = new Map((view.brokers || []).map((b) => [b.id, b]));
  const recs = p.records || [];
  const logRef = useRef(null);
  const lastOffset = recs.length ? recs[recs.length - 1].offset : -1;
  useEffect(() => { if (logRef.current) logRef.current.scrollLeft = logRef.current.scrollWidth; }, [lastOffset, p.end]);

  const known = p.start >= 0 && p.end >= 0;
  const first = recs.length ? recs[0].offset : p.end;
  const hidden = known && recs.length ? first - p.start : 0;
  const slots = [];
  if (hidden > 0) slots.push({ kind: "more", at: first - 1 });
  for (const r of recs) slots.push({ kind: "rec", at: r.offset, r });
  if (known) slots.push({ kind: "next", at: p.end });

  // Committed offsets and positions point at the next record to read; pin each marker to that slot.
  const marks = {};
  const pin = (off, m) => {
    let at = off;
    if (hidden > 0 && off < first) at = first - 1;
    if (known && off > p.end) at = p.end;
    (marks[at] = marks[at] || []).push(m);
  };
  for (const g of view.groups || []) {
    const c = g.committed && g.committed[p.partition];
    if (c !== undefined) pin(c, { cls: "commit", text: g.name, title: `${g.name} đã commit: đọc tiếp từ offset ${c}` });
  }
  for (const c of view.consumers || []) {
    const pos = c.positions && c.positions[p.partition];
    if (pos !== undefined && c.assigned && c.assigned.includes(p.partition)) {
      pin(pos, { cls: "pos", text: c.name, title: `${c.name} đã poll tới đây (offset ${pos}), chưa chắc đã commit` });
    }
  }
  const sel = is(selection, "partition", p.partition);
  const meta = p.end < 0 ? "offset: ?" : p.start >= 0 ? `start ${p.start} · end ${p.end}` : `end ${num(p.end)}`;
  return html`
    <div class=${"lane" + (sel ? " sel" : "") + (health !== "ok" ? " " + health : "")} data-node=${"partition:" + p.partition}>
      <button class="lane-head" onClick=${() => onSelect({ kind: "partition", id: p.partition })}>
        <span class="lane-name">P${p.partition}</span>
        ${p.leader < 0 && html`<span class="tag err">không có leader</span>`}
        ${p.replicas.map((r) => html`<span key=${r} class=${replicaClass(r, p, byId)} title=${replicaTitle(r, p, byId)}>${r}</span>`)}
        <span class="lane-meta">${meta}</span>
      </button>
      ${view.mode !== "service" && html`
      <div class="log" ref=${logRef}>
        ${slots.map((s) => html`
          <div class="slot" key=${s.kind + s.at}>
            ${s.kind === "more" && html`<div class="cell more" title=${`${hidden} record trước`}><span>…${hidden}</span></div>`}
            ${s.kind === "next" && html`<div class="cell next" title=${`record kế tiếp ghi vào partition này sẽ có offset ${p.end}`}><span>kế tiếp</span><span>${p.end}</span></div>`}
            ${s.kind === "rec" && html`
              <button class=${"cell" + (s.r.key ? " keyed" : "") + (is(selection, "record", `${p.partition}:${s.r.offset}`) ? " sel" : "")}
                style=${s.r.key ? `--h:${hue(s.r.key)}` : ""} title=${`offset ${s.r.offset} · key ${s.r.key || "(không có)"}`}
                onClick=${() => onSelect({ kind: "record", id: `${p.partition}:${s.r.offset}` })}>
                <b>${s.r.offset}</b><span>${s.r.key || "∅"}</span>
              </button>`}
            <div class="marks">${(marks[s.at] || []).map((m, i) => html`<span key=${i} class=${"mark " + m.cls} title=${m.title}>${m.text}</span>`)}</div>
          </div>`)}
      </div>`}
    </div>`;
}

function BrokerRow({ view, selection, onSelect }) {
  return html`
    <div class="caps">Broker</div>
    <div class="broker-row">
      ${(view.brokers || []).map((b) => {
        const up = b.state === "running", unknown = !b.state;
        const limbo = !up && !unknown && b.registered && view.mode !== "service";
        const cls = up || unknown ? "" : limbo ? " limbo" : " down";
        const dot = unknown ? "" : up ? "ok" : limbo ? "warn" : "err";
        const label = unknown ? "trạng thái chưa rõ" : up ? (b.registered ? "đang chạy" : "đang khởi động") : limbo ? "đã tắt, controller vẫn tưởng còn sống" : `đã tắt (${b.state})`;
        return html`<button key=${b.id} title=${label} class=${"broker" + cls + (is(selection, "broker", b.id) ? " sel" : "")}
          onClick=${() => onSelect({ kind: "broker", id: b.id })}><span class=${"dot " + dot}></span>B${b.id}</button>`;
      })}
    </div>`;
}

function Paths({ edges, paths, colors }) {
  return html`
    <svg class="edges" aria-hidden="true">
      <defs><marker id="flow-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M0 0 8 4 0 8" fill="none" stroke="context-stroke" stroke-width="1.2" /></marker></defs>
      ${edges.map((e) => paths[e.id] && html`<path key=${e.id} class=${"edge " + (colors[e.id] || "idle")} d=${paths[e.id]} />`)}
    </svg>`;
}

function curve(a, b, box) {
  const stacked = b.top >= a.bottom - 1 && b.left < a.right && b.right > a.left;
  if (stacked) {
    const x1 = a.left + a.width / 2 - box.left, y1 = a.bottom - box.top;
    const x2 = b.left + b.width / 2 - box.left, y2 = b.top - box.top;
    const m = (y1 + y2) / 2;
    return `M${x1.toFixed(1)} ${y1.toFixed(1)} C${x1.toFixed(1)} ${m.toFixed(1)} ${x2.toFixed(1)} ${m.toFixed(1)} ${x2.toFixed(1)} ${y2.toFixed(1)}`;
  }
  const x1 = a.right - box.left, y1 = a.top + a.height / 2 - box.top;
  const x2 = b.left - box.left, y2 = b.top + b.height / 2 - box.top;
  const m = (x1 + x2) / 2;
  return `M${x1.toFixed(1)} ${y1.toFixed(1)} C${m.toFixed(1)} ${y1.toFixed(1)} ${m.toFixed(1)} ${y2.toFixed(1)} ${x2.toFixed(1)} ${y2.toFixed(1)}`;
}

function useEdgePaths(ref, edges, deps) {
  const [paths, setPaths] = useState({});
  const measure = () => {
    const root = ref.current;
    if (!root) return;
    const box = root.getBoundingClientRect();
    const rect = (id) => { const el = root.querySelector(`[data-node="${CSS.escape(id)}"]`); return el && el.getBoundingClientRect(); };
    const next = {};
    for (const e of edges) {
      const a = rect(e.from), b = rect(e.to);
      if (a && b) next[e.id] = curve(a, b, box);
    }
    setPaths((cur) => (JSON.stringify(cur) === JSON.stringify(next) ? cur : next));
  };
  useLayoutEffect(measure, deps);
  useEffect(() => {
    if (!ref.current || typeof ResizeObserver === "undefined") return undefined;
    const ro = new ResizeObserver(() => measure());
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, [edges.map((e) => e.id).join("|")]);
  return paths;
}

function useTokens(track, consumers, enabled) {
  const [flying, setFlying] = useState([]);
  const [extra, setExtra] = useState(0);
  useEffect(() => {
    if (!enabled || !track.tokens.length) { if (enabled) setExtra(track.overflow || 0); return undefined; }
    const groupOf = new Map((consumers || []).map((c) => [c.name, c.group]));
    const batch = track.tokens.map((t, i) => ({
      id: `${track.seq}-${i}`, kind: t.kind, delay: i * 50,
      edge: t.kind === "send" ? `producer:${t.from}>partition:${t.partition}` : `partition:${t.from}>group:${groupOf.get(t.to)}`,
    }));
    setFlying((f) => [...f, ...batch]);
    setExtra(track.overflow || 0);
    const ids = new Set(batch.map((b) => b.id));
    const timer = setTimeout(() => setFlying((f) => f.filter((x) => !ids.has(x.id))), 1500);
    return () => clearTimeout(timer);
  }, [track.seq]);
  return { flying, extra };
}

const worst = (colors) => (colors.includes("red") ? "red" : colors.includes("yellow") ? "yellow" : colors.includes("blue") ? "blue" : "idle");

export function playgroundView(st) {
  const topic = st && st.topic;
  const consumers = ((st && st.consumers) || []).filter((c) => topic && c.topic === topic.name);
  const assigned = {};
  for (const c of consumers) for (const p of c.assigned || []) (assigned[c.group] = assigned[c.group] || new Set()).add(p);
  const names = new Set([...((st && st.groups) || []).map((g) => g.name), ...consumers.map((c) => c.group)]);
  const groups = [...names].sort().map((name) => {
    const g = ((st && st.groups) || []).find((x) => x.name === name) || { name, state: "", members: 0, committed: {} };
    const parts = new Set([...Object.keys(g.committed || {}).map(Number), ...(assigned[name] || [])]);
    return { ...g, partitions: [...parts].sort((a, b) => a - b) };
  });
  const producers = ((st && st.producers) || []).map((p) => ({
    ...p,
    partitions: [...new Set((p.sends || [])
      .filter((s) => topic && s.topic === topic.name && !s.err && !s.pending && String(s.acks) !== "0").map((s) => s.partition))],
  }));
  return { mode: "playground", topic, brokers: (st && st.brokers) || [], groups, consumers, producers, errors: (st && st.errors) || {} };
}

function lastSendOf(producer, topic, partition) {
  const list = (producer.sends || []).filter((s) => s.topic === topic && !s.pending && (partition === undefined || s.err || s.partition === partition));
  const s = list[list.length - 1];
  return s ? { ok: !s.err, at: new Date(s.at).getTime() } : null;
}

function serviceEdge(id, view) {
  const up = view.instances.filter((i) => i.container === "running");
  if (id.endsWith(">dlq")) return view.dlq > 0 ? "yellow" : "idle";
  if (id.endsWith(">db") && view.errors.db) return "red";
  if (!up.length) return "idle";
  if (up.some((i) => i.state === "PAUSED")) return "red";
  if (up.some((i) => i.state === "RECOVERING")) return "yellow";
  return up.some((i) => i.state === "RUNNING") ? "blue" : "idle";
}

function useColors(view, layout, track) {
  const now = Date.now();
  const topic = view.topic;
  const healths = {};
  for (const p of (topic && topic.partitions) || []) {
    healths[p.partition] = partitionHealth({ partition: p, brokers: view.brokers, minIsr: topic.minIsr });
  }
  const colors = {};
  for (const e of layout.edges) {
    if (e.from === "service" || e.to === "service") { colors[e.id] = serviceEdge(e.id, view); continue; }
    if (e.from.startsWith("producer:")) {
      const name = e.from.slice("producer:".length);
      const prod = view.producers.find((p) => p.name === name);
      if (e.to === "cluster") {
        const last = prod ? lastSendOf(prod, topic.name) : null;
        colors[e.id] = edgeColor({ partitionHealth: "ok", lastSend: last, lagSince: null, members: 0, lastChangeAt: null, now });
        continue;
      }
      colors[e.id] = edgeColor({ partitionHealth: healths[e.partition], lastSend: prod ? lastSendOf(prod, topic.name, e.partition) : null,
        lagSince: null, members: 0, lastChangeAt: track.changeAt[`${topic.name}:${e.partition}`] ?? null, now });
      continue;
    }
    const groupName = e.to.slice("group:".length);
    const g = view.groups.find((x) => x.name === groupName);
    colors[e.id] = edgeColor({ partitionHealth: healths[e.partition], lastSend: null,
      lagSince: track.lagSince[`${topic.name}:${groupName}:${e.partition}`] ?? null, members: g ? g.members : 0,
      lastChangeAt: track.changeAt[`${topic.name}:${e.partition}`] ?? null, now });
  }
  return { colors, healths };
}

function GroupNode({ g, view, colors, selection, onSelect }) {
  const parts = (view.topic && view.topic.partitions) || [];
  let lag = null, unknown = false;
  for (const p of parts) {
    const l = lagOf(p.end, g.committed && g.committed[p.partition]);
    if (l === null) unknown = true; else lag = (lag || 0) + l;
  }
  const c = worst(layoutColors(colors, `group:${g.name}`));
  const members = (view.consumers || []).filter((x) => x.group === g.name);
  return html`
    <div class=${"node" + (is(selection, "group", g.name) ? " sel" : "") + (c === "red" || c === "yellow" ? " " + c : "")} data-node=${"group:" + g.name}
      role="button" tabindex="0" onClick=${() => onSelect({ kind: "group", id: g.name })}
      onKeyDown=${(e) => { if (e.key === "Enter") onSelect({ kind: "group", id: g.name }); }}>
      <div class="node-title" title=${g.name}>${g.name}</div>
      <div class="node-sub">${g.state || "?"} · ${g.members} member</div>
      <div class="node-sub">lag ${lag === null ? "?" : `${unknown ? "≥ " : ""}${num(lag)}`}</div>
      ${members.length > 0 && html`<div class="member-row">${members.map((m) => html`
        <button key=${m.name} class=${"member" + (is(selection, "consumer", m.name) ? " sel" : "")} title=${`consumer ${m.name}: partition ${(m.assigned || []).join(", ") || "chưa có"}`}
          onClick=${(e) => { e.stopPropagation(); onSelect({ kind: "consumer", id: m.name }); }}>${m.name}</button>`)}</div>`}
    </div>`;
}

const layoutColors = (colors, node) => Object.entries(colors).filter(([id]) => id.endsWith(">" + node)).map(([, c]) => c);

function ServiceColumns({ view, colors }) {
  const up = view.instances.filter((i) => i.container === "running");
  const state = !view.instances.length ? "chưa chạy" : up.length ? [...new Set(up.map((i) => i.state || "?"))].join(", ") : "đã tắt";
  const c = colors["service>db"];
  return html`
    <div class="flow-col">
      <span class="col-label">Service</span>
      <div class=${"node static" + (c === "red" || c === "yellow" ? " " + c : "")} data-node="service">
        <div class="node-title">gap-traffic</div>
        <div class="node-sub">${up.length}/${view.instances.length} instance</div>
        <div class="node-sub" title=${state}>${state}</div>
      </div>
    </div>
    <div class="flow-col sink-col">
      <span class="col-label">Đích</span>
      <div class=${"node static" + (view.errors.db ? " red" : "")} data-node="db">
        <div class="node-sub">TimescaleDB</div><div class="node-metric">${num(view.dbRows)}</div><div class="node-sub">rows</div>
      </div>
      <div class=${"node static" + (view.dlq > 0 ? " yellow" : "")} data-node="dlq">
        <div class="node-sub">DLQ</div><div class="node-metric">${num(view.dlq)}</div><div class="node-sub">records</div>
      </div>
    </div>`;
}

export function FlowMap({ lab, mode = "playground", selection, onSelect, empty }) {
  const view = useMemo(() => (mode === "service"
    ? { ...serviceView(lab.snapshot), mode: "service", consumers: [], producers: [] }
    : playgroundView(lab.st)), [mode, mode === "service" ? lab.snapshot : lab.st]);
  const topic = view.topic;
  const layout = useMemo(() => layoutFlow({
    producers: view.producers, partitions: topic && topic.partitions, groups: view.groups,
    service: mode === "service" ? {} : undefined, dlq: view.dlq,
  }), [view]);
  const ref = useRef(null);
  const paths = useEdgePaths(ref, layout.edges, [layout, selection]);
  const { colors, healths } = useColors(view, layout, lab.track);
  const { flying, extra } = useTokens(mode === "service" ? { tokens: [], seq: 0, overflow: 0 } : lab.track, view.consumers, !lab.frozen);

  if (!topic) {
    return html`<div class="flow-wrap"><div class="stack" style="align-items:flex-start;position:relative;z-index:3;background:var(--canvas);padding:8px">
      ${empty || html`<span class="muted">Chưa có topic để vẽ flow.</span>`}</div></div>`;
  }
  return html`
    <div class="flow-wrap">
      <div class="flow-head">
        <span class="tag blue">${mode === "service" ? "SERVICE FLOW" : "DATA FLOW"}</span>
        <span class="mono" style="overflow-wrap:anywhere">${topic.name} · ${topic.partitions.length} partition · RF ${topic.rf || "?"}${topic.minIsr > 0 ? ` · min ISR ${topic.minIsr}` : ""}</span>
        ${extra > 0 && html`<span class="tag">+${num(extra)} record</span>`}
      </div>
      <div class=${"flow " + mode} ref=${ref}>
        <${Paths} edges=${layout.edges} paths=${paths} colors=${colors} />
        ${flying.map((t) => paths[t.edge] && html`<div key=${t.id} class=${"token " + t.kind}
          style=${`offset-path:path('${paths[t.edge]}');animation-delay:${t.delay}ms`} />`)}
        ${mode === "playground" && html`
          <div class="flow-col clients">
            <span class="col-label">Producer</span>
            ${view.producers.map((p) => html`
              <button key=${p.name} class=${"node" + (is(selection, "producer", p.name) ? " sel" : "")} data-node=${"producer:" + p.name}
                onClick=${() => onSelect({ kind: "producer", id: p.name })}>
                <div class="node-title" title=${p.name}>${p.name}</div>
                <div class="node-sub">${(p.javaProps || []).find((x) => x.startsWith("acks=")) || ""}</div>
                <div class="node-sub">${(p.sends || []).length} lần gửi gần đây</div>
              </button>`)}
            ${view.producers.length === 0 && html`<span class="muted small" style="background:var(--canvas)">Chưa có producer</span>`}
          </div>`}
        <div class="flow-col">
          <div class="cluster" data-node="cluster">
            <div class="caps" style="margin-bottom:4px">Kafka cluster</div>
            <${BrokerRow} view=${view} selection=${selection} onSelect=${onSelect} />
            ${topic.partitions.map((p) => html`<${Lane} key=${p.partition} p=${p} view=${view} health=${healths[p.partition]} selection=${selection} onSelect=${onSelect} />`)}
          </div>
        </div>
        <div class="flow-col clients">
          <span class="col-label">Consumer group</span>
          ${view.groups.map((g) => html`<${GroupNode} key=${g.name} g=${g} view=${view} colors=${colors} selection=${selection} onSelect=${onSelect} />`)}
          ${view.groups.length === 0 && html`<span class="muted small" style="background:var(--canvas)">Chưa có group</span>`}
        </div>
        ${mode === "service" && html`<${ServiceColumns} view=${view} colors=${colors} />`}
      </div>
      <div class="flow-legend">
        <span><i class="swatch"></i>có dữ liệu chảy (5 giây)</span>
        <span><i class="swatch idle"></i>chưa có luồng</span>
        <span><i class="swatch yellow"></i>suy giảm hoặc lag kéo dài</span>
        <span><i class="swatch red"></i>đang hỏng</span>
        ${mode === "playground" && html`<span><span class="mark commit">g1</span>đã commit</span><span><span class="mark pos">c1</span>vị trí đọc</span>`}
        <span>${mode === "service" ? "Số tổng từ service, không theo từng record" : "Chuyển động trễ tối đa một lần đọc (0,7 giây)"}</span>
      </div>
    </div>`;
}
