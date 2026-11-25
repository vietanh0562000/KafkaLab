import { useState } from "preact/hooks";
import { html, Cli } from "./shell.js";
import { isScenarioTopic } from "./store.js";
import { buildTrace } from "./logic/trace.js";
import { isDead, lagOf } from "./logic/health.js";

const ago = (iso) => (Date.now() - new Date(iso).getTime()) / 1000;
const secs = (ms) => (ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`);
const time = (iso) => new Date(iso).toLocaleTimeString("vi-VN");
const enc = encodeURIComponent;
const q = (v) => (v === null || v === undefined || v < 0 ? "?" : v);

export function TopicSelect({ topics, value, onChange }) {
  const own = topics.filter((t) => !isScenarioTopic(t));
  const scen = topics.filter(isScenarioTopic);
  return html`<select value=${value} onChange=${(e) => onChange(e.target.value)}>
    ${!value && html`<option value="">chọn topic</option>`}
    ${own.map((t) => html`<option key=${t} value=${t}>${t}</option>`)}
    ${scen.length > 0 && html`<optgroup label="của kịch bản">${scen.map((t) => html`<option key=${t} value=${t}>${t}</option>`)}</optgroup>`}
  </select>`;
}

const Kv = ({ k, children }) => html`<div class="kv"><span>${k}</span><b>${children}</b></div>`;
const Head = ({ eyebrow, title, children }) => html`
  <div class="between"><span class="caps">${eyebrow}</span>${children}</div>
  <div class="title insp-title" title=${title}>${title}</div>`;

function ProducerInspector({ p, lab }) {
  const topics = ((lab.st && lab.st.topics) || []).map((t) => t.name).filter((t) => !isScenarioTopic(t));
  const current = isScenarioTopic(lab.topic) ? topics[0] || "" : lab.topic;
  const [f, setF] = useState({ topic: "", key: "", value: "v{i}", count: 1 });
  const t = f.topic || current;
  const acks = (p.javaProps.find((x) => x.startsWith("acks=")) || "acks=all").slice(5);
  const send = (e) => {
    e.preventDefault();
    lab.act(`${p.name} gửi ${f.count} record vào ${t}${f.key ? ` (key ${f.key})` : " (không key)"}`, "POST",
      `/api/pg/producers/${enc(p.name)}/send`, { topic: t, key: f.key, value: f.value, count: f.count, acks });
  };
  const sends = [...(p.sends || [])].reverse().slice(0, 12);
  return html`
    <${Head} eyebrow="Producer" title=${p.name}>
      <button class="btn tiny danger" onClick=${() => lab.act(`xoá producer ${p.name}`, "DELETE", `/api/pg/producers/${enc(p.name)}`)}>xoá</button>
    <//>
    <div class="row">${p.javaProps.map((x) => html`<span key=${x} class="tag">${x}</span>`)}</div>
    <form class="form-grid" onSubmit=${send}>
      <label class="field wide">Topic<${TopicSelect} topics=${topics} value=${t} onChange=${(v) => setF({ ...f, topic: v })} /></label>
      <label class="field">Key<input placeholder="trống = không key" value=${f.key} onInput=${(e) => setF({ ...f, key: e.target.value })} /></label>
      <label class="field">Value<input value=${f.value} onInput=${(e) => setF({ ...f, value: e.target.value })} /></label>
      <label class="field">Số lượng<input type="number" min="1" max="1000" value=${f.count} onInput=${(e) => setF({ ...f, count: Number(e.target.value) })} /></label>
      <button class="btn primary end" disabled=${!t}>Gửi</button>
    </form>
    <div class="muted small">{i} trong key hoặc value được thay bằng 1, 2, 3… theo thứ tự record.</div>
    <div class="caps">Đã gửi</div>
    ${sends.length === 0 && html`<div class="muted small">Chưa gửi record nào.</div>`}
    <ul class="events">
      ${sends.map((s) => html`<li key=${s.id}>
        ${s.pending
          ? html`<span class="grow ellipsis">${s.key || "∅"} → ${s.topic}: đang chờ ack… ${ago(s.at).toFixed(1)} s</span>`
          : s.err
            ? html`<span class="grow err-text wrap">✗ ${s.key || "∅"}: ${s.err}</span><span class="muted">${secs(s.tookMs)}</span>`
            : html`<span class="grow ellipsis">${s.key || "∅"} → <b>P${s.partition}</b> @${String(s.acks) === "0" ? "?" : s.offset}</span>
                ${s.brokersDown > 0 && html`<span class="tag warn">${s.brokersDown} broker tắt</span>`}
                <span class="muted">${secs(s.tookMs)}</span>`}
      </li>`)}
    </ul>`;
}

function ConsumerInspector({ c, lab }) {
  const [busy, setBusy] = useState(false);
  const path = `/api/pg/consumers/${enc(c.name)}`;
  const poll = async () => {
    setBusy(true);
    await lab.act(`${c.name} poll`, "POST", `${path}/poll`, { max: 10 }, (res) => {
      const recs = res.data || [];
      if (!recs.length) return `${c.name} poll: không nhận được record nào sau 3 s`;
      const again = recs.filter((x) => x.redelivered).length;
      return `${c.name} poll: nhận ${recs.length} record${again ? `, trong đó ${again} record đọc lại` : ""}`;
    });
    setBusy(false);
  };
  const recv = [...(c.received || [])].reverse().slice(0, 12);
  const positions = Object.entries(c.positions || {}).map(([p, o]) => `P${p}@${o}`).join(" ");
  return html`
    <${Head} eyebrow=${"Consumer · group " + c.group} title=${c.name}>
      <button class="btn tiny danger" onClick=${() => lab.act(`xoá consumer ${c.name} (rời group ${c.group})`, "DELETE", path)}>xoá</button>
    <//>
    <div class="row">
      ${c.assigned && c.assigned.length
        ? c.assigned.map((p) => html`<span key=${p} class="tag blue">P${p}</span>`)
        : html`<span class="tag warn">chưa có partition</span>`}
    </div>
    <div class="inset">
      <${Kv} k="Topic">${c.topic}<//>
      <${Kv} k="auto.offset.reset">${c.reset}<//>
      <${Kv} k="Số lần poll">${c.polls}<//>
      <${Kv} k="Vị trí đọc">${positions || "—"}<//>
      ${c.redeliveries > 0 && html`<${Kv} k="Đọc lại">${c.redeliveries} record<//>`}
    </div>
    <div class="row">
      <button class="btn primary" disabled=${busy || c.auto} onClick=${poll}>${busy ? "đang poll…" : "Poll"}</button>
      <button class="btn" disabled=${c.auto} onClick=${() => lab.act(`${c.name} commit`, "POST", `${path}/commit`)}>Commit</button>
      <button class=${"btn" + (c.auto ? " on" : "")} title="poll và commit liên tục, như một service"
        onClick=${() => lab.act(`${c.name} tự động: ${c.auto ? "tắt" : "bật"}`, "POST", `${path}/auto`, { on: !c.auto })}>${c.auto ? "Tự động: bật" : "Tự động"}</button>
    </div>
    ${c.err && html`<div class="err-text small wrap">${c.err}</div>`}
    <div class="caps">Đã nhận</div>
    ${recv.length === 0 && html`<div class="muted small">Chưa nhận record nào.</div>`}
    <ul class="events">
      ${recv.map((r, i) => html`<li key=${`${r.partition}-${r.offset}-${r.redelivered}-${i}`}>
        <span class="grow ellipsis"><b>P${r.partition}</b>@${r.offset} ${r.key || "∅"} <span class="muted">${r.value}</span></span>
        ${r.redelivered && html`<span class="tag warn">đọc lại</span>`}
      </li>`)}
    </ul>`;
}

function GroupInspector({ g, view }) {
  const parts = (view.topic && view.topic.partitions) || [];
  const members = (view.consumers || []).filter((c) => c.group === g.name);
  return html`
    <${Head} eyebrow="Consumer group" title=${g.name}><span class="tag">${g.state || "?"}</span><//>
    <div class="inset"><${Kv} k="Member">${g.members}<//><${Kv} k="Consumer của lab">${members.map((m) => m.name).join(", ") || "—"}<//></div>
    <div class="scroll-x"><table>
      <thead><tr><th>P</th><th class="num">End</th><th class="num">Committed</th><th class="num">Lag</th></tr></thead>
      <tbody>${parts.map((p) => {
        const c = g.committed && g.committed[p.partition];
        const l = lagOf(p.end, c);
        return html`<tr key=${p.partition}><td>P${p.partition}</td><td class="num">${q(p.end)}</td><td class="num">${c === undefined ? "chưa" : c}</td><td class="num">${l === null ? "?" : l}</td></tr>`;
      })}</tbody>
    </table></div>
    <${Cli} text=${`docker exec lab-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group ${g.name}`} />`;
}

function PartitionInspector({ p, view }) {
  const recs = [...(p.records || [])].reverse().slice(0, 6);
  const down = p.isr.length < p.replicas.length;
  return html`
    <${Head} eyebrow="Partition" title=${`P${p.partition} (${view.topic.name})`}>
      <span class=${"tag " + (p.leader < 0 ? "err" : down ? "warn" : "ok")}>${p.leader < 0 ? "không có leader" : down ? "thiếu ISR" : "đủ ISR"}</span>
    <//>
    <div class="inset">
      <${Kv} k="Leader">${p.leader < 0 ? "không có" : `Broker ${p.leader}`}<//>
      <${Kv} k="Replicas">[${p.replicas.join(", ")}]<//>
      <${Kv} k="ISR">[${p.isr.join(", ")}]<//>
      <${Kv} k="Start offset">${q(p.start)}<//>
      <${Kv} k="End offset">${q(p.end)}<//>
    </div>
    ${recs.length > 0 && html`<div class="caps">Record gần nhất</div>
      <ul class="events">${recs.map((r) => html`<li key=${r.offset}><span class="grow ellipsis"><b>#${r.offset}</b> ${r.key || "∅"} <span class="muted">${r.value}</span></span></li>`)}</ul>`}
    <${Cli} text=${`docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic ${view.topic.name}`} />`;
}

function BrokerInspector({ b, view, lab }) {
  const up = b.state === "running";
  const parts = (view.topic && view.topic.partitions) || [];
  const leads = parts.filter((p) => p.leader === b.id).map((p) => `P${p.partition}`);
  const inIsr = parts.filter((p) => p.isr.includes(b.id)).map((p) => `P${p.partition}`);
  const label = !b.state ? "chưa rõ" : up ? (b.registered ? "đang chạy" : "đang khởi động, chưa đăng ký với controller")
    : b.registered ? "đã tắt, nhưng controller vẫn tưởng còn sống" : `đã tắt (${b.state})`;
  const run = (verb) => lab.act(`docker ${verb} ${b.container}`, "POST", "/api/chaos", { action: verb, target: b.container });
  return html`
    <${Head} eyebrow="Broker" title=${`Broker ${b.id}`}><span class=${"tag " + (up ? "ok" : isDead(b) ? "err" : "")}>${b.state || "?"}</span><//>
    <div class="muted small">${label}</div>
    <div class="inset">
      <${Kv} k="Container">${b.container}<//>
      <${Kv} k="Leader của">${leads.join(", ") || "—"}<//>
      <${Kv} k="Trong ISR của">${inIsr.join(", ") || "—"}<//>
    </div>
    <div class="row">
      <button class="btn danger" disabled=${!up} onClick=${() => run("kill")} title="tắt đột ngột, như mất điện">kill</button>
      <button class="btn" disabled=${!up} onClick=${() => run("stop")} title="tắt có trật tự">stop</button>
      <button class="btn" disabled=${up} onClick=${() => run("start")}>start</button>
    </div>`;
}

const MARK = { ok: "✓", pending: "○", unverified: "?", evicted: "–" };

function Hop({ status, name, children }) {
  return html`<li class=${"hop " + status}><span class="hop-mark">${MARK[status]}</span>
    <span class="hop-name">${name}</span><span class="hop-body">${children}</span></li>`;
}

const unverifiedText = (t) => (t.reason === "acks0" ? "acks=0: không có xác nhận" : "không xác minh được");

function RecordInspector({ record, partition, view, lab }) {
  const name = view.topic.name;
  const t = buildTrace({ topic: name, record, partition: partition.partition, producers: view.producers, consumers: view.consumers,
    groups: view.groups, staleBefore: lab.track.staleBefore[name] || 0, ignoredReads: lab.track.ignoredReads[name] || new Set() });
  const s = t.send;
  return html`
    <${Head} eyebrow="Hành trình record" title=${`#${record.offset} · P${partition.partition}`} />
    <div class="inset">
      <${Kv} k="Key">${record.key || "∅ (không có)"}<//>
      <div class="kv block"><span>Value</span><b>${record.value}</b></div>
    </div>
    <ol class="journey">
      <${Hop} status=${s.status} name="Gửi">
        ${s.status === "ok" ? `${s.producer} · acks=${s.acks} · ${secs(s.tookMs)} · ${time(s.at)}`
          : s.status === "unverified" ? `${s.producer}: ${unverifiedText(s)}` : "không còn trong bộ đệm"}
      <//>
      <${Hop} status="ok" name="Ghi log">
        leader ${partition.leader < 0 ? "không có" : `B${partition.leader}`} · ISR lúc đang xem [${partition.isr.join(",")}]
      <//>
      ${t.reads.length === 0 && html`<${Hop} status="pending" name="Consumer">chưa có consumer nào trên topic này<//>`}
      ${t.reads.map((r) => html`<${Hop} key=${r.consumer} status=${r.status} name="Consumer">
        ${r.consumer} (${r.group}) ${r.status === "ok" ? `đã nhận · đọc lại: ${r.redelivered ? "có" : "không"}`
          : r.status === "pending" ? "chưa đọc" : r.status === "evicted" ? "đã đọc qua, không còn trong bộ đệm" : "không xác minh được"}
      <//>`)}
      ${t.commits.map((c) => html`<${Hop} key=${c.group} status=${c.status} name="Commit">
        ${c.group} ${c.status === "ok" ? `đã commit tới ${c.committed}` : c.committed === null ? "chưa commit" : `mới commit tới ${c.committed}, chưa bao gồm #${record.offset}`}
      <//>`)}
    </ol>
    <div class="muted small">Ghép theo topic, partition, offset, key, value. Chỉ đánh dấu ✓ khi lab có bằng chứng.</div>`;
}

// resolve finds the live object behind a selection; it may have been deleted since it was clicked.
export function resolveSelection(sel, view) {
  if (!sel || !view.topic && sel.kind !== "producer" && sel.kind !== "consumer" && sel.kind !== "broker") return null;
  const parts = (view.topic && view.topic.partitions) || [];
  switch (sel.kind) {
    case "producer": return view.producers.find((p) => p.name === sel.id) || null;
    case "consumer": return view.consumers.find((c) => c.name === sel.id) || null;
    case "group": return view.groups.find((g) => g.name === sel.id) || null;
    case "broker": return view.brokers.find((b) => String(b.id) === String(sel.id)) || null;
    case "partition": return parts.find((p) => String(p.partition) === String(sel.id)) || null;
    case "record": {
      const [pn, off] = String(sel.id).split(":").map(Number);
      const partition = parts.find((p) => p.partition === pn);
      const record = partition && (partition.records || []).find((r) => r.offset === off);
      return record ? { record, partition } : null;
    }
    default: return null;
  }
}

export function Inspector({ selection, view, lab }) {
  const obj = resolveSelection(selection, view);
  let body;
  if (!obj) {
    body = html`
      <div class="caps">Inspector</div>
      <div class="title">Chọn một phần của flow</div>
      <p class="muted">Bấm một ô record để xem hành trình của nó. Bấm producer, partition, broker, group hoặc consumer để xem chi tiết và thao tác.</p>`;
  } else if (selection.kind === "producer") body = html`<${ProducerInspector} key=${obj.name} p=${obj} lab=${lab} />`;
  else if (selection.kind === "consumer") body = html`<${ConsumerInspector} key=${obj.name} c=${obj} lab=${lab} />`;
  else if (selection.kind === "group") body = html`<${GroupInspector} g=${obj} view=${view} />`;
  else if (selection.kind === "partition") body = html`<${PartitionInspector} p=${obj} view=${view} />`;
  else if (selection.kind === "broker") body = html`<${BrokerInspector} b=${obj} view=${view} lab=${lab} />`;
  else body = html`<${RecordInspector} record=${obj.record} partition=${obj.partition} view=${view} lab=${lab} />`;
  return html`<aside class="panel inspector stack">${body}</aside>`;
}
