import { useEffect, useMemo, useState } from "preact/hooks";
import { html, Cli, Safe } from "./shell.js";
import { FlowMap, playgroundView } from "./flow.js";
import { Inspector, TopicSelect } from "./inspector.js";
import { isScenarioTopic, LESSON_TOPIC } from "./store.js";
import { safeStore } from "./api.js";

const nextName = (prefix, taken) => { for (let i = 1; ; i++) if (!taken.includes(prefix + i)) return prefix + i; };

function TopicForm({ lab, done }) {
  const [f, setF] = useState({ name: LESSON_TOPIC, partitions: 3, rf: 3, minIsr: 2 });
  const set = (k, n) => (e) => setF({ ...f, [k]: n ? Number(e.target.value) : e.target.value.trim() });
  const submit = async (e) => {
    e.preventDefault();
    if (await lab.act(`tạo topic ${f.name}`, "POST", "/api/pg/topics", f)) { lab.setTopic(f.name); done(); }
  };
  return html`<form class="tool-form" onSubmit=${submit}>
    <label class="field">Tên<input value=${f.name} onInput=${set("name")} /></label>
    <label class="field">Partition<input type="number" min="1" max="12" value=${f.partitions} onInput=${set("partitions", true)} /></label>
    <label class="field">RF (bản sao)<input type="number" min="1" max="3" value=${f.rf} onInput=${set("rf", true)} /></label>
    <label class="field">min ISR<input type="number" min="1" max=${f.rf} value=${f.minIsr} onInput=${set("minIsr", true)} /></label>
    <button class="btn primary">Tạo</button>
  </form>`;
}

function ProducerForm({ lab, onCreated }) {
  const taken = ((lab.st && lab.st.producers) || []).map((p) => p.name);
  const auto = nextName("p", taken);
  const [name, setName] = useState("");
  const [acks, setAcks] = useState("all");
  const [idem, setIdem] = useState(true);
  const submit = async (e) => {
    e.preventDefault();
    const n = name || auto;
    const ok = await lab.act(`tạo producer ${n} (acks=${acks}, idempotence=${idem && acks === "all"})`, "POST", "/api/pg/producers",
      { name: n, acks, idempotent: idem && acks === "all" });
    if (ok) { setName(""); onCreated({ kind: "producer", id: n }); }
  };
  return html`<form class="tool-form" onSubmit=${submit}>
    <label class="field">Tên<input placeholder=${auto} value=${name} onInput=${(e) => setName(e.target.value.trim())} /></label>
    <label class="field">acks<select value=${acks} onChange=${(e) => setAcks(e.target.value)}>
      <option value="all">all</option><option value="1">1</option><option value="0">0</option></select></label>
    <label class="check" title="Java yêu cầu acks=all khi bật idempotence">
      <input type="checkbox" checked=${idem && acks === "all"} disabled=${acks !== "all"} onChange=${(e) => setIdem(e.target.checked)} />
      enable.idempotence</label>
    <button class="btn primary">Tạo producer</button>
  </form>`;
}

function ConsumerForm({ lab, onCreated }) {
  const taken = ((lab.st && lab.st.consumers) || []).map((c) => c.name);
  const auto = nextName("c", taken);
  const topics = ((lab.st && lab.st.topics) || []).map((t) => t.name).filter((t) => !isScenarioTopic(t));
  const current = isScenarioTopic(lab.topic) ? topics[0] || "" : lab.topic;
  const [f, setF] = useState({ name: "", group: "g1", topic: "", reset: "earliest" });
  const t = f.topic || current;
  const submit = async (e) => {
    e.preventDefault();
    const n = f.name || auto;
    const ok = await lab.act(`tạo consumer ${n} (group ${f.group}, ${t}, ${f.reset})`, "POST", "/api/pg/consumers",
      { name: n, group: f.group, topic: t, reset: f.reset });
    if (ok) { setF({ ...f, name: "" }); onCreated({ kind: "consumer", id: n }); }
  };
  return html`<form class="tool-form" onSubmit=${submit}>
    <label class="field">Tên<input placeholder=${auto} value=${f.name} onInput=${(e) => setF({ ...f, name: e.target.value.trim() })} /></label>
    <label class="field">group.id<input value=${f.group} onInput=${(e) => setF({ ...f, group: e.target.value.trim() })} /></label>
    <label class="field">Topic<${TopicSelect} topics=${topics} value=${t} onChange=${(v) => setF({ ...f, topic: v })} /></label>
    <label class="field" title="dùng cho partition mà group chưa commit, hoặc có offset đã commit không còn trong log">auto.offset.reset
      <select value=${f.reset} onChange=${(e) => setF({ ...f, reset: e.target.value })}><option value="earliest">earliest</option><option value="latest">latest</option></select></label>
    <button class="btn primary" disabled=${!t}>Tạo consumer</button>
  </form>`;
}

const SOURCE = {
  topics: "danh sách topic là bản đọc lần trước",
  brokers: "trạng thái broker là bản đọc lần trước",
  groups: "cờ commit của group là bản đọc lần trước",
  topic: "chưa đọc được offset của một số partition (hiện “?”)",
};

function Toolbar({ lab, onCreated, open, setOpen }) {
  const topics = ((lab.st && lab.st.topics) || []).map((t) => t.name);
  const toggle = (k) => setOpen(open === k ? null : k);
  const del = () => {
    if (confirm(`Xoá topic ${lab.topic}? Mọi record trong đó sẽ mất.`)) {
      lab.act(`xoá topic ${lab.topic}`, "DELETE", `/api/pg/topics/${encodeURIComponent(lab.topic)}`);
    }
  };
  const clear = () => {
    if (confirm("Đóng mọi producer/consumer, xoá các topic và group của sân chơi, và xoá tiến độ bài học?")) {
      safeStore.set("pg-done", "");
      lab.act("dọn sân chơi", "POST", "/api/pg/clear");
    }
  };
  const errs = Object.entries((lab.st && lab.st.errors) || {}).filter(([k]) => k !== "lessonTopic");
  return html`
    <div class="toolbar">
      <div class="row">
        <label class="tool-select"><span class="caps">Topic</span>
          <${TopicSelect} topics=${topics} value=${lab.topic} onChange=${lab.setTopic} /></label>
        ${lab.topic && !isScenarioTopic(lab.topic) && html`<button class="btn tiny danger" onClick=${del} title="xoá topic và mọi record trong đó">xoá</button>`}
        <span class="grow"></span>
        <button class=${"btn" + (open === "topic" ? " on" : "")} onClick=${() => toggle("topic")}>+ Topic</button>
        <button class=${"btn" + (open === "producer" ? " on" : "")} onClick=${() => toggle("producer")}>+ Producer</button>
        <button class=${"btn" + (open === "consumer" ? " on" : "")} onClick=${() => toggle("consumer")}>+ Consumer</button>
        <button class="btn danger" onClick=${clear}>Dọn sân chơi</button>
      </div>
      ${open === "topic" && html`<${TopicForm} lab=${lab} done=${() => setOpen((o) => (o === "topic" ? null : o))} />`}
      ${open === "producer" && html`<${ProducerForm} lab=${lab} onCreated=${onCreated} />`}
      ${open === "consumer" && html`<${ConsumerForm} lab=${lab} onCreated=${onCreated} />`}
      ${errs.length > 0 && html`<div class="banner" title=${errs.map(([k, v]) => `${k}: ${v}`).join("\n")}>
        Có broker không trả lời: ${errs.map(([k]) => SOURCE[k] || k).join("; ")}. Rê chuột vào đây để xem lỗi gốc.</div>`}
    </div>`;
}

export function Workspace({ lab }) {
  const [selection, setSelection] = useState(null);
  const [open, setOpen] = useState(null);
  const view = useMemo(() => playgroundView(lab.st), [lab.st]);
  const select = (s) => setSelection((cur) => (cur && s && cur.kind === s.kind && String(cur.id) === String(s.id) ? null : s));

  // A selection belongs to the topic it was made on, except clients and brokers, which outlive it.
  useEffect(() => {
    setSelection((s) => (s && ["partition", "record", "group"].includes(s.kind) ? null : s));
  }, [lab.topic]);

  const empty = html`<p>Chưa có topic nào được chọn. Bài 1 bắt đầu bằng việc tạo topic <code>orders</code>.</p>
    <button class="btn primary" onClick=${() => setOpen("topic")}>+ Tạo topic</button>`;
  return html`
    <div class="workspace">
      <div class="stack work-main">
        <${Toolbar} lab=${lab} onCreated=${setSelection} open=${open} setOpen=${setOpen} />
        <${Safe} name="flow"><${FlowMap} lab=${lab} selection=${selection} onSelect=${select} empty=${empty} /><//>
      </div>
      <${Safe} name="inspector"><${Inspector} selection=${selection} view=${view} lab=${lab} /><//>
      <div class="work-foot">
        <div class="panel stack">
          <div class="caps">Lệnh tương đương</div>
          ${lab.cli ? html`<${Cli} text=${lab.cli} />` : html`<div class="muted small">Mỗi thao tác bạn làm sẽ hiện lệnh CLI tương ứng ở đây, để dùng được ở prod.</div>`}
        </div>
        <div class="panel stack">
          <div class="caps">Nhật ký</div>
          <ul class="events log-list">
            ${lab.log.map((e, i) => html`<li key=${i} class=${e.err ? "err-text" : ""}><span class="muted mono">${e.t}</span><span class="grow wrap">${e.text}</span></li>`)}
          </ul>
        </div>
      </div>
    </div>`;
}
