import { h, render } from "preact";
import { useEffect, useErrorBoundary, useRef, useState } from "preact/hooks";
import htm from "htm";

const html = htm.bind(h);
const LESSON_TOPIC = "orders";
const isScenarioTopic = (name) => name.startsWith("lab.") || name.startsWith("dlq-lab.");

// Storage can be blocked (private window, cleared site data); the page must work without it.
const store = {
  get(k, d) { try { const v = localStorage.getItem(k); return v === null ? d : v; } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* per-viewer convenience only */ } },
};

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (method !== "GET") {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body || {});
  }
  const r = await fetch(path, opts);
  const text = await r.text();
  if (!r.ok) throw new Error(text.trim() || `HTTP ${r.status}`);
  return text ? JSON.parse(text) : {};
}

// FNV-1a spread over the golden angle, so similar keys like xe-1 and xe-4 get distinct colours.
function hue(key) {
  let h = 2166136261;
  for (const c of key) h = Math.imul(h ^ c.codePointAt(0), 16777619);
  return Math.round(((h >>> 0) * 137.508) % 360);
}

const ago = (iso) => (Date.now() - new Date(iso).getTime()) / 1000;
const secs = (ms) => (ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`);

// ---------------------------------------------------------------- app

function App() {
  const [mode, setMode] = useState(store.get("pg-mode", "lessons"));
  const [lessons, setLessons] = useState([]);
  const [lessonId, setLessonId] = useState(store.get("pg-lesson", "1"));
  const [topic, setTopic] = useState(() => {
    const t = store.get("pg-topic", LESSON_TOPIC);
    return isScenarioTopic(t) ? LESSON_TOPIC : t;
  });
  const [st, setSt] = useState(null);
  const [live, setLive] = useState(false);
  const [log, setLog] = useState([]);
  const [cli, setCli] = useState("");
  const [toast, setToast] = useState(null);
  const lastGood = useRef({});
  const topicRef = useRef(topic);
  topicRef.current = topic;

  const note = (text, err = false) => {
    setLog((l) => [{ t: new Date().toLocaleTimeString(), text, err }, ...l].slice(0, 80));
    setToast({ text, err });
  };
  useEffect(() => { if (toast) { const id = setTimeout(() => setToast(null), 3500); return () => clearTimeout(id); } }, [toast]);

  // act runs one user action, logs it and shows the equivalent CLI command the server sent back;
  // describe, when given, turns the response into the log line.
  const act = async (label, method, path, body, describe) => {
    try {
      const res = await api(method, path, body);
      if (res.cli) setCli(res.cli);
      note(describe ? describe(res) : label);
      poke();
      return res;
    } catch (e) {
      note(`${label}: ${e.message}`, true);
      return null;
    }
  };

  const pokeRef = useRef(() => {});
  const poke = () => pokeRef.current();
  useEffect(() => {
    let stop = false, timer;
    const tick = async () => {
      clearTimeout(timer);
      try {
        const s = await api("GET", `/api/pg/state?topic=${encodeURIComponent(topicRef.current)}`);
        if (stop) return;
        // A source that fails while a broker is down keeps showing its last answer instead of vanishing.
        for (const k of ["topics", "brokers", "groups"]) {
          const slot = k === "groups" ? `groups:${topicRef.current}` : k; // commit flags belong to one topic
          if (s.errors && s.errors[k]) s[k] = lastGood.current[slot] || [];
          else lastGood.current[slot] = s[k] || [];
        }
        setSt(s);
        setLive(true);
      } catch {
        if (!stop) setLive(false);
      }
      if (!stop) timer = setTimeout(tick, 700);
    };
    pokeRef.current = tick;
    tick();
    api("GET", "/api/pg/lessons").then(setLessons).catch((e) => note(`không tải được bài học: ${e.message}`, true));
    return () => { stop = true; clearTimeout(timer); };
  }, []);

  useEffect(() => { store.set("pg-mode", mode); }, [mode]);
  useEffect(() => { store.set("pg-lesson", lessonId); }, [lessonId]);
  // A scenario topic is a one-off look; it is not where the playground reopens.
  useEffect(() => { if (!isScenarioTopic(topic)) store.set("pg-topic", topic); poke(); }, [topic]);

  // Fall back to an existing playground topic once the list is known; none means the empty state.
  useEffect(() => {
    if (!st || !st.topics) return;
    const names = st.topics.map((t) => t.name);
    if (topic && names.includes(topic)) return;
    const own = names.filter((n) => !isScenarioTopic(n));
    setTopic(own.includes(LESSON_TOPIC) ? LESSON_TOPIC : own[0] || "");
  }, [st && st.topics && st.topics.map((t) => t.name).join(",")]);

  const ctx = { st, topic, setTopic, act, note };
  return html`
    <div class="app">
      <${Header} mode=${mode} setMode=${setMode} live=${live} act=${act} />
      <main class=${mode}>
        ${mode === "lessons" && html`<div class="col side"><${Safe} name="bài học"><${LessonPanel} lessons=${lessons}
          statuses=${(st && st.lessons) || []} id=${lessonId} setId=${setLessonId} /><//></div>`}
        <div class="col center"><${Safe} name="cluster"><${Cluster} ...${ctx} /><//></div>
        <div class="col side"><${Safe} name="producer/consumer"><${Clients} ...${ctx} /><//></div>
      </main>
      <div class="foot">
        <div>
          <h2>Lệnh tương đương</h2>
          ${cli ? html`<${Cli} text=${cli} />` : html`<div class="muted small">Mỗi thao tác bạn làm sẽ hiện lệnh CLI tương ứng ở đây, để dùng được ở prod.</div>`}
        </div>
        <div>
          <h2>Nhật ký</h2>
          <ul>${log.map((e) => html`<li class=${e.err ? "err" : ""}><span class="t">${e.t}</span>${e.text}</li>`)}</ul>
        </div>
      </div>
      ${toast && html`<div class=${"toast" + (toast.err ? " err" : "")}>${toast.text}</div>`}
    </div>`;
}

// Safe keeps one broken panel from blanking the whole page.
function Safe({ name, children }) {
  const [err, reset] = useErrorBoundary();
  if (err) {
    return html`<div class="errs">Panel ${name} gặp lỗi hiển thị: ${String((err && err.message) || err)}
      <button class="btn tiny" onClick=${reset}>vẽ lại</button></div>`;
  }
  return children;
}

function Header({ mode, setMode, live, act }) {
  const clear = () => {
    if (confirm("Đóng mọi producer/consumer, xoá các topic và group của sân chơi, và xoá tiến độ bài học?")) {
      store.set("pg-done", "");
      act("dọn sân chơi", "POST", "/api/pg/clear");
    }
  };
  return html`
    <header class="top">
      <span class="brand">Kafka Lab</span>
      <div class="tabs">
        <button class=${mode === "lessons" ? "on" : ""} onClick=${() => setMode("lessons")}>Học theo bài</button>
        <button class=${mode === "free" ? "on" : ""} onClick=${() => setMode("free")}>Sân chơi tự do</button>
      </div>
      <a href="classic.html#free">Kịch bản nâng cao (service thật) →</a>
      <span class="spacer"></span>
      <span class="live"><span class=${"dot " + (live ? "ok" : "bad")}></span>${live ? "đang cập nhật" : "mất kết nối với lab"}</span>
      <button class="btn danger" onClick=${clear}>Dọn sân chơi</button>
    </header>`;
}

function Cli({ text }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try { await navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1200); } catch { /* clipboard blocked */ }
  };
  return html`<div class="cli">${text}<button class="btn tiny" onClick=${copy}>${copied ? "đã chép" : "chép"}</button></div>`;
}

// ---------------------------------------------------------------- lessons

// Progress lives in lab-control's memory; the browser remembers it too, so restarting the lab keeps it.
function rememberDone(statuses) {
  const done = new Set(store.get("pg-done", "").split(",").filter(Boolean));
  for (const s of statuses) if (s.done) done.add(s.id);
  store.set("pg-done", [...done].join(","));
  return statuses.map((s) => ({ ...s, done: s.done || done.has(s.id) }));
}

function LessonPanel({ lessons, statuses: live, id, setId }) {
  const statuses = rememberDone(live);
  const status = Object.fromEntries(statuses.map((s) => [s.id, s]));
  const done = statuses.filter((s) => s.done).length;
  const l = lessons.find((x) => x.id === id) || lessons[0];
  return html`
    <div>
      <h2>Bài học · ${done}/${lessons.length || 8} xong</h2>
      <div class="progress"><div style=${`width:${lessons.length ? (100 * done) / lessons.length : 0}%`}></div></div>
      <div class="lesson-list">
        ${lessons.map((x) => html`
          <button class=${"lesson-item" + (l && x.id === l.id ? " on" : "")} onClick=${() => setId(x.id)}>
            <span class=${"lesson-num" + (status[x.id] && status[x.id].done ? " done" : "")}>${status[x.id] && status[x.id].done ? "✓" : x.id}</span>
            <span>${x.title}</span>
          </button>`)}
      </div>
      ${l && html`<${Lesson} key=${l.id} l=${l} s=${status[l.id]} />`}
    </div>`;
}

function Lesson({ l, s }) {
  const [guess, setGuess] = useState(store.get(`pg-predict-${l.id}`, ""));
  const [shown, setShown] = useState(false);
  const done = s && s.done;
  return html`
    <div class="lesson">
      <h3>Bài ${l.id} · ${l.title}</h3>
      <div class="predict">
        <p><b>Đoán trước khi làm.</b> ${l.question}</p>
        <textarea placeholder="Ghi dự đoán của bạn (chỉ lưu trên máy này)…" value=${guess}
          onInput=${(e) => { setGuess(e.target.value); store.set(`pg-predict-${l.id}`, e.target.value); }}></textarea>
      </div>
      <div class="section">
        <h2>Làm</h2>
        <ol class="steps">${l.steps.map((x) => html`<li>${x}</li>`)}</ol>
      </div>
      <div class=${"status" + (done ? " done" : "")}>
        <div class="goal">${done ? "✓ Xong: " : "Mục tiêu: "}${l.goal}</div>
        <div class="detail">${s ? s.detail : "…"}</div>
      </div>
      <div class="section">
        ${shown || done
          ? html`
            <h2>Giải thích</h2>
            ${guess && html`<div class="hint">Dự đoán của bạn: “${guess}”. Đúng chỗ nào, sai chỗ nào?</div>`}
            <div class="reveal" dangerouslySetInnerHTML=${{ __html: l.reveal }}></div>
            <h2 style="margin-top:14px">Lệnh tương đương</h2>
            ${l.cli.map((c) => html`<${Cli} text=${c} />`)}
            <h2 style="margin-top:14px">Tự kiểm tra (không có đáp án)</h2>
            <ul class="confirm">${l.confirm.map((q) => html`<li>${q}</li>`)}</ul>`
          : html`<div class="row"><span class="muted small">Giải thích mở ra khi bạn đạt mục tiêu.</span>
              <button class="linkbtn" onClick=${() => setShown(true)}>xem luôn</button></div>`}
      </div>
    </div>`;
}

// ---------------------------------------------------------------- cluster

function Cluster({ st, topic, setTopic, act }) {
  const [creating, setCreating] = useState(false);
  if (!st) return html`<div class="muted">Đang kết nối tới lab…</div>`;
  const topics = st.topics || [];
  const own = topics.filter((t) => !isScenarioTopic(t.name));
  const errs = Object.entries(st.errors || {}).filter(([k]) => k !== "lessonTopic");
  const del = () => {
    if (confirm(`Xoá topic ${topic}? Mọi record trong đó sẽ mất.`)) act(`xoá topic ${topic}`, "DELETE", `/api/pg/topics/${encodeURIComponent(topic)}`);
  };
  return html`
    <div>
      <div class="section">
        <h2>Broker</h2>
        <${Brokers} st=${st} act=${act} />
      </div>
      <div class="section">
        <h2>Topic</h2>
        <div class="topicbar">
          ${own.length > 0 || topic
            ? html`<${TopicSelect} topics=${topics.map((t) => t.name)} value=${topic} onChange=${setTopic} />`
            : ""}
          ${topic && !isScenarioTopic(topic) && html`<button class="btn tiny danger" onClick=${del} title="xoá topic và mọi record trong đó">xoá</button>`}
          ${st.topic && html`<span class="chip">${st.topic.partitions.length} partition</span>
            <span class="chip">RF ${st.topic.rf || "?"}</span>
            ${st.topic.minIsr > 0 && html`<span class="chip">min ISR ${st.topic.minIsr}</span>`}`}
          <span class="spacer grow"></span>
          <button class=${"btn" + (creating ? " on" : "")} onClick=${() => setCreating(!creating)}>＋ Topic mới</button>
        </div>
        ${creating && html`<${CreateTopic} act=${act} done=${(name) => { setCreating(false); setTopic(name); }} />`}
        ${errs.length > 0 && html`<div class="errs" title=${errs.map(([k, v]) => `${k}: ${v}`).join("\n")}>
          ⚠ Có broker không trả lời: ${errs.map(([k]) => SOURCE[k] || k).join("; ")}. Rê chuột vào đây để xem lỗi gốc.</div>`}
        ${st.topic ? html`<${Lanes} st=${st} />`
          : topic ? html`<div class="muted">Đang đọc topic…</div>`
          : !creating && html`<div class="empty-state"><p>Sân chơi chưa có topic nào. Bài 1 bắt đầu bằng việc tạo topic <code>orders</code>.</p>
              <button class="btn primary" onClick=${() => setCreating(true)}>＋ Tạo topic</button></div>`}
      </div>
    </div>`;
}

const SOURCE = {
  topics: "danh sách topic là bản đọc lần trước",
  brokers: "trạng thái broker là bản đọc lần trước",
  groups: "cờ commit của group là bản đọc lần trước",
  topic: "chưa đọc được offset của một số partition (hiện “?”)",
};

function TopicSelect({ topics, value, onChange }) {
  const own = topics.filter((t) => !isScenarioTopic(t));
  const scen = topics.filter(isScenarioTopic);
  return html`<select value=${value} onChange=${(e) => onChange(e.target.value)}>
    ${!value && html`<option value="">chọn topic</option>`}
    ${own.map((t) => html`<option value=${t}>${t}</option>`)}
    ${scen.length > 0 && html`<optgroup label="của kịch bản nâng cao">${scen.map((t) => html`<option value=${t}>${t}</option>`)}</optgroup>`}
  </select>`;
}

function Brokers({ st, act }) {
  const leads = {};
  for (const p of (st.topic && st.topic.partitions) || []) leads[p.leader] = (leads[p.leader] || 0) + 1;
  return html`
    <div class="brokers">
      ${(st.brokers || []).map((b) => {
        const up = b.state === "running";
        // Killed but not yet fenced: the controller still lists it until broker.session.timeout.ms passes.
        const limbo = !up && b.registered;
        const cls = up ? "" : limbo ? " limbo" : " down";
        const label = up ? (b.registered ? "đang chạy" : "đang khởi động, chưa đăng ký với controller")
          : limbo ? "đã tắt, nhưng controller vẫn tưởng còn sống" : `đã tắt (${b.state || "?"})`;
        const run = (verb) => act(`docker ${verb} ${b.container}`, "POST", "/api/chaos", { action: verb, target: b.container });
        return html`
          <div class=${"card broker" + cls}>
            <div class="row"><span class=${"dot " + (up ? "ok" : limbo ? "warn" : "bad")}></span><span class="name">Broker ${b.id}</span>
              ${leads[b.id] ? html`<span class="chip ok">leader của ${leads[b.id]} partition</span>` : ""}</div>
            <div class="small muted">${label}</div>
            <div class="actions">
              <button class="btn tiny danger" disabled=${!up} onClick=${() => run("kill")} title="tắt đột ngột, như mất điện">kill</button>
              <button class="btn tiny" disabled=${!up} onClick=${() => run("stop")} title="tắt có trật tự">stop</button>
              <button class="btn tiny" disabled=${up} onClick=${() => run("start")}>start</button>
            </div>
          </div>`;
      })}
    </div>`;
}

function CreateTopic({ act, done }) {
  const [f, setF] = useState({ name: LESSON_TOPIC, partitions: 3, rf: 3, minIsr: 2 });
  const set = (k, num) => (e) => setF({ ...f, [k]: num ? Number(e.target.value) : e.target.value.trim() });
  const submit = async (e) => {
    e.preventDefault();
    if (await act(`tạo topic ${f.name}`, "POST", "/api/pg/topics", f)) done(f.name);
  };
  return html`
    <form class="card form g4" style="margin-bottom:10px" onSubmit=${submit}>
      <label class="field">Tên<input value=${f.name} onInput=${set("name")} /></label>
      <label class="field">Partition<input type="number" min="1" max="12" value=${f.partitions} onInput=${set("partitions", true)} /></label>
      <label class="field">RF (bản sao)<input type="number" min="1" max="3" value=${f.rf} onInput=${set("rf", true)} /></label>
      <label class="field">min ISR<input type="number" min="1" max=${f.rf} value=${f.minIsr} onInput=${set("minIsr", true)} /></label>
      <button class="btn primary">Tạo</button>
    </form>`;
}

function Lanes({ st }) {
  // Unknown docker state stays undefined, so nothing is drawn as dead without evidence.
  const brokerUp = Object.fromEntries((st.brokers || []).map((b) => [b.id, b.state ? b.state === "running" : undefined]));
  const groups = st.groups || [];
  const consumers = (st.consumers || []).filter((c) => c.topic === st.topic.name);
  return html`
    <div>
      <div class="legend">
        <span><span class="replica leader">1</span> leader</span>
        <span><span class="replica">2</span> trong ISR</span>
        <span><span class="replica out">3</span> ngoài ISR</span>
        <span><span class="replica dead-isr">2</span> đã tắt, vẫn trong ISR</span>
        <span><span class="replica dead">3</span> đã tắt, ra khỏi ISR</span>
        <span><span class="mark commit">g1</span> committed offset của group</span>
        <span><span class="mark pos">c1</span> vị trí đọc của consumer (chưa commit)</span>
      </div>
      ${st.topic.partitions.map((p) => html`<${Lane} key=${p.partition} p=${p} up=${brokerUp} groups=${groups} consumers=${consumers} />`)}
    </div>`;
}

function Lane({ p, up, groups, consumers }) {
  const recs = p.records || [];
  // Keep the newest record in view as the log grows.
  const logRef = useRef(null);
  const lastOffset = recs.length ? recs[recs.length - 1].offset : -1;
  useEffect(() => { if (logRef.current) logRef.current.scrollLeft = logRef.current.scrollWidth; }, [lastOffset, p.end]);
  const first = recs.length ? recs[0].offset : p.end;
  const known = p.start >= 0 && p.end >= 0;
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
  for (const g of groups) {
    const c = g.committed && g.committed[p.partition];
    if (c !== undefined) pin(c, { cls: "commit", text: g.name, title: `${g.name} đã commit: đọc tiếp từ offset ${c}` });
  }
  for (const c of consumers) {
    const pos = c.positions && c.positions[p.partition];
    if (pos !== undefined && c.assigned && c.assigned.includes(p.partition)) {
      pin(pos, { cls: "pos", text: c.name, title: `${c.name} đã poll tới đây (offset ${pos}), chưa chắc đã commit` });
    }
  }
  const owners = consumers.filter((c) => c.assigned && c.assigned.includes(p.partition));
  const total = known ? p.end - p.start : null;
  return html`
    <div class="lane">
      <div class="lane-head">
        <span class="pname">P${p.partition}</span>
        ${p.leader < 0 ? html`<span class="chip bad">không có leader</span>` : ""}
        ${p.replicas.map((r) => {
          const inIsr = p.isr.includes(r);
          const leader = r === p.leader;
          // The window lesson 7 is about: a dead broker still listed as leader or still in the ISR.
          const cls = up[r] === false
            ? leader ? "replica leader dead-leader" : inIsr ? "replica dead-isr" : "replica dead"
            : leader ? "replica leader" : inIsr ? "replica" : "replica out";
          const state = up[r] === false
            ? leader ? "đã tắt nhưng vẫn là leader: controller chưa chọn leader mới"
              : inIsr ? "đã tắt nhưng vẫn trong ISR: acks=all phải đợi nó" : "đã tắt, đã bị loại khỏi ISR"
            : leader ? "leader" : inIsr ? "trong ISR" : "ngoài ISR (đang đuổi theo leader)";
          const title = `broker ${r} · ${state}`;
          return html`<span class=${cls} title=${title}>${r}</span>`;
        })}
        <span class="lane-meta">${known ? `start ${p.start} · end ${p.end} · ${total} record` : "offset: ? (leader không trả lời)"}</span>
      </div>
      <div class="log" ref=${logRef}>
        ${slots.length === 0 && html`<div class="cell empty">trống</div>`}
        ${slots.map((s) => html`
          <div class="slot" key=${s.kind + s.at}>
            ${s.kind === "more" && html`<div class="cell more">… ${hidden} record trước</div>`}
            ${s.kind === "next" && html`<div class="cell next" title="record kế tiếp ghi vào partition này sẽ có offset ${p.end}">kế tiếp<br />${p.end}</div>`}
            ${s.kind === "rec" && html`
              <div class=${"cell" + (s.r.key ? "" : " nokey")} style=${s.r.key ? `--h:${hue(s.r.key)}` : ""}
                title=${`P${p.partition} offset ${s.r.offset}\nkey: ${s.r.key || "(không có)"}\nvalue: ${s.r.value}`}>
                <span class="off">${s.r.offset}</span>
                <span class="k">${s.r.key || "∅"}</span>
                <span class="v">${s.r.value}</span>
              </div>`}
            <div class="marks">${(marks[s.at] || []).map((m) => html`<span class=${"mark " + m.cls} title=${m.title}>${m.text}</span>`)}</div>
          </div>`)}
      </div>
      <div class="owners">${owners.length ? `đang đọc: ${owners.map((c) => `${c.name} (${c.group})`).join(", ")}` : "chưa consumer nào của sân chơi được gán partition này"}</div>
    </div>`;
}

// ---------------------------------------------------------------- producers and consumers

function nextName(prefix, taken) {
  for (let i = 1; ; i++) if (!taken.includes(prefix + i)) return prefix + i;
}

function Clients({ st, topic, act }) {
  if (!st) return null;
  // Sends and consumers stay on playground topics; scenario topics feed the gap service.
  const topics = (st.topics || []).map((t) => t.name).filter((t) => !isScenarioTopic(t));
  const current = isScenarioTopic(topic) ? topics[0] || "" : topic;
  return html`
    <div>
      <div class="section">
        <h2>Producer</h2>
        <${AddProducer} act=${act} taken=${(st.producers || []).map((p) => p.name)} />
        ${(st.producers || []).map((p) => html`<${Producer} key=${p.name} p=${p} topic=${current} topics=${topics} act=${act} />`)}
      </div>
      <div class="section">
        <h2>Consumer</h2>
        <${AddConsumer} act=${act} topic=${current} topics=${topics} taken=${(st.consumers || []).map((c) => c.name)} />
        <${Groups} st=${st} act=${act} />
      </div>
    </div>`;
}

function AddProducer({ act, taken }) {
  const [acks, setAcks] = useState("all");
  const [idem, setIdem] = useState(true);
  const name = nextName("p", taken);
  const [custom, setCustom] = useState("");
  const submit = (e) => {
    e.preventDefault();
    const n = custom || name;
    act(`tạo producer ${n} (acks=${acks}, idempotence=${idem && acks === "all"})`, "POST", "/api/pg/producers",
      { name: n, acks, idempotent: idem && acks === "all" }).then((r) => r && setCustom(""));
  };
  return html`
    <form class="add form two" onSubmit=${submit}>
      <label class="field">Tên<input placeholder=${name} value=${custom} onInput=${(e) => setCustom(e.target.value.trim())} /></label>
      <label class="field">acks<select value=${acks} onChange=${(e) => setAcks(e.target.value)}>
        <option value="all">all</option><option value="1">1</option><option value="0">0</option></select></label>
      <label class="check full" title="Java yêu cầu acks=all khi bật idempotence">
        <input type="checkbox" checked=${idem && acks === "all"} disabled=${acks !== "all"} onChange=${(e) => setIdem(e.target.checked)} />
        enable.idempotence <span class="muted small">(chỉ dùng được với acks=all)</span></label>
      <button class="btn primary">＋ Tạo producer</button>
    </form>`;
}

function Producer({ p, topic, topics, act }) {
  const [f, setF] = useState({ topic: "", key: "", value: "v{i}", count: 1 });
  const t = f.topic || topic;
  const acks = (p.javaProps.find((x) => x.startsWith("acks=")) || "acks=all").slice(5);
  const send = (e) => {
    e.preventDefault();
    act(`${p.name} gửi ${f.count} record vào ${t}${f.key ? ` (key ${f.key})` : " (không key)"}`, "POST",
      `/api/pg/producers/${encodeURIComponent(p.name)}/send`, { topic: t, key: f.key, value: f.value, count: f.count, acks });
  };
  const sends = [...(p.sends || [])].reverse().slice(0, 8);
  return html`
    <div class="card">
      <div class="client-head"><span class="name">${p.name}</span><span class="spacer grow"></span>
        <button class="btn tiny danger" onClick=${() => act(`xoá producer ${p.name}`, "DELETE", `/api/pg/producers/${encodeURIComponent(p.name)}`)}>xoá</button></div>
      <div class="props">${p.javaProps.map((x) => html`<span class="chip mono">${x}</span>`)}</div>
      <form class="form send" onSubmit=${send}>
        <label class="field wide">Topic<${TopicSelect} topics=${topics} value=${t} onChange=${(v) => setF({ ...f, topic: v })} /></label>
        <label class="field">Số lượng<input type="number" min="1" max="1000" value=${f.count} onInput=${(e) => setF({ ...f, count: Number(e.target.value) })} /></label>
        <label class="field">Key<input placeholder="trống = không key" value=${f.key} onInput=${(e) => setF({ ...f, key: e.target.value })} /></label>
        <label class="field">Value<input value=${f.value} onInput=${(e) => setF({ ...f, value: e.target.value })} /></label>
        <button class="btn primary" disabled=${!t}>Gửi</button>
      </form>
      <div class="hint">{i} trong key hoặc value được thay bằng 1, 2, 3… theo thứ tự record.</div>
      ${sends.length > 0 && html`<ul class="events">
        ${sends.map((s) => html`<li key=${s.id}>
          ${s.pending
            ? html`<span class="spin"></span><span>${s.key || "∅"} → ${s.topic}: đang chờ ack… ${ago(s.at).toFixed(1)} s</span>`
            : s.err
              ? html`<span class="err">✗ ${s.key || "∅"}: ${s.err}</span><span class="when">${secs(s.tookMs)}</span>`
              : html`<span>${s.key || "∅"} → <b>P${s.partition}</b> @${s.offset}</span>
                  ${s.brokersDown > 0 ? html`<span class="chip warn">${s.brokersDown} broker tắt</span>` : ""}
                  <span class="when">${secs(s.tookMs)}</span>`}
        </li>`)}
      </ul>`}
    </div>`;
}

function AddConsumer({ act, topic, topics, taken }) {
  const name = nextName("c", taken);
  const [f, setF] = useState({ name: "", group: "g1", topic: "", reset: "earliest" });
  const t = f.topic || topic;
  const submit = (e) => {
    e.preventDefault();
    const n = f.name || name;
    act(`tạo consumer ${n} (group ${f.group}, ${t}, ${f.reset})`, "POST", "/api/pg/consumers",
      { name: n, group: f.group, topic: t, reset: f.reset }).then((r) => r && setF({ ...f, name: "" }));
  };
  return html`
    <form class="add form two" onSubmit=${submit}>
      <label class="field">Tên<input placeholder=${name} value=${f.name} onInput=${(e) => setF({ ...f, name: e.target.value.trim() })} /></label>
      <label class="field">group.id<input value=${f.group} onInput=${(e) => setF({ ...f, group: e.target.value.trim() })} /></label>
      <label class="field">Topic<${TopicSelect} topics=${topics} value=${t} onChange=${(v) => setF({ ...f, topic: v })} /></label>
      <label class="field" title="dùng cho partition mà group chưa commit, hoặc có offset đã commit không còn trong log">auto.offset.reset<select value=${f.reset} onChange=${(e) => setF({ ...f, reset: e.target.value })}>
        <option value="earliest">earliest</option><option value="latest">latest</option></select></label>
      <button class="btn primary" disabled=${!t}>＋ Tạo consumer</button>
    </form>`;
}

function Groups({ st, act }) {
  const byGroup = {};
  for (const c of st.consumers || []) (byGroup[c.group] = byGroup[c.group] || []).push(c);
  const info = Object.fromEntries((st.groups || []).map((g) => [g.name, g]));
  const names = Object.keys(byGroup).sort();
  if (!names.length) return html`<div class="muted small">Chưa có consumer nào.</div>`;
  return names.map((g) => {
    const gi = info[g];
    const committed = gi && Object.keys(gi.committed || {}).length
      ? Object.entries(gi.committed).map(([p, o]) => `P${p}@${o}`).join(" ") : "chưa commit";
    return html`
      <div class="group" key=${g}>
        <div class="row small" style="margin-bottom:8px">
          <b class="mono">${g}</b>
          ${gi && html`<span class="chip">${gi.state || "?"} · ${gi.members} member</span>`}
          <span class="muted mono">commit: ${committed}</span>
        </div>
        ${byGroup[g].map((c) => html`<${Consumer} key=${c.name} c=${c} act=${act} />`)}
      </div>`;
  });
}

function Consumer({ c, act }) {
  const [busy, setBusy] = useState(false);
  const path = `/api/pg/consumers/${encodeURIComponent(c.name)}`;
  const poll = async () => {
    setBusy(true);
    await act(`${c.name} poll`, "POST", `${path}/poll`, { max: 10 }, (res) => {
      const recs = res.data || [];
      if (!recs.length) return `${c.name} poll: không nhận được record nào sau 3 s`;
      const again = recs.filter((x) => x.redelivered).length;
      return `${c.name} poll: nhận ${recs.length} record${again ? `, trong đó ${again} record đọc lại` : ""}`;
    });
    setBusy(false);
  };
  const recv = [...(c.received || [])].reverse().slice(0, 8);
  const positions = Object.entries(c.positions || {}).map(([p, o]) => `P${p}@${o}`).join(" ");
  return html`
    <div class="card">
      <div class="client-head">
        <span class="name">${c.name}</span>
        ${c.assigned && c.assigned.length
          ? c.assigned.map((p) => html`<span class="chip accent mono">P${p}</span>`)
          : html`<span class="chip warn">chưa có partition</span>`}
        <span class="spacer grow"></span>
        <button class="btn tiny danger" onClick=${() => act(`xoá consumer ${c.name} (rời group ${c.group})`, "DELETE", path)}>xoá</button>
      </div>
      <div class="small muted" style="margin:4px 0 8px">${c.topic} · ${c.reset} · ${c.polls} lần poll${positions ? ` · vị trí ${positions}` : ""}${c.redeliveries ? ` · ${c.redeliveries} record đọc lại` : ""}</div>
      <div class="row">
        <button class="btn primary" disabled=${busy || c.auto} onClick=${poll}>${busy ? "đang poll…" : "Poll"}</button>
        <button class="btn" disabled=${c.auto} onClick=${() => act(`${c.name} commit`, "POST", `${path}/commit`)}>Commit</button>
        <button class=${"btn" + (c.auto ? " on" : "")} title="poll và commit liên tục, như một service" onClick=${() => act(`${c.name} tự động: ${c.auto ? "tắt" : "bật"}`, "POST", `${path}/auto`, { on: !c.auto })}>
          ${c.auto ? "Tự động: bật" : "Tự động"}</button>
      </div>
      ${c.err && html`<div class="err small" style="margin-top:6px">${c.err}</div>`}
      ${recv.length > 0 && html`<ul class="events">
        ${recv.map((r) => html`<li key=${`${r.partition}-${r.offset}-${r.redelivered}`}>
          <b>P${r.partition}</b>@${r.offset} <span>${r.key || "∅"}</span><span class="muted">${r.value}</span>
          ${r.redelivered ? html`<span class="chip warn">đọc lại</span>` : ""}
        </li>`)}
      </ul>`}
    </div>`;
}

render(html`<${App} />`, document.getElementById("app"));
