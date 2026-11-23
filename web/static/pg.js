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
const clock = () => new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });

// Lucide-style strokes; every icon here is decorative next to a text label or carries an aria-label on its button.
const ICONS = {
  plus: ["M5 12h14", "M12 5v14"],
  trash: ["M3 6h18", "M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6", "M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"],
  x: ["M18 6 6 18", "M6 6l12 12"],
  check: ["M20 6 9 17l-5-5"],
  left: ["m15 18-6-6 6-6"],
  right: ["m9 18 6-6-6-6"],
  up: ["m18 15-6-6-6 6"],
  down: ["m6 9 6 6 6-6"],
  copy: ["M9 9h11v11H9z", "M5 15H4V4h11v1"],
  zap: ["M13 2 3 14h9l-1 8 10-12h-9l1-8z"],
  stop: ["M6 6h12v12H6z"],
  play: ["M7 4l13 8-13 8z"],
  alert: ["M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z", "M12 9v4", "M12 17h.01"],
  terminal: ["m4 17 6-6-6-6", "M12 19h8"],
  send: ["m22 2-7 20-4-9-9-4z", "M22 2 11 13"],
  inbox: ["M12 3v11", "m7 9 5 5 5-5", "M4 15v4a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-4"],
  repeat: ["m17 2 4 4-4 4", "M3 11v-1a4 4 0 0 1 4-4h14", "m7 22-4-4 4-4", "M21 13v1a4 4 0 0 1-4 4H3"],
  layers: ["m12 2 10 5-10 5L2 7z", "m2 17 10 5 10-5", "m2 12 10 5 10-5"],
  ext: ["M15 3h6v6", "M10 14 21 3", "M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"],
  lock: ["M5 11h14v10H5z", "M8 11V7a4 4 0 0 1 8 0v4"],
  eraser: ["m7 21-4.3-4.3a1 1 0 0 1 0-1.4l9.9-9.9a1 1 0 0 1 1.4 0l5.6 5.6a1 1 0 0 1 0 1.4L13 21", "M22 21H7", "m5 11 9 9"],
};
const Icon = ({ name, size = 14 }) => html`
  <svg class="i" width=${size} height=${size} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
    stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[name].map((d) => html`<path d=${d} />`)}</svg>`;

// One request at a time per control group: its buttons show which one runs and cannot be pressed twice.
function useAction(act) {
  const [s, setS] = useState({ busy: "", err: "" });
  const running = useRef("");
  const run = async (key, label, method, path, body, describe) => {
    if (running.current) return null;
    running.current = key;
    setS({ busy: key, err: "" });
    let err = "";
    const res = await act(label, method, path, body, describe, (m) => { err = m; });
    running.current = "";
    setS({ busy: "", err });
    return res;
  };
  return { ...s, run };
}

const Lead = ({ busy, icon, size = 13 }) => (busy ? html`<span class="spin"></span>` : icon ? html`<${Icon} name=${icon} size=${size} />` : null);
const Err = ({ msg }) => msg ? html`<div class="inline-err" role="alert"><${Icon} name="alert" size=${13} /><span>${msg}</span></div>` : null;

// Actions that lose data arm on the first press and run on the second; they disarm after 4 s, on blur or on Esc.
function ConfirmButton({ cls, icon, label, armedLabel, title, ariaLabel, busy, disabled, onConfirm }) {
  const [armed, setArmed] = useState(false);
  useEffect(() => {
    if (!armed) return undefined;
    const t = setTimeout(() => setArmed(false), 4000);
    return () => clearTimeout(t);
  }, [armed]);
  const press = () => {
    if (!armed) { setArmed(true); return; }
    setArmed(false);
    onConfirm();
  };
  return html`<button class=${cls + (armed ? " armed" : "")} disabled=${disabled || busy} aria-busy=${busy ? "true" : undefined}
    title=${title} aria-label=${armed ? armedLabel : ariaLabel || label} onClick=${press} onBlur=${() => setArmed(false)}
    onKeyDown=${(e) => { if (e.key === "Escape") setArmed(false); }}>
    <${Lead} busy=${busy} icon=${icon} />${armed ? html`<span>${armedLabel}</span>` : label && html`<span class="lbl">${label}</span>`}</button>`;
}

// ---------------------------------------------------------------- app

const MODES = ["lessons", "free"];

function App() {
  // The scenarios page links here with #lessons or #free; otherwise the page reopens where the learner left it.
  const [mode, setMode] = useState(() => {
    const h = location.hash.slice(1);
    return MODES.includes(h) ? h : store.get("pg-mode", "lessons");
  });
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
  const lastGood = useRef({});
  const topicRef = useRef(topic);
  topicRef.current = topic;

  const note = (text, err = false) => setLog((l) => [{ id: Date.now() + Math.random(), t: clock(), text, err }, ...l].slice(0, 100));

  // act runs one user action, logs it and shows the equivalent CLI command the server sent back;
  // describe, when given, turns the response into the log line; onErr shows a failure next to the control.
  const act = async (label, method, path, body, describe, onErr) => {
    try {
      const res = await api(method, path, body);
      if (res.cli) setCli(res.cli);
      note(describe ? describe(res) : label);
      poke();
      return res;
    } catch (e) {
      note(`${label}: ${e.message}`, true);
      if (onErr) onErr(e.message);
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

  useEffect(() => {
    store.set("pg-mode", mode);
    if (location.hash !== `#${mode}`) history.replaceState(null, "", `#${mode}`);
  }, [mode]);
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

  const ctx = { st, topic, setTopic, act };
  return html`
    <div class="app">
      <${Header} mode=${mode} setMode=${setMode} live=${live} act=${act} />
      <main class=${mode}>
        ${mode === "lessons" && html`<aside class="col lesson"><${Safe} name="bài học"><${LessonPanel} lessons=${lessons}
          statuses=${(st && st.lessons) || []} id=${lessonId} setId=${setLessonId} /><//></aside>`}
        <div class="work">
          <section class="col stage"><${Safe} name="cluster"><${Stage} ...${ctx} /><//></section>
          <aside class="col side"><${Safe} name="producer/consumer"><${Clients} ...${ctx} /><//></aside>
        </div>
      </main>
      <${Console} cli=${cli} log=${log} />
    </div>`;
}

// Safe keeps one broken panel from blanking the whole page.
function Safe({ name, children }) {
  const [err, reset] = useErrorBoundary();
  if (err) {
    return html`<div class="banner"><${Icon} name="alert" /><span>Panel ${name} gặp lỗi hiển thị: ${String((err && err.message) || err)}</span>
      <button class="btn sm" onClick=${reset}>Vẽ lại</button></div>`;
  }
  return children;
}

function Header({ mode, setMode, live, act }) {
  const a = useAction(act);
  const clear = () => {
    store.set("pg-done", "");
    a.run("clear", "dọn sân chơi", "POST", "/api/pg/clear");
  };
  const cur = (m) => (mode === m ? "page" : undefined);
  return html`
    <header class="top">
      <div class="brand"><span class="mark"><${Icon} name="layers" size=${14} /></span><span>Kafka Lab</span></div>
      <nav class="sections" aria-label="Phần">
        <button aria-current=${cur("lessons")} onClick=${() => setMode("lessons")}>Bài học</button>
        <button aria-current=${cur("free")} onClick=${() => setMode("free")}>Sân chơi</button>
        <a href="classic.html" title="Kịch bản chạy với service gap thật">Kịch bản</a>
      </nav>
      <span class="spacer"></span>
      <span class="status" role="status"><span class=${"dot " + (live ? "ok" : "bad")}></span><span>${live ? "Đang cập nhật" : "Mất kết nối với lab"}</span></span>
      <${ConfirmButton} cls="btn sm ghost" icon="eraser" label="Dọn sân chơi" armedLabel="Bấm lần nữa để xoá hết"
        title="Đóng mọi producer/consumer, xoá topic và group của sân chơi, và xoá tiến độ bài học" busy=${a.busy === "clear"} onConfirm=${clear} />
    </header>`;
}

function Copy({ text }) {
  const [done, setDone] = useState(false);
  const copy = async () => {
    try { await navigator.clipboard.writeText(text); setDone(true); setTimeout(() => setDone(false), 1200); } catch { /* clipboard blocked */ }
  };
  return html`<button class="btn icon sm ghost" onClick=${copy} aria-label="Chép lệnh" title=${done ? "Đã chép" : "Chép"}>
    <${Icon} name=${done ? "check" : "copy"} size=${13} /></button>`;
}

const Cli = ({ text }) => html`<div class="cli"><code>${text}</code><${Copy} text=${text} /></div>`;

function Console({ cli, log }) {
  const [open, setOpen] = useState(false);
  const last = log[0];
  return html`
    <footer class="console">
      <div class="console-bar">
        <span class="console-label"><${Icon} name="terminal" size=${13} />CLI tương đương</span>
        ${cli
          ? html`<code class="console-cmd" title=${cli}>${cli}</code><${Copy} text=${cli} />`
          : html`<span class="console-cmd empty">Mỗi thao tác sẽ hiện lệnh tương ứng ở đây, để dùng được ở prod.</span>`}
        <span class="vsep"></span>
        ${last
          ? html`<span key=${last.id} class=${"console-last fresh" + (last.err ? " err" : "")} role="status">
              ${last.err && html`<${Icon} name="alert" size=${13} />`}<span class="t">${last.t}</span><span class="txt">${last.text}</span></span>`
          : html`<span class="console-last muted">Chưa có thao tác nào.</span>`}
        <button class="btn sm ghost" onClick=${() => setOpen(!open)} aria-expanded=${open}>
          Nhật ký${log.length ? ` (${log.length})` : ""}<${Icon} name=${open ? "down" : "up"} size=${13} /></button>
      </div>
      ${open && html`<ul class="console-log">${log.length
        ? log.map((e) => html`<li key=${e.id} class=${e.err ? "err" : ""}><span class="t">${e.t}</span>${e.text}</li>`)
        : html`<li class="muted">Trống.</li>`}</ul>`}
    </footer>`;
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
  const idx = Math.max(0, lessons.findIndex((x) => x.id === id));
  const l = lessons[idx];
  if (!l) return html`<div class="lesson-body muted">Đang tải bài học…</div>`;
  const go = (i) => setId(lessons[i].id);
  return html`
    <div class="lesson-nav">
      <div class="row">
        <span class="eyebrow">Bài ${idx + 1} / ${lessons.length} · ${done} bài đã xong</span>
        <span class="spacer"></span>
        <button class="btn icon sm" disabled=${idx === 0} onClick=${() => go(idx - 1)} aria-label="Bài trước"><${Icon} name="left" /></button>
        <button class="btn icon sm" disabled=${idx === lessons.length - 1} onClick=${() => go(idx + 1)} aria-label="Bài sau"><${Icon} name="right" /></button>
      </div>
      <nav class="stepper" aria-label="Các bài học">
        ${lessons.map((x, i) => {
          const d = status[x.id] && status[x.id].done;
          const name = `Bài ${x.id}: ${x.title}${d ? " (đã xong)" : ""}`;
          return html`<button class=${d ? "done" : ""} aria-current=${i === idx ? "step" : undefined} title=${name} aria-label=${name}
            onClick=${() => go(i)}>${x.id}${d && html`<span class="tick"><${Icon} name="check" size=${10} /></span>`}</button>`;
        })}
      </nav>
    </div>
    <${Lesson} key=${l.id} l=${l} s=${status[l.id]} />`;
}

function Lesson({ l, s }) {
  const [guess, setGuess] = useState(store.get(`pg-predict-${l.id}`, ""));
  const [peek, setPeek] = useState(false);
  const revealRef = useRef(null);
  const done = s && s.done;
  const open = done || peek;
  const toReveal = () => { setPeek(true); setTimeout(() => revealRef.current && revealRef.current.scrollIntoView({ behavior: "smooth", block: "start" }), 0); };
  // The learning loop: each step is done once its evidence exists; the first one not done is where the learner is.
  const loop = [["Dự đoán", Boolean(guess.trim())], ["Làm", Boolean(done)], ["Quan sát", Boolean(done)], ["Giải thích", open]];
  const cur = loop.findIndex(([, d]) => !d);
  return html`
    <div class="lesson-body">
      <h1 class="lesson-title">${l.title}</h1>
      <ol class="loop" aria-label="Vòng học">
        ${loop.map(([name, d], i) => html`<li class=${d ? "done" : i === cur ? "cur" : ""} aria-current=${i === cur ? "step" : undefined}>${name}</li>`)}
      </ol>
      <section class="block">
        <h2 class="block-h"><span class="n">1</span>Dự đoán</h2>
        <p class="prose">${l.question}</p>
        <textarea aria-label="Dự đoán của bạn" placeholder="Ghi dự đoán trước khi làm. Chỉ lưu trên máy này." value=${guess}
          onInput=${(e) => { setGuess(e.target.value); store.set(`pg-predict-${l.id}`, e.target.value); }}></textarea>
      </section>
      <section class="block">
        <h2 class="block-h"><span class="n">2</span>Làm</h2>
        <ol class="todo prose">${l.steps.map((x) => html`<li>${x}</li>`)}</ol>
      </section>
      <section class="block" ref=${revealRef}>
        <h2 class="block-h"><span class="n">4</span>Giải thích</h2>
        ${open
          ? html`
            ${guess && html`<p class="quote">Bạn đã đoán: “${guess}”. Đúng chỗ nào, sai chỗ nào?</p>`}
            <div class="reveal prose" dangerouslySetInnerHTML=${{ __html: l.reveal }}></div>
            <div class="sub-h">Lệnh tương đương</div>
            ${l.cli.map((c) => html`<${Cli} text=${c} />`)}
            <div class="sub-h">Tự kiểm tra (không có đáp án)</div>
            <ul class="confirm prose">${l.confirm.map((q) => html`<li>${q}</li>`)}</ul>`
          : html`<div class="locked"><${Icon} name="lock" size=${13} />Mở khi bạn đạt mục tiêu.
              <button class="linkbtn" onClick=${toReveal}>Xem luôn</button></div>`}
      </section>
    </div>
    <div class=${"goal" + (done ? " done" : "")} role="status">
      <div class="goal-h">
        ${done ? html`<${Icon} name="check" size=${13} />3 · Quan sát: đạt mục tiêu` : html`<span class="pulse"></span>3 · Quan sát: lab đang theo dõi`}
        <span class="spacer"></span>
        ${done && !peek && html`<button class="btn sm" onClick=${toReveal}>Đọc giải thích</button>`}
      </div>
      <div class="goal-text">${l.goal}</div>
      <div class="goal-detail">${s ? s.detail : "…"}</div>
    </div>`;
}

// ---------------------------------------------------------------- cluster

const SOURCE = {
  topics: "danh sách topic là bản đọc lần trước",
  brokers: "trạng thái broker là bản đọc lần trước",
  groups: "cờ commit của group là bản đọc lần trước",
  topic: "chưa đọc được offset của một số partition (hiện “?”)",
};

function Stage({ st, topic, setTopic, act }) {
  const [creating, setCreating] = useState(false);
  const a = useAction(act);
  if (!st) return html`<div class="panel empty-state">Đang kết nối tới lab…</div>`;
  const topics = st.topics || [];
  const own = topics.filter((t) => !isScenarioTopic(t.name));
  const errs = Object.entries(st.errors || {}).filter(([k]) => k !== "lessonTopic");
  const del = () => a.run("del", `xoá topic ${topic}`, "DELETE", `/api/pg/topics/${encodeURIComponent(topic)}`);
  const tv = st.topic;
  return html`
    <div class="panel">
      <div class="panel-h"><span class="panel-t">Cluster</span><span class="panel-sub">3 broker · KRaft controller</span></div>
      <${Brokers} st=${st} act=${act} />
    </div>
    <div class="panel topic-panel">
      <div class="panel-h">
        ${own.length > 0 || topic
          ? html`<${TopicSelect} cls="select topic" topics=${topics.map((t) => t.name)} value=${topic} onChange=${setTopic} />`
          : html`<span class="panel-t">Topic</span>`}
        ${tv && html`<div class="meta"><span><b>${tv.partitions.length}</b> partition</span><span>RF <b>${tv.rf || "?"}</b></span>
          ${tv.minIsr > 0 && html`<span class="rf-extra">min ISR <b>${tv.minIsr}</b></span>`}</div>`}
        <span class="spacer"></span>
        ${topic && !isScenarioTopic(topic) && html`<${ConfirmButton} cls="btn icon danger" icon="trash" ariaLabel=${`Xoá topic ${topic}`}
          armedLabel=${`Xoá ${topic}? Mọi record sẽ mất`} title="Xoá topic" busy=${a.busy === "del"} onConfirm=${del} />`}
        <button class=${"btn" + (creating ? " on" : "")} aria-expanded=${creating} onClick=${() => setCreating(!creating)}><${Icon} name="plus" />Topic mới</button>
      </div>
      ${a.err && html`<div class="panel-err"><${Err} msg=${a.err} /></div>`}
      ${creating && html`<${CreateTopic} act=${act} cancel=${() => setCreating(false)} done=${(name) => { setCreating(false); setTopic(name); }} />`}
      ${errs.length > 0 && html`<div class="banner" title=${errs.map(([k, v]) => `${k}: ${v}`).join("\n")}><${Icon} name="alert" />
        <span>Có broker không trả lời: ${errs.map(([k]) => SOURCE[k] || k).join("; ")}. Rê chuột để xem lỗi gốc.</span></div>`}
      ${tv ? html`<${Log} st=${st} />`
        : topic ? html`<div class="empty-state">Đang đọc topic…</div>`
        : !creating && html`<div class="empty-state"><h3>Chưa có topic nào</h3><p>Bài 1 bắt đầu bằng việc tạo topic <code>orders</code>.</p>
            <button class="btn primary" onClick=${() => setCreating(true)}><${Icon} name="plus" />Tạo topic</button></div>`}
    </div>`;
}

function TopicSelect({ topics, value, onChange, cls = "", label = "Topic" }) {
  const own = topics.filter((t) => !isScenarioTopic(t));
  const scen = topics.filter(isScenarioTopic);
  return html`<select class=${cls} aria-label=${label} value=${value} onChange=${(e) => onChange(e.target.value)}>
    ${!value && html`<option value="">Chọn topic</option>`}
    ${own.map((t) => html`<option value=${t}>${t}</option>`)}
    ${scen.length > 0 && html`<optgroup label="Của kịch bản nâng cao (chỉ xem)">${scen.map((t) => html`<option value=${t}>${t}</option>`)}</optgroup>`}
  </select>`;
}

function Brokers({ st, act }) {
  const leads = {};
  for (const p of (st.topic && st.topic.partitions) || []) leads[p.leader] = (leads[p.leader] || 0) + 1;
  return html`
    <div class="brokers">
      ${(st.brokers || []).map((b) => html`<${Broker} key=${b.id} b=${b} leads=${leads[b.id]} act=${act} />`)}
    </div>`;
}

function Broker({ b, leads, act }) {
  const a = useAction(act);
  const up = b.state === "running";
  // Killed but not yet fenced: the controller still lists it until broker.session.timeout.ms passes.
  const limbo = !up && b.registered;
  const cls = up ? "" : limbo ? " limbo" : " down";
  const label = up ? (b.registered ? "Đang chạy" : "Đang khởi động, chưa đăng ký với controller")
    : limbo ? "Đã tắt, nhưng controller vẫn tưởng còn sống" : `Đã tắt (${b.state || "?"})`;
  const run = (verb) => a.run(verb, `docker ${verb} ${b.container}`, "POST", "/api/chaos", { action: verb, target: b.container });
  const btn = (verb, text, icon, cls, disabled, title) => html`
    <button class=${cls} disabled=${disabled || a.busy} aria-busy=${a.busy === verb ? "true" : undefined}
      aria-label=${`${text} broker ${b.id}`} title=${title || text} onClick=${() => run(verb)}>
      <${Lead} busy=${a.busy === verb} icon=${icon} size=${12} /><span class="lbl">${text}</span></button>`;
  return html`
    <div class=${"broker" + cls}>
      <div class="broker-top">
        <span class=${"dot " + (up ? "ok" : limbo ? "warn" : "bad")}></span>
        <span class="broker-name">Broker ${b.id}</span>
        <span class="spacer"></span>
        ${leads ? html`<span class="badge" title="Số partition của topic đang xem mà broker này làm leader">leader ×${leads}</span>` : ""}
      </div>
      <div class="broker-state" key=${label}>${label}</div>
      <div class="btngroup broker-actions">
        ${btn("kill", "Kill", "zap", "btn sm danger", !up, "Tắt đột ngột, như mất điện")}
        ${btn("stop", "Stop", "stop", "btn sm", !up, "Tắt có trật tự")}
        ${btn("start", "Start", "play", up ? "btn sm" : "btn sm primary", up)}
      </div>
      <${Err} msg=${a.err} />
    </div>`;
}

function CreateTopic({ act, done, cancel }) {
  const [f, setF] = useState({ name: LESSON_TOPIC, partitions: 3, rf: 3, minIsr: 2 });
  const a = useAction(act);
  const set = (k, num) => (e) => setF({ ...f, [k]: num ? Number(e.target.value) : e.target.value.trim() });
  const submit = async (e) => {
    e.preventDefault();
    if (await a.run("create", `tạo topic ${f.name}`, "POST", "/api/pg/topics", f)) done(f.name);
  };
  return html`
    <form class="create" onSubmit=${submit}>
      <label class="field">Tên<input value=${f.name} onInput=${set("name")} /></label>
      <label class="field">Partition<input type="number" min="1" max="12" value=${f.partitions} onInput=${set("partitions", true)} /></label>
      <label class="field">Replication factor<input type="number" min="1" max="3" value=${f.rf} onInput=${set("rf", true)} /></label>
      <label class="field">min.insync.replicas<input type="number" min="1" max=${f.rf} value=${f.minIsr} onInput=${set("minIsr", true)} /></label>
      <button type="button" class="btn" onClick=${cancel}>Huỷ</button>
      <button class="btn primary" disabled=${a.busy} aria-busy=${a.busy ? "true" : undefined}><${Lead} busy=${a.busy} />Tạo topic</button>
      <${Err} msg=${a.err} />
    </form>`;
}

function Log({ st }) {
  // Unknown docker state stays undefined, so nothing is drawn as dead without evidence.
  const brokerUp = Object.fromEntries((st.brokers || []).map((b) => [b.id, b.state ? b.state === "running" : undefined]));
  const groups = st.groups || [];
  const consumers = (st.consumers || []).filter((c) => c.topic === st.topic.name);
  return html`
    <div class="legend" aria-label="Chú giải">
      <span><span class="rep leader">1</span>leader</span>
      <span><span class="rep">2</span>trong ISR</span>
      <span><span class="rep out">3</span>ngoài ISR</span>
      <span><span class="rep dead-isr">2</span>đã tắt, vẫn trong ISR</span>
      <span><span class="rep dead">3</span>đã tắt, ra khỏi ISR</span>
      <span><span class="swatch commit"></span>committed offset của group</span>
      <span><span class="swatch pos"></span>consumer đã đọc tới (chưa chắc commit)</span>
    </div>
    ${st.topic.partitions.map((p) => html`<${Partition} key=${p.partition} p=${p} up=${brokerUp} groups=${groups} consumers=${consumers} />`)}`;
}

function Partition({ p, up, groups, consumers }) {
  const recs = p.records || [];
  // Keep the newest record in view as the log grows.
  const stripRef = useRef(null);
  const lastOffset = recs.length ? recs[recs.length - 1].offset : -1;
  const [clipped, setClipped] = useState(false);
  useEffect(() => {
    const el = stripRef.current;
    if (el) { el.scrollLeft = el.scrollWidth; setClipped(el.scrollLeft > 0); }
  }, [lastOffset, p.end]);
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
  const leaderUp = up[p.leader];
  return html`
    <div class="prow">
      <div class="pgut">
        <div class="pname"><b>P${p.partition}</b>
          <span>${p.leader < 0 ? html`<span class="badge bad">không có leader</span>` : `leader b${p.leader}${leaderUp === false ? " (đã tắt)" : ""}`}</span></div>
        <div class="reps" aria-label="Replica">
          ${p.replicas.map((r) => {
            const inIsr = p.isr.includes(r);
            const leader = r === p.leader;
            // The window lesson 7 is about: a dead broker still listed as leader or still in the ISR.
            const cls = up[r] === false
              ? leader ? "rep dead-leader" : inIsr ? "rep dead-isr" : "rep dead"
              : leader ? "rep leader" : inIsr ? "rep" : "rep out";
            const state = up[r] === false
              ? leader ? "đã tắt nhưng vẫn là leader: controller chưa chọn leader mới"
                : inIsr ? "đã tắt nhưng vẫn trong ISR: acks=all phải đợi nó" : "đã tắt, đã bị loại khỏi ISR"
              : leader ? "leader" : inIsr ? "trong ISR" : "ngoài ISR (đang đuổi theo leader)";
            return html`<span class=${cls} title=${`broker ${r} · ${state}`}>${r}</span>`;
          })}
        </div>
        <div class="pmeta">${known ? `${p.end - p.start} record · end ${p.end}` : "offset: ? (leader không trả lời)"}</div>
        ${owners.length > 0 && html`<div class="powner">${owners.map((c) => html`<span class="who" title=${`${c.name} trong group ${c.group} đang được gán partition này`}>${c.name}·${c.group}</span>`)}</div>`}
      </div>
      <div class=${"pstrip" + (clipped ? " clipped" : "")} ref=${stripRef}
        onScroll=${(e) => setClipped(e.currentTarget.scrollLeft > 0)}>
        <div class="logline">
          ${slots.length === 0 && html`<div class="cell empty">Chưa có record</div>`}
          ${slots.map((s) => html`
            <div class="slot" key=${s.kind + s.at}>
              ${s.kind === "more" && html`<div class="cell more">${hidden} record<br />trước</div>`}
              ${s.kind === "next" && html`<div class="cell next" title=${`Record kế tiếp ghi vào partition này sẽ có offset ${p.end}`}><small>kế tiếp</small>${p.end}</div>`}
              ${s.kind === "rec" && html`
                <div class=${"cell" + (s.r.key ? "" : " nokey")} style=${s.r.key ? `--h:${hue(s.r.key)}` : ""}
                  title=${`P${p.partition} offset ${s.r.offset}\nkey: ${s.r.key || "(không có)"}\nvalue: ${s.r.value}`}>
                  <span class="off">${s.r.offset}</span>
                  <span class="k">${s.r.key || "không key"}</span>
                  <span class="v">${s.r.value}</span>
                </div>`}
              ${(marks[s.at] || []).length > 0 && html`<div class="marks">${marks[s.at].map((m) => html`<span class=${"mark " + m.cls} title=${m.title}>${m.text}</span>`)}</div>`}
            </div>`)}
        </div>
      </div>
    </div>`;
}

// ---------------------------------------------------------------- producers and consumers

function nextName(prefix, taken) {
  for (let i = 1; ; i++) if (!taken.includes(prefix + i)) return prefix + i;
}

function Clients({ st, topic, act }) {
  const [addP, setAddP] = useState(false);
  const [addC, setAddC] = useState(false);
  if (!st) return null;
  // Sends and consumers stay on playground topics; scenario topics feed the gap service.
  const topics = (st.topics || []).map((t) => t.name).filter((t) => !isScenarioTopic(t));
  const current = isScenarioTopic(topic) ? topics[0] || "" : topic;
  const producers = st.producers || [];
  const consumers = st.consumers || [];
  return html`
    <section class="sec" aria-label="Producer">
      <div class="sec-h"><span class="sec-t">Producer</span><span class="count">${producers.length}</span><span class="spacer"></span>
        ${producers.length > 0 && html`<button class=${"btn sm" + (addP ? " on" : "")} aria-expanded=${addP} onClick=${() => setAddP(!addP)}><${Icon} name="plus" size=${13} />Thêm</button>`}</div>
      ${(addP || producers.length === 0) && html`<${AddProducer} act=${act} taken=${producers.map((p) => p.name)}
        close=${producers.length ? () => setAddP(false) : null} />`}
      ${producers.map((p) => html`<${Producer} key=${p.name} p=${p} topic=${current} topics=${topics} act=${act} />`)}
    </section>
    <section class="sec" aria-label="Consumer">
      <div class="sec-h"><span class="sec-t">Consumer</span><span class="count">${consumers.length}</span><span class="spacer"></span>
        ${consumers.length > 0 && html`<button class=${"btn sm" + (addC ? " on" : "")} aria-expanded=${addC} onClick=${() => setAddC(!addC)}><${Icon} name="plus" size=${13} />Thêm</button>`}</div>
      ${topics.length === 0 && consumers.length === 0 && html`<p class="hint">Consumer cần một topic để đọc. Tạo topic trước.</p>`}
      ${topics.length > 0 && (addC || consumers.length === 0) && html`<${AddConsumer} act=${act} topic=${current} topics=${topics}
        taken=${consumers.map((c) => c.name)} close=${consumers.length ? () => setAddC(false) : null} />`}
      <${Groups} st=${st} act=${act} />
    </section>`;
}

function AddProducer({ act, taken, close }) {
  const [acks, setAcks] = useState("all");
  const [idem, setIdem] = useState(true);
  const [custom, setCustom] = useState("");
  const a = useAction(act);
  const name = nextName("p", taken);
  const submit = async (e) => {
    e.preventDefault();
    const n = custom || name;
    const r = await a.run("create", `tạo producer ${n} (acks=${acks}, idempotence=${idem && acks === "all"})`, "POST", "/api/pg/producers",
      { name: n, acks, idempotent: idem && acks === "all" });
    if (r) { setCustom(""); close && close(); }
  };
  return html`
    <form class="addform" onSubmit=${submit}>
      <div class="grid2">
        <label class="field">Tên<input placeholder=${name} value=${custom} onInput=${(e) => setCustom(e.target.value.trim())} /></label>
        <label class="field">acks<select value=${acks} onChange=${(e) => setAcks(e.target.value)}>
          <option value="all">all</option><option value="1">1</option><option value="0">0</option></select></label>
        <label class="check full"><input type="checkbox" checked=${idem && acks === "all"} disabled=${acks !== "all"} onChange=${(e) => setIdem(e.target.checked)} />
          <span><code>enable.idempotence</code> <span class="muted small">(chỉ dùng được với acks=all)</span></span></label>
      </div>
      <${Err} msg=${a.err} />
      <div class="actions">
        ${close && html`<button type="button" class="btn sm" onClick=${close}>Huỷ</button>`}
        <button class="btn sm primary" disabled=${a.busy} aria-busy=${a.busy ? "true" : undefined}><${Lead} busy=${a.busy} icon="plus" />Tạo producer</button>
      </div>
    </form>`;
}

function Producer({ p, topic, topics, act }) {
  const [f, setF] = useState({ topic: "", key: "", value: "v{i}", count: 1 });
  const t = topics.includes(f.topic) ? f.topic : topic;
  const prop = (k) => (p.javaProps.find((x) => x.startsWith(k + "=")) || "").slice(k.length + 1);
  const acks = prop("acks") || "all";
  const summary = [`acks=${acks}`, prop("enable.idempotence") === "true" ? "idempotent" : "không idempotent",
    prop("delivery.timeout.ms") && `${Number(prop("delivery.timeout.ms")) / 1000}s`].filter(Boolean).join(" · ");
  const a = useAction(act);
  const send = (e) => {
    e.preventDefault();
    a.run("send", `${p.name} gửi ${f.count} record vào ${t}${f.key ? ` (key ${f.key})` : " (không key)"}`, "POST",
      `/api/pg/producers/${encodeURIComponent(p.name)}/send`, { topic: t, key: f.key, value: f.value, count: f.count, acks });
  };
  const sends = [...(p.sends || [])].reverse().slice(0, 6);
  return html`
    <div class="item">
      <div class="item-h">
        <span class="item-name">${p.name}</span>
        <span class="item-sub" title=${p.javaProps.join("\n")}>${summary}</span>
        <span class="spacer"></span>
        <button class="btn icon sm ghost danger" aria-label=${`Xoá producer ${p.name}`} title="Xoá producer" disabled=${a.busy}
          aria-busy=${a.busy === "del" ? "true" : undefined}
          onClick=${() => a.run("del", `xoá producer ${p.name}`, "DELETE", `/api/pg/producers/${encodeURIComponent(p.name)}`)}><${Lead} busy=${a.busy === "del"} icon="trash" /></button>
      </div>
      <form class="item-b sendform" onSubmit=${send}>
        <label class="field">Key<input placeholder="không key" value=${f.key} onInput=${(e) => setF({ ...f, key: e.target.value })} /></label>
        <label class="field">Value<input value=${f.value} onInput=${(e) => setF({ ...f, value: e.target.value })} /></label>
        <label class="field">Số lượng<input type="number" min="1" max="1000" value=${f.count} onInput=${(e) => setF({ ...f, count: Number(e.target.value) })} /></label>
        <label class="field wide">Topic<${TopicSelect} topics=${topics} value=${t} onChange=${(v) => setF({ ...f, topic: v })} /></label>
        <button class="btn primary" disabled=${!t || a.busy} aria-busy=${a.busy === "send" ? "true" : undefined}><${Lead} busy=${a.busy === "send"} icon="send" />Gửi</button>
        <${Err} msg=${a.err} />
        <div class="hint">Dùng <code>{i}</code> trong key hoặc value để đánh số 1, 2, 3… theo thứ tự record.</div>
      </form>
      ${sends.length > 0 && html`<ul class="events sends" aria-label="Lần gửi gần nhất">
        ${sends.map((s) => html`<li key=${s.id}>
          ${s.pending
            ? html`<span class="pending"><span class="spin"></span>${s.key || "không key"} đang chờ ack… ${ago(s.at).toFixed(1)} s</span>`
            : s.err
              ? html`<span class="err"><${Icon} name="x" size=${12} /> ${s.key || "không key"}: ${s.err}</span><span class="t">${secs(s.tookMs)}</span>`
              : html`<span>${s.key || "không key"}</span><span class="arrow">→</span><b>P${s.partition}</b><span class="muted">@${s.offset}</span>
                  ${s.brokersDown > 0 ? html`<span class="badge warn">${s.brokersDown} broker tắt</span>` : ""}
                  <span class="t">${secs(s.tookMs)}</span>`}
        </li>`)}
      </ul>`}
    </div>`;
}

function AddConsumer({ act, topic, topics, taken, close }) {
  const name = nextName("c", taken);
  const [f, setF] = useState({ name: "", group: "g1", topic: "", reset: "earliest" });
  const a = useAction(act);
  const t = topics.includes(f.topic) ? f.topic : topic;
  const submit = async (e) => {
    e.preventDefault();
    const n = f.name || name;
    const r = await a.run("create", `tạo consumer ${n} (group ${f.group}, ${t}, ${f.reset})`, "POST", "/api/pg/consumers",
      { name: n, group: f.group, topic: t, reset: f.reset });
    if (r) { setF({ ...f, name: "" }); close && close(); }
  };
  return html`
    <form class="addform" onSubmit=${submit}>
      <div class="grid2">
        <label class="field">Tên<input placeholder=${name} value=${f.name} onInput=${(e) => setF({ ...f, name: e.target.value.trim() })} /></label>
        <label class="field">group.id<input value=${f.group} onInput=${(e) => setF({ ...f, group: e.target.value.trim() })} /></label>
        <label class="field">Topic<${TopicSelect} topics=${topics} value=${t} onChange=${(v) => setF({ ...f, topic: v })} /></label>
        <label class="field" title="Dùng cho partition mà group chưa commit, hoặc có offset đã commit không còn trong log">auto.offset.reset
          <select value=${f.reset} onChange=${(e) => setF({ ...f, reset: e.target.value })}>
            <option value="earliest">earliest</option><option value="latest">latest</option></select></label>
      </div>
      <${Err} msg=${a.err} />
      <div class="actions">
        ${close && html`<button type="button" class="btn sm" onClick=${close}>Huỷ</button>`}
        <button class="btn sm primary" disabled=${!t || a.busy} aria-busy=${a.busy ? "true" : undefined}><${Lead} busy=${a.busy} icon="plus" />Tạo consumer</button>
      </div>
    </form>`;
}

function Groups({ st, act }) {
  const byGroup = {};
  for (const c of st.consumers || []) (byGroup[c.group] = byGroup[c.group] || []).push(c);
  const info = Object.fromEntries((st.groups || []).map((g) => [g.name, g]));
  const names = Object.keys(byGroup).sort();
  if (!names.length) return null;
  return names.map((g) => {
    const gi = info[g];
    const commits = gi && Object.keys(gi.committed || {}).length
      ? Object.entries(gi.committed).map(([p, o]) => `P${p}@${o}`).join(" ") : "chưa commit";
    return html`
      <div class="group" key=${g}>
        <div class="group-h">
          <span class="gname">${g}</span>
          ${gi && html`<span class="badge">${gi.state || "?"} · ${gi.members} member</span>`}
          <span class="spacer"></span>
          <span class="commits" title="Committed offset của group trên topic đang xem">▲ ${commits}</span>
        </div>
        ${byGroup[g].map((c) => html`<${Consumer} key=${c.name} c=${c} act=${act} />`)}
      </div>`;
  });
}

function Consumer({ c, act }) {
  const a = useAction(act);
  const path = `/api/pg/consumers/${encodeURIComponent(c.name)}`;
  const poll = () => a.run("poll", `${c.name} poll`, "POST", `${path}/poll`, { max: 10 }, (res) => {
    const recs = res.data || [];
    if (!recs.length) return `${c.name} poll: không nhận được record nào sau 3 s`;
    const again = recs.filter((x) => x.redelivered).length;
    return `${c.name} poll: nhận ${recs.length} record${again ? `, trong đó ${again} record đọc lại` : ""}`;
  });
  const busy = (k) => (a.busy === k ? "true" : undefined);
  const recv = [...(c.received || [])].reverse().slice(0, 4);
  const positions = Object.entries(c.positions || {}).map(([p, o]) => `P${p}@${o}`).join(" ");
  const meta = [c.topic, c.reset, `${c.polls} poll`, positions && `đọc tới ${positions}`, c.redeliveries && `${c.redeliveries} đọc lại`].filter(Boolean).join(" · ");
  return html`
    <div class="item">
      <div class="item-h">
        <span class="item-name">${c.name}</span>
        <span class="parts">${c.assigned && c.assigned.length
          ? c.assigned.map((p) => html`<span class="badge primary mono">P${p}</span>`)
          : html`<span class="badge warn">chưa có partition</span>`}</span>
        <span class="spacer"></span>
        <button class="btn icon sm ghost danger" aria-label=${`Xoá consumer ${c.name}`} title="Xoá consumer (rời group)" disabled=${a.busy}
          aria-busy=${busy("del")} onClick=${() => a.run("del", `xoá consumer ${c.name} (rời group ${c.group})`, "DELETE", path)}>
          <${Lead} busy=${a.busy === "del"} icon="trash" /></button>
      </div>
      <div class="consumer-meta">${meta}</div>
      <div class="consumer-actions">
        <button class="btn sm primary" disabled=${a.busy || c.auto} aria-busy=${busy("poll")} onClick=${poll}>
          <${Lead} busy=${a.busy === "poll"} icon="inbox" />${a.busy === "poll" ? "Đang poll" : "Poll"}</button>
        <button class="btn sm" disabled=${a.busy || c.auto} aria-busy=${busy("commit")}
          onClick=${() => a.run("commit", `${c.name} commit`, "POST", `${path}/commit`)}><${Lead} busy=${a.busy === "commit"} icon="check" />Commit</button>
        <button class=${"btn sm" + (c.auto ? " on" : "")} aria-pressed=${c.auto} disabled=${a.busy} aria-busy=${busy("auto")}
          title="Poll và commit liên tục, như một service"
          onClick=${() => a.run("auto", `${c.name} tự động: ${c.auto ? "tắt" : "bật"}`, "POST", `${path}/auto`, { on: !c.auto })}>
          <${Lead} busy=${a.busy === "auto"} icon="repeat" />${c.auto ? "Đang tự động" : "Tự động"}</button>
      </div>
      ${a.err && html`<${Err} msg=${a.err} />`}
      ${c.err && html`<div class="consumer-err">${c.err}</div>`}
      ${recv.length > 0 && html`<ul class="events" aria-label="Record nhận gần nhất">
        ${recv.map((r) => html`<li key=${`${r.partition}-${r.offset}-${r.redelivered}`}>
          <b>P${r.partition}</b><span class="muted">@${r.offset}</span><span>${r.key || "không key"}</span>
          <span class="val">${r.value}</span>
          ${r.redelivered ? html`<span class="t"><span class="badge warn">đọc lại</span></span>` : ""}
        </li>`)}
      </ul>`}
    </div>`;
}

render(html`<${App} />`, document.getElementById("app"));
