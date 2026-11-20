"use strict";

const MODULES = [
  ["M0", "Cơ bản"], ["M1", "Producer"], ["M2", "Consumer group"], ["M3", "Xử lý lỗi của service"],
  ["M4", "Offset & thời gian"], ["M5", "Cluster"], ["M6", "Transaction & compaction"],
];

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const short = (name) => String(name || "").replace(/^lab-gap-/, "").replace(/^lab-/, "");
const fmt = (n) => (n === undefined || n === null ? "–" : Number(n).toLocaleString("en-US"));

let scenarios = [];
let current = null;
const steps = {};      // scenario id -> [{step, status, observed, err}]
const finished = {};   // scenario id -> Result
let lastContainers = "";

function store(key, value) { try { localStorage.setItem(key, value); } catch (_) {} }
function load(key) { try { return localStorage.getItem(key) || ""; } catch (_) { return ""; } }

async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(`${r.status}: ${(await r.text()).trim()}`);
  return r;
}

function log(text, at) {
  const li = document.createElement("li");
  const t = at ? new Date(at) : new Date();
  li.innerHTML = `<time>${t.toLocaleTimeString("vi-VN")}</time>${esc(text)}`;
  const ol = $("log");
  ol.prepend(li);
  while (ol.children.length > 200) ol.lastChild.remove();
}

// Navigation

function renderNav() {
  const nav = $("nav");
  let html = "";
  for (const [id, name] of MODULES) {
    html += `<h4>${id} · ${esc(name)}</h4>`;
    const list = scenarios.filter((s) => s.module === id);
    if (list.length === 0) html += `<div class="soon">sắp có</div>`;
    for (const s of list) html += `<a href="#sc/${esc(s.id)}" data-id="${esc(s.id)}">${esc(s.id)} ${esc(s.title)}</a>`;
  }
  html += `<h4>Khác</h4><a href="#free" data-view="free">Free play</a><a href="#gaps" data-view="gaps">Chưa cover (20%)</a>`;
  nav.innerHTML = html;
}

function route() {
  const h = location.hash.slice(1);
  for (const v of ["scenario", "free", "gaps"]) $("view-" + v).hidden = true;
  document.querySelectorAll("nav a").forEach((a) => a.classList.remove("active"));
  if (h.startsWith("sc/")) {
    const id = h.slice(3);
    const s = scenarios.find((x) => x.id === id);
    if (s) {
      showScenario(s);
      document.querySelector(`nav a[data-id="${CSS.escape(id)}"]`)?.classList.add("active");
      return;
    }
  }
  const view = h === "gaps" ? "gaps" : "free";
  $("view-" + view).hidden = false;
  document.querySelector(`nav a[data-view="${view}"]`)?.classList.add("active");
}

// Scenario card

function showScenario(s) {
  current = s;
  $("view-scenario").hidden = false;
  $("sc-title").textContent = `${s.id} · ${s.title}`;
  $("sc-question").textContent = s.question;
  $("sc-guess").value = load("guess-" + s.id);
  $("sc-answer").hidden = true;
  renderSteps();
  renderRunState();
}

function renderSteps() {
  if (!current) return;
  $("sc-steps").innerHTML = (steps[current.id] || []).map((st) =>
    `<li class="${esc(st.status)}">${esc(st.step)}` +
    (st.observed ? ` <span class="obs">— ${esc(st.observed)}</span>` : "") +
    (st.err ? `<div class="err">${esc(st.err)}</div>` : "") + `</li>`).join("");
}

function renderRunState() {
  const res = finished[current.id];
  $("sc-reveal").disabled = !res;
  $("sc-status").textContent = res ? (res.passed ? "Kịch bản PASS: hệ thống làm đúng như giải thích." : "Kịch bản FAIL: xem bước bị ✗.") : "";
}

function reveal() {
  const s = current;
  $("sc-reveal-body").innerHTML = s.reveal;
  $("sc-props").textContent = (s.javaProps || []).join("\n") || "—";
  $("sc-cli").textContent = (s.cli || []).join("\n\n") || "—";
  $("sc-confirm").innerHTML = (s.confirm || []).map((q) => `<li>${esc(q)}</li>`).join("");
  $("sc-answer").hidden = false;
}

async function runScenario() {
  const id = current.id;
  steps[id] = [];
  delete finished[id];
  renderSteps();
  renderRunState();
  try {
    await api(`/api/scenarios/${encodeURIComponent(id)}/run`, { method: "POST" });
    $("sc-status").textContent = "Đang chạy…";
  } catch (e) {
    $("sc-status").textContent = e.message.startsWith("409") ? "Đang có kịch bản hoặc reset khác chạy." : e.message;
  }
}

function onStep(ev) {
  const list = (steps[ev.scenario] ||= []);
  const existing = list.find((x) => x.step === ev.step);
  if (existing) Object.assign(existing, ev); else list.push({ ...ev });
  if (current && current.id === ev.scenario) renderSteps();
}

function onScenario(res) {
  if (res.id === "reset") {
    log(res.passed ? "reset xong" : `reset lỗi: ${res.steps?.[0]?.err || ""}`);
    return;
  }
  finished[res.id] = res;
  log(`kịch bản ${res.id}: ${res.passed ? "PASS" : "FAIL"}`);
  if (current && current.id === res.id) renderRunState();
}

// Live panel

function renderLive(s) {
  $("errors").innerHTML = Object.entries(s.errors || {}).map(([k, v]) => `<div><b>${esc(k)}</b>: ${esc(v)}</div>`).join("");

  const names = Object.keys(s.containers || {}).sort();
  $("containers").innerHTML = names.map((n) => `<span class="chip st-${esc(s.containers[n])}" title="${esc(s.containers[n])}">${esc(short(n))}</span>`).join("");

  const g = s.group || {};
  $("group-name").textContent = g.name || "";
  $("group-state").textContent = g.state || "?";
  $("group-state").className = "pill s-" + (g.state || "");

  const owner = {};
  for (const inst of s.instances || []) for (const p of inst.partitions || []) owner[p] = short(inst.name);
  const lags = Object.fromEntries((g.lags || []).map((l) => [l.partition, l]));
  $("partitions").innerHTML = `<tr><th>P</th><th>Leader</th><th>ISR</th><th class="num">End</th><th class="num">Committed</th><th class="num">Lag</th><th>Owner</th></tr>` +
    (s.partitions || []).map((p) => {
      const l = lags[p.partition] || {};
      const lag = l.lag ?? 0;
      return `<tr><td>${p.partition}</td><td>${p.leader < 0 ? '<span class="s-PAUSED">none</span>' : "k" + p.leader}</td>` +
        `<td>${(p.isr || []).join(",")}</td><td class="num">${fmt(p.end)}</td>` +
        `<td class="num">${l.committed === undefined ? "–" : l.committed < 0 ? "none" : fmt(l.committed)}</td>` +
        `<td class="num ${lag > 1000 ? "lag-hot" : ""}">${fmt(lag)}</td><td>${esc(owner[p.partition] || "—")}</td></tr>`;
    }).join("");

  const t = s.tracking || {};
  const tlag = (t.lags || []).reduce((a, l) => a + (l.lag || 0), 0);
  $("tracking").textContent = t.name ? `Group thứ hai trên cùng topic: ${t.name} · ${t.state || "?"} · lag ${fmt(tlag)}` : "";

  $("instances").innerHTML = `<tr><th>Instance</th><th>Container</th><th>Pipeline</th><th class="num">Polled</th><th class="num">To processor</th><th class="num">Dropped</th><th class="num">Failed</th><th>P</th></tr>` +
    (s.instances || []).map((i) => {
      const failed = Object.entries(i.failed || {});
      const failedSum = failed.reduce((a, [, v]) => a + v, 0);
      return `<tr><td>${esc(short(i.name))}</td><td class="st-${esc(i.container)}">${esc(i.container)}</td>` +
        `<td class="s-${esc(i.state)}">${esc(i.state || "—")}</td><td class="num">${fmt(i.polled)}</td>` +
        `<td class="num">${fmt(i.accepted)}</td><td class="num">${fmt(i.filteredDrop)}</td>` +
        `<td class="num" title="${esc(failed.map(([k, v]) => `${k}: ${v}`).join("\n"))}">${fmt(failedSum)}</td>` +
        `<td>${(i.partitions || []).join(",")}</td></tr>`;
    }).join("");

  $("db-rows").textContent = fmt(s.dbRows);
  $("late-rows").textContent = fmt(s.lateRows);
  $("dlq-end").textContent = fmt(s.dlqEnd);
  renderControls(s.containers || {});
}

function renderControls(containers) {
  const key = JSON.stringify(containers);
  if (key === lastContainers) return;
  lastContainers = key;
  const actions = ["stop", "start", "kill", "pause", "unpause"];
  $("containers-ctl").innerHTML = Object.keys(containers).sort().map((n) => {
    const net = n.startsWith("lab-gap-svc") ? ["disconnect", "connect"] : [];
    return `<tr><td>${esc(n)}</td><td class="st-${esc(containers[n])}">${esc(containers[n])}</td><td>` +
      [...actions, ...net].map((a) => `<button class="small" data-action="${a}" data-target="${esc(n)}">${a}</button>`).join("") + `</td></tr>`;
  }).join("");
}

async function chaos(action, target) {
  try {
    await api("/api/chaos", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, target }) });
  } catch (e) {
    log(`chaos ${action} ${target} lỗi: ${e.message}`);
  }
}

async function produce(form) {
  const f = new FormData(form);
  const body = {
    count: +f.get("count"), sensors: +f.get("sensors"), ratePerSec: +f.get("ratePerSec"),
    acks: f.get("acks"), idempotent: f.get("idempotent") === "on", poisonEvery: +f.get("poisonEvery"),
  };
  try {
    await api("/api/produce", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  } catch (e) {
    log(`produce lỗi: ${e.message}`);
  }
}

// Wiring

function connect() {
  const es = new EventSource("/api/events");
  es.onopen = () => { $("conn").textContent = "live"; $("conn").className = "pill s-RUNNING"; };
  es.onerror = () => { $("conn").textContent = "mất kết nối, đang thử lại…"; $("conn").className = "pill s-PAUSED"; };
  es.addEventListener("snapshot", (e) => renderLive(JSON.parse(e.data).data));
  es.addEventListener("log", (e) => { const ev = JSON.parse(e.data); log(ev.data, ev.at); });
  es.addEventListener("step", (e) => onStep(JSON.parse(e.data).data));
  es.addEventListener("scenario", (e) => onScenario(JSON.parse(e.data).data));
}

async function init() {
  $("sc-run").onclick = runScenario;
  $("sc-reveal").onclick = reveal;
  $("sc-skip").onclick = (e) => { e.preventDefault(); reveal(); };
  $("sc-guess").oninput = (e) => current && store("guess-" + current.id, e.target.value);
  $("reset-btn").onclick = async () => {
    try { await api("/api/reset", { method: "POST" }); log("reset bắt đầu…"); }
    catch (e) { log(e.message.startsWith("409") ? "đang có kịch bản chạy, chưa reset được" : e.message); }
  };
  $("containers-ctl").onclick = (e) => {
    const b = e.target.closest("button[data-action]");
    if (b) chaos(b.dataset.action, b.dataset.target);
  };
  $("produce").onsubmit = (e) => { e.preventDefault(); produce(e.target); };
  window.onhashchange = route;
  try {
    scenarios = await (await api("/api/scenarios")).json();
  } catch (e) {
    log(`không tải được danh sách kịch bản: ${e.message}`);
  }
  renderNav();
  route();
  connect();
  api("/api/snapshot").then((r) => r.json()).then(renderLive).catch(() => {});
}

init();
