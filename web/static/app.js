"use strict";

const MODULES = [
  ["M0", "Cơ bản"], ["M1", "Producer"], ["M2", "Consumer group"], ["M3", "Xử lý lỗi của service"],
  ["M4", "Offset & thời gian"], ["M5", "Cluster"], ["M6", "Transaction & compaction"],
];

const $ = (id) => document.getElementById(id);
const JSON_HEADERS = { "Content-Type": "application/json" };
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const short = (name) => String(name || "").replace(/^lab-gap-/, "").replace(/^lab-/, "");
const fmt = (n) => (n === undefined || n === null ? "–" : Number(n).toLocaleString("en-US"));

let scenarios = [];
let current = null;
let running = null;    // id of the scenario this page started and has not seen finish
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
  const soon = [];
  for (const [id, name] of MODULES) {
    const list = scenarios.filter((s) => s.module === id);
    if (list.length === 0) { soon.push(`${id} ${name}`); continue; }
    html += `<h4>${id} · ${esc(name)}</h4>`;
    for (const s of list) html += `<a href="#sc/${esc(s.id)}" data-id="${esc(s.id)}">${esc(s.id)} ${esc(s.title)}</a>`;
  }
  html += `<h4>Khác</h4><a href="#free" data-view="free">Free play</a><a href="#gaps" data-view="gaps">Chưa cover (20%)</a>`;
  if (soon.length) html += `<p class="soon">Sắp có: ${esc(soon.join(", "))}</p>`;
  nav.innerHTML = html;
}

function route() {
  // Coming from the lessons page there is no hash: reopen the last scenario, or the first one.
  if (!location.hash && scenarios.length) {
    history.replaceState(null, "", load("scn-last") || `#sc/${scenarios[0].id}`);
  }
  const h = location.hash.slice(1);
  for (const v of ["scenario", "free", "gaps"]) $("view-" + v).hidden = true;
  document.querySelectorAll("#nav a").forEach((a) => a.removeAttribute("aria-current"));
  if (h.startsWith("sc/")) {
    const id = h.slice(3);
    const s = scenarios.find((x) => x.id === id);
    if (s) {
      showScenario(s);
      store("scn-last", "#" + h);
      document.querySelector(`#nav a[data-id="${CSS.escape(id)}"]`)?.setAttribute("aria-current", "page");
      return;
    }
  }
  const view = h === "gaps" ? "gaps" : "free";
  $("view-" + view).hidden = false;
  document.querySelector(`#nav a[data-view="${view}"]`)?.setAttribute("aria-current", "page");
}

// Scenario card

function showScenario(s) {
  current = s;
  $("view-scenario").hidden = false;
  $("sc-title").textContent = `${s.id} · ${s.title}`;
  $("sc-question").textContent = s.question;
  $("sc-guess").value = load("guess-" + s.id);
  $("sc-answer").hidden = true;
  $("sc-gate").hidden = false;
  renderSteps();
  renderRunState();
}

function renderSteps() {
  if (!current) return;
  $("sc-steps").innerHTML = (steps[current.id] || []).map((st) =>
    `<li class="${esc(st.status)}">${esc(st.step)}` +
    (st.observed ? `<div class="obs">${esc(st.observed)}</div>` : "") +
    (st.err ? `<div class="err">${esc(st.err)}</div>` : "") + `</li>`).join("");
}

function renderRunState() {
  if (!current) return;
  const res = finished[current.id];
  const btn = $("sc-run");
  // The lab runs one scenario at a time; a second press would only come back as 409.
  btn.disabled = Boolean(running);
  btn.setAttribute("aria-busy", running === current.id ? "true" : "false");
  btn.innerHTML = running === current.id ? `<span class="spin"></span>Đang chạy…` : "Chạy kịch bản";
  $("sc-reveal").disabled = !res;
  const st = $("sc-status");
  st.className = "status-text" + (res ? (res.passed ? " pass" : " fail") : "");
  st.textContent = res ? (res.passed ? "PASS: hệ thống làm đúng như giải thích." : "FAIL: xem bước bị ✕.")
    : running && running !== current.id ? `Kịch bản ${running} đang chạy.` : "";
}

function reveal() {
  const s = current;
  $("sc-gate").hidden = true;
  $("sc-reveal-body").innerHTML = s.reveal;
  $("sc-props").textContent = (s.javaProps || []).join("\n") || "—";
  $("sc-cli").textContent = (s.cli || []).join("\n\n") || "—";
  $("sc-confirm").innerHTML = (s.confirm || []).map((q) => `<li>${esc(q)}</li>`).join("");
  $("sc-answer").hidden = false;
}

async function runScenario() {
  if (!current || running) return;
  const id = current.id;
  steps[id] = [];
  delete finished[id];
  running = id;
  renderSteps();
  renderRunState();
  try {
    await api(`/api/scenarios/${encodeURIComponent(id)}/run`, { method: "POST", headers: JSON_HEADERS });
  } catch (e) {
    running = null;
    renderRunState();
    $("sc-status").className = "status-text fail";
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
  if (running === res.id) running = null;
  log(`kịch bản ${res.id}: ${res.passed ? "PASS" : "FAIL"}`);
  renderRunState();
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

  $("instances").innerHTML = `<tr><th>Instance</th><th>State</th><th class="num">Polled</th><th class="num" title="records_accepted_total: handed to the processor">To proc.</th><th class="num" title="dropped by the interval filter">Drop</th><th class="num">Failed</th><th>P</th></tr>` +
    (s.instances || []).map((i) => {
      const failed = Object.entries(i.failed || {});
      const failedSum = failed.reduce((a, [, v]) => a + v, 0);
      // A stopped or paused container has no pipeline state; show the container state instead.
      const state = i.container === "running" ? `<td class="s-${esc(i.state)}">${esc(i.state || "—")}</td>`
        : `<td class="st-${esc(i.container)}">container ${esc(i.container)}</td>`;
      return `<tr><td>${esc(short(i.name))}</td>${state}<td class="num">${fmt(i.polled)}</td>` +
        `<td class="num">${fmt(i.accepted)}</td><td class="num">${fmt(i.filteredDrop)}</td>` +
        `<td class="num" title="${esc(failed.map(([k, v]) => `${k}: ${v}`).join("\n"))}">${fmt(failedSum)}</td>` +
        `<td>${(i.partitions || []).join(",")}</td></tr>`;
    }).join("");

  // While the DB is unreachable the counts are unknown, not zero.
  const dbDown = Boolean((s.errors || {}).db);
  $("db-rows").textContent = dbDown ? "?" : fmt(s.dbRows);
  $("late-rows").textContent = dbDown ? "?" : fmt(s.lateRows);
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
    return `<tr><td class="mono">${esc(n)}</td><td class="st-${esc(containers[n])}">${esc(containers[n])}</td><td>` +
      [...actions, ...net].map((a) => `<button class="btn sm${a === "kill" ? " danger" : ""}" data-action="${a}" data-target="${esc(n)}">${a}</button>`).join("") + `</td></tr>`;
  }).join("");
}

// busy disables a button while its request runs and shows a spinner in it.
function busy(btn, on) {
  if (!btn) return;
  if (on) { btn.dataset.label = btn.innerHTML; btn.innerHTML = `<span class="spin"></span>${btn.innerHTML}`; }
  else if (btn.dataset.label !== undefined) { btn.innerHTML = btn.dataset.label; delete btn.dataset.label; }
  btn.disabled = on;
  btn.setAttribute("aria-busy", on ? "true" : "false");
}

function showErr(id, msg) {
  $(id).hidden = !msg;
  $(id).textContent = msg || "";
}

async function chaos(action, target, btn) {
  busy(btn, true);
  showErr("ctl-err", "");
  try {
    await api("/api/chaos", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, target }) });
  } catch (e) {
    log(`chaos ${action} ${target} lỗi: ${e.message}`);
    showErr("ctl-err", `${action} ${target}: ${e.message}`);
  }
  busy(btn, false);
}

async function produce(form) {
  const f = new FormData(form);
  const body = {
    count: +f.get("count"), sensors: +f.get("sensors"), ratePerSec: +f.get("ratePerSec"),
    acks: f.get("acks"), idempotent: f.get("idempotent") === "on", poisonEvery: +f.get("poisonEvery"),
  };
  busy($("produce-btn"), true);
  showErr("produce-err", "");
  try {
    await api("/api/produce", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  } catch (e) {
    log(`produce lỗi: ${e.message}`);
    showErr("produce-err", e.message);
  }
  busy($("produce-btn"), false);
}

// Actions that lose data arm on the first press and run on the second; they disarm after 4 s, on blur or on Esc.
function confirmTwice(btn, armedText, action) {
  const label = btn.innerHTML;
  let timer = 0;
  const disarm = () => { clearTimeout(timer); btn.classList.remove("armed"); btn.innerHTML = label; };
  btn.onclick = () => {
    if (!btn.classList.contains("armed")) {
      btn.classList.add("armed");
      btn.textContent = armedText;
      timer = setTimeout(disarm, 4000);
      return;
    }
    disarm();
    action();
  };
  btn.onblur = disarm;
  btn.onkeydown = (e) => { if (e.key === "Escape") disarm(); };
}

// Wiring

async function loadScenarios() {
  try {
    scenarios = await (await api("/api/scenarios")).json();
  } catch (e) {
    log(`không tải được danh sách kịch bản: ${e.message}`);
  }
  renderNav();
  route();
}

function connect() {
  const es = new EventSource("/api/events");
  // A reconnect usually means lab-control restarted, possibly with new scenarios.
  es.onopen = () => { $("conn").textContent = "Đang cập nhật"; $("conn-dot").className = "dot ok"; loadScenarios(); };
  es.onerror = () => { $("conn").textContent = "Mất kết nối, đang thử lại…"; $("conn-dot").className = "dot bad"; };
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
  // Reset recreates the topics and truncates the table, so it is confirmed like any other data loss.
  confirmTwice($("reset-btn"), "Bấm lần nữa để reset", async () => {
    busy($("reset-btn"), true);
    try { await api("/api/reset", { method: "POST", headers: JSON_HEADERS }); log("reset bắt đầu…"); }
    catch (e) { log(e.message.startsWith("409") ? "đang có kịch bản chạy, chưa reset được" : e.message); }
    busy($("reset-btn"), false);
  });
  $("containers-ctl").onclick = (e) => {
    const b = e.target.closest("button[data-action]");
    if (b && !b.disabled) chaos(b.dataset.action, b.dataset.target, b);
  };
  $("produce").onsubmit = (e) => { e.preventDefault(); produce(e.target); };
  window.onhashchange = route;
  connect();
  api("/api/snapshot").then((r) => r.json()).then(renderLive).catch(() => {});
}

init();
