import { useEffect, useState } from "preact/hooks";
import { html, Cli, Safe } from "../shell.js";
import { api, safeStore } from "../api.js";
import { FlowMap } from "../flow.js";
import { serviceStatus, stepOf } from "../logic/stepper.js";

const MODULES = [
  ["M0", "Cơ bản"], ["M1", "Producer"], ["M2", "Consumer group"], ["M3", "Xử lý lỗi của service"],
  ["M4", "Offset & thời gian"], ["M5", "Cluster"], ["M6", "Transaction & compaction"],
];
const STEPS = [["predict", "1. Dự đoán"], ["run", "2. Chạy"], ["observe", "3. Quan sát"], ["explain", "4. Giải thích"]];
const CHAOS = ["stop", "start", "kill", "pause", "unpause"];
const short = (n) => String(n || "").replace(/^lab-gap-/, "").replace(/^lab-/, "");

function ScenarioList({ scenarios, id, setId }) {
  return html`
    <div class="panel scenario-list">
      <div class="caps">Failure scenarios</div>
      ${MODULES.map(([m, name]) => {
        const list = scenarios.filter((s) => s.module === m);
        return html`<div key=${m} class="module">
          <div class="module-name">${m} · ${name}</div>
          ${list.length === 0 && html`<div class="muted small soon">sắp có</div>`}
          ${list.map((s) => html`<button key=${s.id} class=${"lesson-item" + (s.id === id ? " on" : "")} onClick=${() => setId(s.id)}>
            <span class="mono muted">${s.id}</span><span class="grow ellipsis" title=${s.title}>${s.title}</span></button>`)}
        </div>`;
      })}
    </div>`;
}

const Stepper = ({ step }) => {
  const at = STEPS.findIndex(([k]) => k === step);
  return html`<ol class="stepper">${STEPS.map(([k, label], i) => html`
    <li key=${k} class=${i === at ? "on" : i < at ? "done" : ""}>${i < at ? "✓ " : ""}${label}</li>`)}</ol>`;
};

const ServiceNotice = ({ status }) => (status === "running" ? null : html`
  <div class="banner">
    ${status === "missing"
      ? html`<b>Service chưa được khởi động.</b> Kịch bản cần service thật. Hãy chạy <code>make jar && make up</code>, rồi quay lại đây.`
      : html`<b>Mọi instance của service đang tắt.</b> Bạn có thể bật lại ở phần Điều khiển container, hoặc bấm Reset baseline.`}
  </div>`);

function Scenario({ s, lab, status }) {
  const key = "guess-" + s.id;
  const [guess, setGuess] = useState(safeStore.get(key, ""));
  const [started, setStarted] = useState(false);
  const [revealed, setRevealed] = useState(false);
  const [error, setError] = useState("");
  const steps = lab.stepsByScenario[s.id] || [];
  const result = lab.resultsByScenario[s.id];
  const running = started && !result;
  const step = stepOf({ guess, running, result, revealed });

  const run = async () => {
    setError("");
    setRevealed(false);
    lab.clearScenario(s.id);
    try {
      await api("POST", `/api/scenarios/${encodeURIComponent(s.id)}/run`);
      setStarted(true);
    } catch (e) {
      setStarted(false);
      setError(e.status === 409 ? "Đang có kịch bản hoặc reset khác chạy." : e.message);
    }
  };

  return html`
    <div class="stack">
      <div class="panel stack">
        <div class="between scenario-head">
          <div class="grow"><div class="caps err-text">Kịch bản ${s.id}</div><h1 class="h1">${s.title}</h1></div>
          <${Stepper} step=${step} />
        </div>
        <${ServiceNotice} status=${status} />
      </div>
      <${Safe} name="flow"><${FlowMap} lab=${lab} mode="service" onSelect=${() => {}} /><//>
      <div class="scenario-grid">
        <div class="panel stack">
          <div class="caps">Dự đoán trước khi chạy</div>
          <p>${s.question}</p>
          <textarea placeholder="Ghi dự đoán của bạn (chỉ lưu trên máy này)…" value=${guess}
            onInput=${(e) => { setGuess(e.target.value); safeStore.set(key, e.target.value); }}></textarea>
          <div class="row">
            <button class="btn primary" disabled=${running || status === "missing"} title=${status === "missing" ? "Cần chạy make jar && make up trước" : ""} onClick=${run}>
              ${running ? "Đang chạy…" : "Chạy kịch bản"}</button>
            <button class="btn" disabled=${!result} onClick=${() => setRevealed(true)}>Xem giải thích</button>
            <button class="btn tiny" onClick=${() => setRevealed(true)}>bỏ qua, xem luôn</button>
          </div>
          ${error && html`<div class="err-text small wrap">${error}</div>`}
        </div>
        <div class="panel stack">
          <div class="between"><span class="caps">Quan sát</span>
            ${result && html`<span class=${"tag " + (result.passed ? "ok" : "err")}>${result.passed ? "PASS" : "FAIL"}</span>`}</div>
          ${steps.length === 0 && html`<div class="muted small">Các bước sẽ hiện ở đây khi kịch bản chạy.</div>`}
          <ol class="journey">
            ${steps.map((x) => html`<li key=${x.step} class=${"hop run-step " + (x.status === "pass" ? "ok" : x.status === "fail" ? "unverified" : "pending")}>
              <span class="hop-mark">${x.status === "pass" ? "✓" : x.status === "fail" ? "✗" : "…"}</span>
              <span class="hop-body">${x.step}${x.observed ? ` — ${x.observed}` : ""}${x.err ? html`<div class="err-text">${x.err}</div>` : ""}</span>
            </li>`)}
          </ol>
          ${result && html`<div class="small muted">${result.passed ? "Hệ thống làm đúng như phần giải thích." : "Xem bước bị ✗."}</div>`}
        </div>
      </div>
      ${revealed && html`
        <div class="panel stack">
          <div class="caps">Giải thích</div>
          ${guess && html`<div class="inset small">Dự đoán của bạn: “${guess}”. Đúng chỗ nào, sai chỗ nào?</div>`}
          <div class="reveal" dangerouslySetInnerHTML=${{ __html: s.reveal }}></div>
          <div class="caps">Java properties liên quan</div>
          <${Cli} text=${(s.javaProps || []).join("\n") || "—"} />
          <div class="caps">Lệnh CLI tương đương</div>
          ${(s.cli || []).map((c, i) => html`<${Cli} key=${i} text=${c} />`)}
          <div class="caps">Tự kiểm tra (không có đáp án)</div>
          <ol class="confirm">${(s.confirm || []).map((q, i) => html`<li key=${i}>${q}</li>`)}</ol>
        </div>`}
    </div>`;
}

function FreePlay({ lab, status }) {
  const [f, setF] = useState({ count: 1000, sensors: 100, ratePerSec: 0, acks: "all", idempotent: true, poisonEvery: 0 });
  const num = (k) => (e) => setF({ ...f, [k]: Number(e.target.value) });
  const submit = (e) => {
    e.preventDefault();
    lab.act(`gửi ${f.count} record vào topic của service`, "POST", "/api/produce", { ...f, idempotent: f.idempotent && f.acks === "all" });
  };
  return html`
    <form class="tool-form" onSubmit=${submit}>
      <label class="field">Số record<input type="number" min="1" value=${f.count} onInput=${num("count")} /></label>
      <label class="field">Số sensor (key)<input type="number" min="1" value=${f.sensors} onInput=${num("sensors")} /></label>
      <label class="field">Rate/s (0 = burst)<input type="number" min="0" value=${f.ratePerSec} onInput=${num("ratePerSec")} /></label>
      <label class="field">Poison mỗi N (0 = không)<input type="number" min="0" value=${f.poisonEvery} onInput=${num("poisonEvery")} /></label>
      <label class="field">acks<select value=${f.acks} onChange=${(e) => setF({ ...f, acks: e.target.value })}>
        <option value="all">all</option><option value="1">1</option><option value="0">0</option></select></label>
      <label class="check"><input type="checkbox" checked=${f.idempotent && f.acks === "all"} disabled=${f.acks !== "all"}
        onChange=${(e) => setF({ ...f, idempotent: e.target.checked })} /> enable.idempotence</label>
      <button class="btn primary" disabled=${status === "missing"}>Gửi</button>
    </form>`;
}

function Containers({ lab }) {
  const containers = (lab.snapshot && lab.snapshot.containers) || {};
  const names = Object.keys(containers).sort();
  return html`<div class="scroll-x"><table>
    <thead><tr><th>Container</th><th>Trạng thái</th><th>Thao tác</th></tr></thead>
    <tbody>${names.map((n) => {
      const actions = n.startsWith("lab-gap-svc") ? [...CHAOS, "disconnect", "connect"] : CHAOS;
      return html`<tr key=${n}><td>${short(n)}</td>
        <td><span class=${"tag " + (containers[n] === "running" ? "ok" : "err")}>${containers[n]}</span></td>
        <td><div class="row">${actions.map((a) => html`<button key=${a} class=${"btn tiny" + (a === "kill" ? " danger" : "")}
          onClick=${() => lab.act(`docker ${a} ${n}`, "POST", "/api/chaos", { action: a, target: n })}>${a}</button>`)}</div></td></tr>`;
    })}</tbody>
  </table></div>`;
}

export function Scenarios({ lab }) {
  const [scenarios, setScenarios] = useState([]);
  const [id, setIdState] = useState(safeStore.get("scenario", ""));
  const setId = (v) => { setIdState(v); safeStore.set("scenario", v); };
  useEffect(() => {
    api("GET", "/api/scenarios").then(setScenarios).catch((e) => lab.note(`không tải được kịch bản: ${e.message}`, true));
  }, []);
  const status = serviceStatus(lab.snapshot && lab.snapshot.containers);
  const s = scenarios.find((x) => x.id === id) || scenarios[0];
  const reset = async () => {
    try {
      await api("POST", "/api/reset");
      lab.note("reset bắt đầu…");
    } catch (e) {
      lab.note(e.status === 409 ? "đang có kịch bản chạy, chưa reset được" : `reset: ${e.message}`, true);
    }
  };
  return html`
    <div class="scenario-page">
      <div class="stack">
        <${ScenarioList} scenarios=${scenarios} id=${s && s.id} setId=${setId} />
        <button class="btn" disabled=${status === "missing"} title="Đưa lab về trạng thái chuẩn: 3 broker, DB, 2 instance, topic trống" onClick=${reset}>Reset baseline</button>
      </div>
      <div class="stack">
        ${s ? html`<${Scenario} key=${s.id} s=${s} lab=${lab} status=${status} />` : html`<div class="panel muted">Đang tải kịch bản…</div>`}
        <details class="panel"><summary class="caps">Free play: gửi dữ liệu vào service</summary><${FreePlay} lab=${lab} status=${status} /></details>
        <details class="panel"><summary class="caps">Điều khiển container</summary><${Containers} lab=${lab} /></details>
        <details class="panel"><summary class="caps">Chưa cover (khoảng 20% còn lại)</summary>
          <ul class="confirm"><li>Bảo mật: SASL, SSL, ACL</li><li>Schema Registry, Avro, Protobuf</li>
            <li>Kafka Connect, Kafka Streams, MirrorMaker</li><li>Consumer protocol mới (KIP-848), tiered storage, multi-DC</li></ul>
        </details>
      </div>
    </div>`;
}
