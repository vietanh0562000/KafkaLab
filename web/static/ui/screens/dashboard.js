import { html, Safe } from "../shell.js";
import { TopicSelect } from "../inspector.js";
import { LineChart } from "../chart.js";
import { isDead, lagOf, partitionHealth } from "../logic/health.js";
import { topicTotals } from "../logic/track.js";
import { playgroundView } from "../flow.js";

const num = (n) => (n === null || n === undefined ? "?" : Number(n).toLocaleString("en-US"));

const Metric = ({ label, value, sub, tag, cls = "" }) => html`
  <div class="panel metric">
    <div class="between"><span class="caps">${label}</span>${tag && html`<span class=${"tag " + cls}>${tag}</span>`}</div>
    <div class="metric-value">${value}</div>
    <div class="muted small">${sub}</div>
  </div>`;

function groupLag(g, parts) {
  let lag = null, partial = false;
  for (const p of parts) {
    const l = lagOf(p.end, g.committed && g.committed[p.partition]);
    if (l === null) partial = true; else lag = (lag || 0) + l;
  }
  return lag === null ? "?" : `${partial ? "≥ " : ""}${num(lag)}`;
}

export function Dashboard({ lab }) {
  const st = lab.st;
  if (!st) return html`<div class="panel muted">Đang kết nối tới lab…</div>`;
  const view = playgroundView(st);
  const topic = view.topic;
  const parts = (topic && topic.partitions) || [];
  const brokers = view.brokers;
  const up = brokers.filter((b) => b.state === "running").length;
  const series = topic ? lab.history.series(topic.name) : [];
  const rates = series.filter((p) => p && p.rate !== null);
  // One 0.7 s read is too spiky to quote, so the headline averages the last few.
  const recent = rates.slice(-6);
  const rate = recent.length ? recent.reduce((a, p) => a + p.rate, 0) / recent.length : null;
  const totals = topicTotals(topic, view.groups);
  const healths = parts.map((p) => partitionHealth({ partition: p, brokers, minIsr: topic.minIsr }));
  const worst = healths.includes("red") ? "red" : healths.includes("yellow") ? "yellow" : parts.length ? "ok" : "";
  const snap = lab.snapshot;
  const dbDown = Boolean(snap && snap.errors && snap.errors.db);
  const dbRows = !snap || dbDown ? null : snap.dbRows;
  const cluster = up === brokers.length ? "ok" : up === 0 ? "err" : "warn";
  return html`
    <div class="stack dashboard">
      <div class="panel row">
        <label class="tool-select"><span class="caps">Topic</span>
          <${TopicSelect} topics=${(st.topics || []).map((t) => t.name)} value=${lab.topic} onChange=${lab.setTopic} /></label>
        <span class="muted small grow">Throughput, lag và consumer group chỉ tính cho topic đang chọn. Lịch sử nằm trong trình duyệt, tối đa 15 phút, mất khi tải lại trang.</span>
      </div>
      <div class="metrics">
        <${Metric} label="Cluster" value=${`${up}/${brokers.length} broker`} sub="đang chạy" tag=${cluster === "ok" ? "ONLINE" : cluster === "err" ? "OFFLINE" : "DEGRADED"} cls=${cluster} />
        <${Metric} label="Throughput" value=${rate === null ? "?" : `${num(Math.round(rate * 10) / 10)} msg/s`} sub=${topic ? `${topic.name} · trung bình ~4 giây` : "chưa chọn topic"} />
        <${Metric} label="Lag" value=${totals.lag === null ? "?" : `${totals.partial ? "≥ " : ""}${num(totals.lag)} msg`} sub=${`${view.groups.length} group trên topic này`}
          tag=${totals.lag === null ? null : totals.lag === 0 ? "BẮT KỊP" : "CÒN LAG"} cls=${totals.lag === 0 ? "ok" : "warn"} />
        <${Metric} label="TimescaleDB rows" value=${num(dbRows)} sub=${dbDown ? "không đọc được DB" : "bảng của service"} tag=${dbDown ? "LỖI" : null} cls="err" />
      </div>
      <div class="dash-charts">
        <${Safe} name="throughput"><${LineChart} title="Throughput" unit="msg/s" points=${series} field="rate" /><//>
        <${Safe} name="lag"><${LineChart} title="Consumer lag" unit="msg" points=${series} field="lag" /><//>
      </div>
      <div class="dash-tables">
        <div class="panel stack">
          <div class="between"><span class="title">Broker</span>
            ${worst && html`<span class=${"tag " + (worst === "ok" ? "ok" : worst === "red" ? "err" : "warn")}>${worst === "ok" ? "ISR đủ" : worst === "red" ? "có partition hỏng" : "thiếu ISR"}</span>`}</div>
          <div class="scroll-x"><table>
            <thead><tr><th>Broker</th><th>Trạng thái</th><th class="num">Leader</th><th class="num">Trong ISR</th></tr></thead>
            <tbody>${brokers.map((b) => html`<tr key=${b.id}>
              <td>broker-${b.id}</td>
              <td><span class=${"tag " + (b.state === "running" ? "ok" : isDead(b) ? "err" : "")}>${b.state || "?"}</span></td>
              <td class="num">${parts.filter((p) => p.leader === b.id).length}</td>
              <td class="num">${parts.filter((p) => p.isr.includes(b.id)).length}/${parts.length}</td></tr>`)}</tbody>
          </table></div>
        </div>
        <div class="panel stack">
          <div class="between"><span class="title">Topic</span><span class="muted small">${(st.topics || []).length} topic</span></div>
          <div class="scroll-x"><table>
            <thead><tr><th>Topic</th><th class="num">Partition</th><th class="num">RF</th></tr></thead>
            <tbody>${(st.topics || []).map((t) => html`<tr key=${t.name} class=${t.name === lab.topic ? "row-on" : ""}>
              <td class="break">${t.name}</td><td class="num">${t.partitions}</td><td class="num">${t.rf}</td></tr>`)}</tbody>
          </table></div>
        </div>
        <div class="panel stack">
          <div class="between"><span class="title">Consumer group</span><span class="muted small">${topic ? topic.name : ""}</span></div>
          ${view.groups.length === 0 && html`<div class="muted small">Chưa có group nào trên topic này.</div>`}
          ${view.groups.length > 0 && html`<div class="scroll-x"><table>
            <thead><tr><th>Group</th><th>State</th><th class="num">Member</th><th class="num">Lag</th></tr></thead>
            <tbody>${view.groups.map((g) => html`<tr key=${g.name}><td class="break">${g.name}</td><td>${g.state || "?"}</td>
              <td class="num">${g.members}</td><td class="num">${groupLag(g, parts)}</td></tr>`)}</tbody>
          </table></div>`}
        </div>
      </div>
    </div>`;
}
