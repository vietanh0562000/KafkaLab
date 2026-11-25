import { h } from "preact";
import { useErrorBoundary, useState } from "preact/hooks";
import htm from "htm";

export const html = htm.bind(h);

export const TABS = [
  ["playground", "Playground"],
  ["lessons", "Lessons"],
  ["scenarios", "Failure Scenarios"],
  ["dashboard", "Live Dashboard"],
];

// Safe keeps one broken panel from blanking the whole page.
export function Safe({ name, children }) {
  const [err, reset] = useErrorBoundary();
  if (err) {
    return html`<div class="banner err">Panel ${name} gặp lỗi hiển thị: ${String((err && err.message) || err)}
      <button class="btn tiny" onClick=${reset}>vẽ lại</button></div>`;
  }
  return children;
}

export function Cli({ text }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch { /* clipboard blocked */ }
  };
  return html`<div class="cli">${text}<button class="btn tiny" onClick=${copy}>${copied ? "đã chép" : "chép"}</button></div>`;
}

function BrokerStatus({ lab }) {
  if (!lab.live) return html`<span class="tag err"><span class="dot err"></span>mất kết nối</span>`;
  const brokers = (lab.st && lab.st.brokers) || [];
  if (!brokers.length) return html`<span class="tag">broker: ?</span>`;
  const up = brokers.filter((b) => b.state === "running").length;
  const cls = up === brokers.length ? "ok" : up === 0 ? "err" : "warn";
  return html`<span class=${"tag " + cls}><span class=${"dot " + cls}></span>${up}/${brokers.length} broker chạy</span>`;
}

export function Header({ route, lab }) {
  return html`
    <header class="top">
      <span class="brand">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
          <path d="M12 2.5 20.5 7.2v9.6L12 21.5l-8.5-4.7V7.2z"/><path d="M3.5 7.2 12 12l8.5-4.8M12 12v9.5"/>
        </svg>
        KafkaLab
      </span>
      <nav class="tabs">
        ${TABS.map(([id, label]) => html`<a key=${id} href=${"#" + id} class=${route === id ? "on" : ""}>${label}</a>`)}
      </nav>
      <${BrokerStatus} lab=${lab} />
      <button class=${"btn" + (lab.frozen ? " on" : "")} title="Chỉ đóng băng màn hình để đọc. Kafka vẫn chạy bình thường."
        onClick=${() => lab.setFrozen(!lab.frozen)}>${lab.frozen ? "Tiếp tục hiển thị" : "Tạm dừng hiển thị"}</button>
    </header>`;
}

export function Footer({ lab }) {
  return html`
    <footer class="foot-bar">
      <span>${lab.frozen ? "Màn hình đang đóng băng, Kafka vẫn chạy" : "Dữ liệu thật từ cụm lab · đọc mỗi 0,7 giây"}</span>
      <a href="classic.html">Trang cũ</a>
    </footer>`;
}
