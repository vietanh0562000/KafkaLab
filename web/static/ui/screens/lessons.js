import { useEffect, useState } from "preact/hooks";
import { html, Cli, Safe } from "../shell.js";
import { api, safeStore } from "../api.js";
import { LESSON_TOPIC } from "../store.js";
import { Workspace } from "../workspace.js";

// Progress lives in lab-control's memory; the browser remembers it too, so restarting the lab keeps it.
function rememberDone(statuses) {
  const done = new Set(safeStore.get("pg-done", "").split(",").filter(Boolean));
  for (const s of statuses) if (s.done) done.add(s.id);
  safeStore.set("pg-done", [...done].join(","));
  return statuses.map((s) => ({ ...s, done: s.done || done.has(s.id) }));
}

function Curriculum({ lessons, status, id, setId, done }) {
  return html`
    <div class="panel curriculum">
      <div class="between"><span class="caps">Curriculum</span><span class="tag blue">${done} / ${lessons.length || 8} xong</span></div>
      <div class="progress"><div style=${`width:${lessons.length ? (100 * done) / lessons.length : 0}%`}></div></div>
      <div class="lesson-list">
        ${lessons.map((x) => {
          const d = status[x.id] && status[x.id].done;
          return html`<button key=${x.id} class=${"lesson-item" + (x.id === id ? " on" : "")} onClick=${() => setId(x.id)}>
            <span class=${"lesson-mark" + (d ? " done" : "")}>${d ? "✓" : x.id}</span>
            <span class="grow ellipsis" title=${x.title}>${x.title}</span>
          </button>`;
        })}
      </div>
    </div>`;
}

// The explanation sits under the map, so finishing a lesson never pushes the flow off the screen.
function Reveal({ l, guess }) {
  return html`<div class="panel stack">
    <div class="caps">Giải thích</div>
    ${guess && html`<div class="inset small">Dự đoán của bạn: “${guess}”. Đúng chỗ nào, sai chỗ nào?</div>`}
    <div class="reveal" dangerouslySetInnerHTML=${{ __html: l.reveal }}></div>
    <div class="caps">Lệnh tương đương</div>
    ${l.cli.map((c, i) => html`<${Cli} key=${i} text=${c} />`)}
    <div class="caps">Tự kiểm tra (không có đáp án)</div>
    <ul class="confirm">${l.confirm.map((q, i) => html`<li key=${i}>${q}</li>`)}</ul>
  </div>`;
}

function LessonPage({ l, s, lessons, status, setId, doneCount, lab }) {
  const key = `pg-predict-${l.id}`;
  const [guess, setGuess] = useState(safeStore.get(key, ""));
  const [shown, setShown] = useState(false);
  const done = s && s.done;
  return html`
    <div class="lesson-top">
      <${Curriculum} lessons=${lessons} status=${status} id=${l.id} setId=${setId} done=${doneCount} />
      <div class="panel stack lesson">
      <div class="row"><span class="tag blue">BÀI ${String(l.id).padStart(2, "0")}</span>
        <span class=${"tag " + (done ? "ok" : "")}>${done ? "Đã xong" : "Đang học"}</span></div>
      <h1 class="h1">${l.title}</h1>
      <div class="lesson-grid">
        <div class="stack">
          <div class="inset stack">
            <div class="caps">Đoán trước khi làm</div>
            <p>${l.question}</p>
            <textarea placeholder="Ghi dự đoán của bạn (chỉ lưu trên máy này)…" value=${guess}
              onInput=${(e) => { setGuess(e.target.value); safeStore.set(key, e.target.value); }}></textarea>
          </div>
          <div class=${"goal" + (done ? " done" : "")}>
            <b>${done ? "✓ Xong: " : "Mục tiêu: "}${l.goal}</b>
            <div class="small">${s ? s.detail : "…"}</div>
          </div>
        </div>
        <div class="stack">
          <div class="caps">Làm</div>
          <ol class="steps">${l.steps.map((x, i) => html`<li key=${i}>${x}</li>`)}</ol>
        </div>
      </div>
      ${shown || done
        ? html`<div class="muted small">Phần giải thích nằm dưới bản đồ flow.</div>`
        : html`<div class="row"><span class="muted small">Giải thích mở ra khi bạn đạt mục tiêu.</span>
            <button class="btn tiny" onClick=${() => setShown(true)}>xem luôn</button></div>`}
      </div>
    </div>
    <${Safe} name="workspace"><${Workspace} lab=${lab} /><//>
    ${(shown || done) && html`<${Reveal} l=${l} guess=${guess} />`}`;
}

export function Lessons({ lab }) {
  const [lessons, setLessons] = useState([]);
  const [id, setIdState] = useState(safeStore.get("pg-lesson", "1"));
  const setId = (v) => { setIdState(v); safeStore.set("pg-lesson", v); };
  useEffect(() => {
    api("GET", "/api/pg/lessons").then(setLessons).catch((e) => lab.note(`không tải được bài học: ${e.message}`, true));
  }, []);

  const hasOrders = Boolean(lab.st && (lab.st.topics || []).some((t) => t.name === LESSON_TOPIC));
  useEffect(() => { if (hasOrders) lab.setTopic(LESSON_TOPIC); }, [hasOrders]);

  const statuses = rememberDone((lab.st && lab.st.lessons) || []);
  const status = Object.fromEntries(statuses.map((s) => [s.id, s]));
  const done = statuses.filter((s) => s.done).length;
  const l = lessons.find((x) => x.id === id) || lessons[0];
  return html`
    <div class="stack">
      ${l
        ? html`<${LessonPage} key=${l.id} l=${l} s=${status[l.id]} lessons=${lessons} status=${status} setId=${setId} doneCount=${done} lab=${lab} />`
        : html`<div class="panel muted">Đang tải bài học…</div>`}
    </div>`;
}
