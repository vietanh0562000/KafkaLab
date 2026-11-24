# Data Flow UI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the KafkaLab web UI with four screens built around a live data-flow map and a per-record journey inspector.

**Architecture:** Pure logic (edge health, record trace, state diff, history) lives in dependency-free ES modules tested with `node --test`. Preact + htm components render it. The new UI is built at `web/static/app.html` beside the old page and replaces `index.html` only in the last task, so every commit leaves a working app.

**Tech Stack:** Preact + htm (already vendored), SVG, CSS custom properties, Go `embed`, `node --test`.

**Spec:** `docs/superpowers/specs/2026-11-24-dataflow-ui-redesign-design.md`. Read it before any task.

## Global Constraints

- No change to Go code, API, scenarios or lesson content. The only non-UI files touched are tests under `web/test/`.
- No build step, no new runtime dependency, no CDN. Fonts are served from `web/static/vendor/fonts`.
- UI copy is Vietnamese; Kafka terms stay English.
- Colors, type and spacing come from `design/notebook_telemetry/DESIGN.md`. Hex values appear only in `web/static/ui/theme.css`. No blur shadows, no pill radii.
- Record key and value render as text only. Only server `reveal` uses `dangerouslySetInnerHTML`.
- Lesson wording must stay true: keep visible labels `Topic`, `Producer`, `Consumer`, `Broker`, consumer cards by name, partition lanes, group flags, `đã gửi`.
- Unknown is never drawn as 0 or healthy. Offsets of `-1` render as `?`.
- Wrap every `localStorage` access in the existing try/catch `store` pattern.
- Work on branch `feat/dataflow-ui`. End commit messages with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Test files live in `web/test/`, outside `go:embed`.

## Review Focus

1. A record with a very long or HTML-like key/value must stay inert text and must not widen the page (Task 5 test, Task 10 browser check).
2. A broker killed mid-session returns partial state with `errors`; the map must keep last-good data, show `?`, and never crash (Tasks 2 and 5).
3. The topic open in the inspector is deleted or recreated; the inspector must close or show stale evidence as unverified (Tasks 3 and 4).
4. `build/service.jar` is missing, so `instances` and `partitions` are `null`; Scenarios and Dashboard must render with guidance (Tasks 8 and 9).
5. A 1000-record send must cap tokens at 12 plus `+N` and keep the page responsive (Task 4).

---

### Task 1: Theme, fonts and app shell

**Files:**
- Create: `web/static/app.html`, `web/static/ui/theme.css`, `web/static/ui/api.js`, `web/static/ui/store.js`, `web/static/ui/main.js`, `web/static/ui/shell.js`
- Create: `web/static/vendor/fonts/` (Inter 400/500/600 and JetBrains Mono 400/500, `latin` and `vietnamese` subsets)
- Test: `web/test/store.test.mjs`

**Interfaces:**
- Produces: `api(method, path, body)` (same contract as `pg.js`); `safeStore.get(k, d)`, `safeStore.set(k, v)`; `useLab()` returning `{st, snapshot, live, frozen, setFrozen, topic, setTopic, act, note, log, cli, stepsByScenario, resultsByScenario}`; `mergeLastGood(next, cache, topic)`.

- [ ] **Step 1:** Create the branch: `git switch -c feat/dataflow-ui`.
- [ ] **Step 2:** Write failing tests: `mergeLastGood` keeps cached `brokers` when `errors.brokers` is set, replaces them when absent, and keys `groups` by topic.
- [ ] **Step 3:** Run `node --test web/test/`. Expected: FAIL, module not found.
- [ ] **Step 4:** Implement `store.js`. Move polling (700 ms), `act`, log and toast logic from `pg.js:39-117`; add SSE `snapshot`, `log`, `step`, `scenario`. While `frozen`, keep polling and update refs but do not publish new state to components.
- [ ] **Step 5:** Fetch fonts with `npm pack @fontsource/inter@5.0.16 @fontsource/jetbrains-mono@5.0.18` in the scratchpad, and copy only the needed woff2 files and license files. Declare `@font-face` with the packages' `unicode-range` so Vietnamese glyphs load.
- [ ] **Step 6:** Implement theme tokens and `shell.js`: brand, tabs `Playground`, `Lessons`, `Failure Scenarios`, `Live Dashboard`, hash routes `#playground|#lessons|#scenarios|#dashboard`, broker status, `Tạm dừng hiển thị`, footer link to `classic.html`.
- [ ] **Step 7:** Run `node --test web/test/` and `go test ./...`. Expected: PASS. Open `/app.html` and confirm tabs route and Vietnamese text uses Inter.
- [ ] **Step 8:** Commit `feat(ui): notebook theme, fonts and app shell`.

### Task 2: Edge health rules

**Files:** Create `web/static/ui/logic/health.js`. Test `web/test/health.test.mjs`.

**Interfaces:**
- Produces: `lagOf(end, committed): number|null`; `partitionHealth({partition, brokers, minIsr}): "red"|"yellow"|"ok"`; `edgeColor({partitionHealth, lastSend, lagSince, members, lastChangeAt, now}): "red"|"yellow"|"blue"|"idle"`.
- `lastSend` is `{ok: boolean, at: ms}|null`. `lagSince` is the ms when lag last stopped decreasing while above 0, or `null`.

- [ ] **Step 1:** Write failing tests, one per rule:
  - no leader, dead listed leader, or `isr.length < minIsr` gives `red`
  - dead follower with ISR at `minIsr` gives `yellow`; dead broker still in ISR gives `yellow`
  - failed send 29 s ago gives `red`; 31 s ago does not; a later success clears it
  - lag held 10 s with `members: 1` gives `yellow`; 9 s gives no yellow; `members: 0` gives no yellow
  - change 4 s ago gives `blue`; 6 s ago gives `idle`
  - `lagOf(-1, 3) === null`; `lagOf(10, undefined) === null`; `lagOf(10, 3) === 7`
  - unknown broker state (`""`) is not treated as dead
- [ ] **Step 2:** Run `node --test web/test/health.test.mjs`. Expected: FAIL.
- [ ] **Step 3:** Implement with rule order red, yellow, blue, idle.
- [ ] **Step 4:** Run the test. Expected: PASS.
- [ ] **Step 5:** Commit `feat(ui): edge health rules`.

### Task 3: Record journey

**Files:** Create `web/static/ui/logic/trace.js`. Test `web/test/trace.test.mjs`.

**Interfaces:**
- Produces: `buildTrace({topic, record, partition, producers, consumers, groups, staleBefore}): {send, log, reads: [{group, consumer, status, redelivered}], commits: [{group, status, committed}]}`.
- `status` is `"ok"|"pending"|"unverified"|"evicted"`. `staleBefore` is a ms timestamp from Task 4; sends older than it are ignored.

- [ ] **Step 1:** Write failing tests:
  - matching send gives `send.status === "ok"`; same offset with a different value gives `"unverified"`
  - `acks: "0"` is never matched by offset and returns `{status: "unverified", reason: "acks0"}`
  - a send older than `staleBefore` is ignored
  - no send in buffer gives `"evicted"`
  - two groups produce separate read and commit rows
  - `committed: 42` for offset 42 is `pending`; `committed: 43` is `ok`
  - consumer record with a different key or value is `"unverified"`
- [ ] **Step 2:** Run `node --test web/test/trace.test.mjs`. Expected: FAIL.
- [ ] **Step 3:** Implement. Consumer records have no timestamp, so the store clears evidence for a recreated topic through the ignored-key set from Task 4; accept it as `ignoredReads: Set<"name:partition:offset">` in the input.
- [ ] **Step 4:** Run the test. Expected: PASS.
- [ ] **Step 5:** Commit `feat(ui): record journey from observed evidence`.

### Task 4: State diff and history

**Files:** Create `web/static/ui/logic/diff.js`, `web/static/ui/logic/history.js`. Test `web/test/diff.test.mjs`, `web/test/history.test.mjs`.

**Interfaces:**
- Produces: `diffState(prev, next, maxTokens = 12): {tokens: [{kind: "send"|"read", from, to, partition, key}], overflow: number, changedPartitions: number[], topicReset: boolean}`.
- Produces: `createHistory(windowMs = 900000)` with `.sample(topic, atMs, {end, lag})` and `.series(topic): [{at, rate, lag}|null]`. `null` marks a gap.

- [ ] **Step 1:** Write failing tests:
  - first state produces no tokens
  - 1000 new sends give 12 tokens and `overflow === 988`
  - a pending send does not produce a token until it completes
  - `end` decreasing or a topic disappearing gives `topicReset: true`
  - history drops samples older than 15 minutes
  - a sampling gap over 5 s yields `null`; a falling `end` does not yield a negative rate
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement, then wire both into `useLab()`: `tokens`, `history`, `staleBefore`, `ignoredReads`.
- [ ] **Step 4:** Run `node --test web/test/`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(ui): state diff and telemetry history`.

### Task 5: Flow map

**Files:** Create `web/static/ui/flow.js`, `web/static/ui/flow.css`, `web/static/ui/logic/layout.js`. Test `web/test/layout.test.mjs`.

**Interfaces:**
- Consumes: Tasks 2 and 4.
- Produces: `<FlowMap st snapshot tokens selection onSelect mode />`; `mode` is `"playground"|"service"`. `selection` is `{kind: "producer"|"partition"|"broker"|"group"|"consumer"|"record", id}`.
- Produces: `layoutFlow({producers, partitions, groups, service, dlq}): {nodes, edges}`.

- [ ] **Step 1:** Write failing layout tests: playground mode has no service or DB nodes; service mode has no producer column and has a DLQ edge from service; a `-tracking` group has no edge to service; null inputs give an empty valid layout.
- [ ] **Step 2:** Run the test. Expected: FAIL.
- [ ] **Step 3:** Implement `layoutFlow` and `FlowMap`: HTML nodes, SVG edges, 16 px grid, partition lanes with records, group flags and consumer positions (reuse the marker logic of `pg.js:371-444`). Tokens animate along edges; disabled under `prefers-reduced-motion`. Show the note that the map trails real events by up to one read cycle. Clamp cell text with ellipsis and show full values only in the inspector.
- [ ] **Step 4:** Run tests, then send records with key `<img src=x onerror=alert(1)>` and a 5000-character value. Expected: text is inert and the page has no horizontal scroll.
- [ ] **Step 5:** Commit `feat(ui): live flow map`.

### Task 6: Inspector and Playground

**Files:** Create `web/static/ui/inspector.js`, `web/static/ui/workspace.js`, `web/static/ui/screens/playground.js`.

**Interfaces:**
- Consumes: `buildTrace`, `FlowMap`, `useLab`.
- Produces: `<Workspace lab mode />`, the map + inspector + toolbar reused by Lessons.

- [ ] **Step 1:** Port every control from `pg.js`: topic create/select/delete, producer add/send/delete, consumer add/poll/commit/auto/delete, broker kill/stop/start, clear, CLI, log. Keep the `Topic`, `Producer`, `Consumer`, `Broker` labels and the `đã gửi` list.
- [ ] **Step 2:** Implement inspectors per selection, including the record journey from spec section 7 with the ISR "lúc đang xem" label. Close the selection when its topic disappears.
- [ ] **Step 3:** Verify in browser: create topic, send 6 keyed records, poll, commit, open a record. Expected: journey shows Gửi, Ghi log, Consumer and Commit correctly. Kill a broker. Expected: edge colors follow Task 2. Start it again.
- [ ] **Step 4:** Commit `feat(ui): playground workspace with record journey`.

### Task 7: Lessons

**Files:** Create `web/static/ui/screens/lessons.js`.

**Interfaces:** Consumes `<Workspace>`, `safeStore`, `st.lessons`.

- [ ] **Step 1:** Port `LessonPanel`, `Lesson` and `rememberDone` from `pg.js:185-246`. Keep storage keys `pg-done`, `pg-predict-<id>`, `pg-lesson` so progress survives.
- [ ] **Step 2:** Lay out curriculum left, lesson content and `<Workspace>` on topic `orders`; empty map offers topic creation.
- [ ] **Step 3:** Verify in browser: complete lesson 1 by following its text exactly. Expected: status becomes done and explanation opens.
- [ ] **Step 4:** Commit `feat(ui): lessons on the live workspace`.

### Task 8: Failure Scenarios

**Files:** Create `web/static/ui/screens/scenarios.js`, `web/static/ui/logic/stepper.js`. Test `web/test/stepper.test.mjs`.

**Interfaces:**
- Produces: `stepOf({guess, running, result, revealed}): "predict"|"run"|"observe"|"explain"`.

- [ ] **Step 1:** Write failing tests for each stepper state, and that `revealed` always gives `"explain"`.
- [ ] **Step 2:** Run the test. Expected: FAIL.
- [ ] **Step 3:** Implement the screen: module list using the `MODULES` names in `app.js:3-6`, stepper, service-mode `FlowMap`, live steps, reveal, Java props, CLI, confirm questions, reset baseline with 409 message, free-play produce, container chaos controls, uncovered-topics list.
- [ ] **Step 4:** When `snapshot.containers` has no running `lab-gap-svc-*`, show guidance `make jar && make up` and render the rest with null data.
- [ ] **Step 5:** Run tests and verify in browser without the jar. Expected: no crash, guidance visible. Record that real scenario runs are unverified unless the jar exists.
- [ ] **Step 6:** Commit `feat(ui): failure scenarios with predict-run-observe-explain`.

### Task 9: Live Dashboard

**Files:** Create `web/static/ui/screens/dashboard.js`, `web/static/ui/chart.js`.

**Interfaces:** Consumes `history.series(topic)`, `lagOf`, `useLab`.

- [ ] **Step 1:** Implement metric cards, broker, topic and group tables, and an SVG chart that breaks lines at `null`. Label throughput, lag and groups as scoped to the selected topic and note that history clears on reload. Unknown DB rows show `?` when `snapshot.errors.db` is set.
- [ ] **Step 2:** Verify in browser: send records and see rate rise; switch topics and see a gap; confirm no CPU or disk fields appear.
- [ ] **Step 3:** Commit `feat(ui): live dashboard`.

### Task 10: Cutover and full verification

**Files:** Modify `web/static/index.html`, `web/static/classic.html` (back link). Delete `web/static/app.html`, `web/static/pg.js`, `web/static/pg.css`.

- [ ] **Step 1:** Move the shell into `index.html`, and confirm nothing references `pg.js`, `pg.css` or `app.html`: `grep -rn 'pg\.js\|pg\.css\|app\.html' web internal`. Expected: no matches.
- [ ] **Step 2:** Run `node --test web/test/ && go test ./...`. Expected: PASS.
- [ ] **Step 3:** Restart lab-control and check all four screens at 1440 px and 375 px. Expected: no console errors, no horizontal page scroll.
- [ ] **Step 4:** Repeat the broker kill and start flow, and the Review Focus cases.
- [ ] **Step 5:** Commit `feat(ui): switch to the data flow UI`.
