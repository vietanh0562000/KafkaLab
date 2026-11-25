# UI design system

Direction: **Instrument** (neutral, hairline, colour only for state) for the whole product, with a **Notebook** reading
treatment for the lesson column. Agreed 2026-11-23 after an audit of the running lab at 1440 × 900 and 390 × 844,
light and dark.

The product is an interactive Kafka lab, not a dashboard. Every region serves one step of the learning loop:

| Loop step | Lessons page | Scenarios page |
|---|---|---|
| Predict | "Dự đoán" block: question + textarea | "Dự đoán" box above the run button |
| Experiment | "Làm" steps + stage controls + producer/consumer column | "Chạy kịch bản" + manual controls |
| Observe | Stage (brokers, partition log, markers) + goal bar ("lab đang theo dõi") | Run step list + Live panel |
| Explain | "Giải thích" prose, CLI, self-check questions | Reveal prose, Java props, CLI, self-check |

Constraints: Preact + htm through an import map, no build step, files embedded by `go:embed`. No new libraries.
The semantic colours below already carry Kafka meaning in the lessons and stay fixed.

## Tokens

All raw colour values live in the two token blocks of `web/static/ui.css`; components use tokens only.

### Colour

| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg` | `#f5f5f6` | `#09090b` | page background behind columns |
| `--surface` | `#ffffff` | `#111114` | columns, panels, cards |
| `--surface-2` | `#fafafa` | `#16161a` | wells: forms, goal bar, code blocks |
| `--surface-3` | `#efeff1` | `#1f1f24` | chips, inline code, hover fill |
| `--ink` | `#09090b` | `#f4f4f5` | primary text |
| `--ink-2` | `#3f3f46` | `#d4d4d8` | body and prose text |
| `--muted` | `#5e5e68` | `#a1a1aa` | secondary text; ≥ 5.5:1 on every surface |
| `--faint` | `#8a8a94` | `#71717a` | non-text only: icons, arrows, disabled; ≥ 3:1 |
| `--line` | `#e4e4e7` | `#26262b` | hairlines between regions (decorative) |
| `--line-2` | `#d4d4d8` | `#34343b` | button borders (the label identifies the button) |
| `--line-strong` | `#8a8a94` | `#71717a` | input, select and checkbox boundaries; ≥ 3:1 (WCAG 1.4.11) |
| `--primary` | `#2557d6` | `#6f9bff` | links, focus ring, current item, selection |
| `--primary-soft` / `--primary-ink` | `#eef3ff` / `#1d47b8` | `#172036` / `#b9cdff` | selected chips, "next offset" slot |
| `--fill` / `--fill-hover` / `--on-fill` | `#18181b` / `#2b2b30` / `#ffffff` | `#f4f4f5` / `#ffffff` / `#09090b` | primary buttons, brand mark, leader replica |

Semantic, unchanged in meaning:

| Token | Light | Dark | Meaning in the lab |
|---|---|---|---|
| `--ok` / `--ok-soft` | `#15803d` / `#edf8f1` | `#4ade80` / `#0f2418` | running broker, replica in ISR, goal met, PASS |
| `--warn` / `--warn-soft` | `#a8500a` / `#fdf6e7` | `#f5b04c` / `#2a1e0c` | limbo (killed, not yet fenced), pending send, redelivered |
| `--bad` / `--bad-soft` | `#b91c1c` / `#fdf1f1` | `#f87171` / `#2c1415` | dead broker, no leader, errors, destructive actions |
| `--commit` / `--commit-ink` | `#6d28d9` / `#6d28d9` | `#7c5bdc` / `#a78bfa` | committed offset of a group (fill / text) |
| `--on-commit` / `--on-bad` | `#ffffff` / `#ffffff` | `#ffffff` / `#09090b` | text on a commit or bad fill (white on dark-mode red is 2.8:1) |
| `--pos` | `#0e7490` | `#4cc3dc` | consumer position (polled, maybe not committed) |
| `--h` (per record) | `hsl(var(--h) 62% 52%)` | `hsl(var(--h) 55% 60%)` | key colour stripe; same key, same hue |

Contrast, computed with the WCAG 2 formula: muted text 5.6–6.4:1 light, 6.4–7.8:1 dark; status text on
its soft background ≥ 4.6:1; white on commit 7.1:1 light, 4.8:1 dark. `--commit` as text in dark is 3.9:1, hence
`--commit-ink`.

### Typography

Inter (system fallbacks) for UI and prose, JetBrains Mono for anything the learner could type or grep: offsets, keys,
topic and group names, commands, config keys. Metadata in sans with `tabular-nums`, not mono.

| Token | Size / line height | Weight | Use |
|---|---|---|---|
| `--fs-xs` | 11 / 16 | 500 | badges, legend, offset numbers in cells |
| `--fs-sm` | 12 / 18 | 400–600 | meta, field labels, table headers, timestamps |
| `--fs-md` | 13 / 19 | 400–600 | UI default: buttons, controls, lists, tables |
| `--fs-lg` | 14 / 20 | 600 | panel titles, loop step headings |
| `--fs-prose` | 16 / 1.65 (15 below 1680 px) | 400 | question, steps, reveal, self-check |
| `--fs-h2` | 18 / 1.3 | 600 | scenario title, empty states |
| `--fs-h1` | 24 / 1.25 (21 below 1360 px) | 600 | lesson title |

Weights are 400, 500, 600 only. Prose paragraphs are spaced 0.85 em; line length is tuned per breakpoint to 60–70
characters of real lesson text (measured in the browser, not with `ch`, because Inter's `0` is wider than average text).

### Spacing, radius, elevation, size

- Spacing on a 4 px grid: `--sp-1` 4, `--sp-2` 8, `--sp-3` 12, `--sp-4` 16, `--sp-5` 20, `--sp-6` 24, `--sp-8` 32.
  2 px is allowed only between tightly coupled marks (replica chips, stacked markers).
- Radius: `--r-sm` 4 (cells, chips, inline code), `--r` 6 (controls), `--r-lg` 8 (panels, cards), `--r-full` (badges, dots).
- Elevation is mostly flat. `--e1` (1 px, 4–5 % shadow) lifts panels off `--bg`; `--e2` for floating layers (open log
  drawer). In dark mode shadows are invisible, so elevation comes from the lighter surface step.
- Control heights: `--h-sm` 28, `--h-md` 32. Icon buttons are square at the same heights.
- Motion: `--t-fast` 120 ms for colour changes, `--t-med` 180 ms for disclosure. Animation is used only to report a
  change in the lab (a record arrives, a broker changes state); never for decoration or count-ups.
  `prefers-reduced-motion` disables all of it.

## Components

**Header (shared).** Brand, section nav (Bài học · Sân chơi · Kịch bản), live status, one page action. The section nav is
links/buttons with `aria-current`, the same markup on both pages, so the two pages read as one product.

**Buttons.** `default` (outline), `primary` (fill), `ghost`, `danger` (red text and icon at rest, red fill on hover).
Kill is `danger`; Stop is `default`; Start becomes `primary` when the broker is down, because it is then the only
useful action. Every button that calls the API shows a spinner and is disabled while its request runs
(`aria-busy`), which also prevents double submission.

**Two-step confirm.** Actions that lose data (delete topic, clear the playground, reset the scenario baseline) arm on
the first click ("Xoá orders?", red fill) and run on the second. They disarm after 4 s, on blur or on Esc. Kill and Stop
are not confirmed: they are the experiment, and Start undoes them.

**Inline status.** The result of an action shows next to the control that started it: errors in `--bad` under the
control group (`role="alert"`, cleared by the next attempt); success is the lab's own visible change (a cell appears, a
broker turns red). The console keeps the full history and the equivalent CLI command.

**Fields.** Label above (12 px, muted), control 32 px with `--line-strong` border, focused border `--primary` with a
soft ring, help text 12 px muted, error text 12 px `--bad`.

**Focus.** Every other focusable element gets a 2 px `--primary` outline at 2 px offset. It is an outline, not a
box-shadow, because current and armed states already use box-shadow and would hide a shadow ring (found by tabbing to
the current lesson step).

**Badges and dots.** Neutral, ok, warn, bad, primary; mono variant for partition ids. Dots carry state only next to a
text label, never alone.

**Panel.** Surface, hairline border, `--r-lg`, `--e1`. Header 44 px: title 14/600, sub 12 muted, actions right.

**Prose (`.prose`).** Body `--fs-prose`, `--ink-2`; `<b>` is a defined concept (ink, 600); `<i>` a contrast word;
`<code>` mono on `--surface-3`. Callouts for content that needs them: `<p class="callout tip">`,
`<p class="callout warn">` (a production pitfall), `<p class="callout concept">`; 3 px left rule, soft background,
no icon, no heading (see `WRITING_STYLE.md`). "Bạn đã đoán" is a neutral quote above the reveal.

**Code block (`.cli`).** `--surface-2`, hairline, mono 12/1.55, wraps, copy button.

**Lesson stepper.** One button per lesson that always shows its number; done adds a check badge and an ok border,
current gets a 2 px ink ring. `nav` with `aria-current="step"`, not a tablist (no arrow-key model to honour).

**Loop row.** Under the lesson title: Dự đoán · Làm · Quan sát · Giải thích, each with done/current state
(prediction written, goal met, explanation open). It shows where the learner is in the loop at a glance.

**Goal bar (Observe).** Sticky at the bottom of the lesson column so the objective never scrolls away: watching
(pulse) or met (ok background, "Đọc giải thích").

**Broker card.** One row: dot, "Broker n", leader count; state line; actions. The strip of three cards is a size
container: below 660 px it drops the action icons and keeps the words (Kill and Stop must not be told apart by an
icon), below 480 px it tightens padding and lets the leader badge wrap. Every action has an `aria-label` naming the
broker. A state change flashes the state line once.

**Partition row and log cell.** Gutter: P-id, leader, replica chips, record count, owners. Cells 72 × 56: offset in the
corner (11 px, muted, as lesson 1 describes it), key 12/600 mono, value 11 muted, key-hue stripe. Markers below the
cell: commit (filled purple), position (teal outline). The "next offset" slot stays.

**Table (`.table`).** Header 12/500 muted on a bottom hairline; rows 13 px, 32 px tall, hairlines; numbers right-aligned
`tabular-nums`; ids mono; state as coloured text. Not interactive, no zebra, no hover.

**Metric row.** Label above value, left-aligned, tabular numbers, separated by hairlines. Used for the scenario numbers
(DB rows, late rows, DLQ records). No tiles, no trends.

**Run step list.** Scenario steps with a status mark: running (spinner), pass (ok check), fail (bad cross) and the
observed value in muted text.

## Layouts

**Lessons** (`index.html`, mode `lessons`): header / lesson column · stage · producer-consumer column / console.
**Playground** (mode `free`): header / stage · producer-consumer column / console.
**Scenarios** (`classic.html`): header / scenario nav · scenario (prose) · live panel.

| Viewport | Lessons | Playground | Scenarios |
|---|---|---|---|
| ≥ 1680 (1920) | 600 · 1fr · 400 | 1fr · 420 | 260 · 1fr · 600 |
| 1360–1679 (1440) | 520 · 1fr · 360 | 1fr · 380 | 240 · 1fr · 560 |
| 1180–1359 (1280) | 480 · 1fr · 320 | 1fr · 340 | 220 · 1fr · 460 |
| 900–1179 (960, half screen) | 400 · 1fr, producer-consumer below the stage | 1fr · 320 | nav as a strip on top; 1fr · 440 |
| < 900 | one column, as today | one column | one column |

Measured characters per line of real text: lessons 70 / 65 / 61 / 50, scenarios 66 / 65 / 65 / 64 (1920 / 1440 /
1280 / 960). The scenario text column is capped at 31 × the prose size and centred.

At 960 px the lesson column keeps its own scroll, so the goal bar stays visible. The stage dissolves into the right
column (`display: contents`) so the cluster panel can stick to its top over the log and the producer-consumer column
alike; a sticky element only sticks inside its parent. The lesson line length drops to about 50 characters there;
that is the price of keeping the experiment on screen.

## CSS architecture

- `ui.css`: tokens (light, dark), base, and every shared component above. Linked first by both pages; the only source of
  colour, type, spacing and radius values.
- `pg.css`: layout and lab-specific pieces of the lessons/playground page (columns, lesson column, broker strip,
  partition log, producer-consumer column, console).
- `scenarios.css` (was `style.css`): layout of the scenarios page only.
- Breakpoints are viewport media queries at 1679, 1359, 1179, 899, 640 px. The broker strip and the topic panel use
  container queries because their width depends on the column, not the viewport.
- Class names stay the ones `pg.js` and `app.js` already render; no CSS framework, no cascade layers, no build.

## Implementation sequence

Each step is checked with Playwright screenshots against the running lab before the next one starts. Previews use a
static server that serves `web/static` from disk and forwards `/api` to the running lab-control, because the embedded
files only change after a rebuild.

1. `ui.css` foundation: tokens, base, shared components; `pg.css` reduced to layout; contrast fixes.
2. Shared header and section nav on both pages.
3. Lesson column: reading treatment, loop row, stepper, goal bar, prose and callouts.
4. Stage: broker strip, Kill/Stop/Start hierarchy, log cells, state flash.
5. Interaction layer: busy states, inline status, two-step confirm, flatter producer-consumer column, keyboard semantics.
6. Scenarios page on the shared system: header, nav, prose, tables, metrics, run step list, confirmed reset.
7. Responsive pass at 1920, 1440, 1280, 960 for all three views.
