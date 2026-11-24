---
name: Notebook Telemetry
colors:
  surface: '#f9f9ff'
  surface-dim: '#d5dae8'
  surface-bright: '#f9f9ff'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f1f3ff'
  surface-container: '#e9edfd'
  surface-container-high: '#e4e8f7'
  surface-container-highest: '#dee2f1'
  on-surface: '#171c26'
  on-surface-variant: '#434653'
  inverse-surface: '#2b303c'
  inverse-on-surface: '#edf0ff'
  outline: '#747685'
  outline-variant: '#c4c5d5'
  surface-tint: '#2c56c5'
  primary: '#234fbe'
  on-primary: '#ffffff'
  primary-container: '#4269d8'
  on-primary-container: '#f5f4ff'
  inverse-primary: '#b5c4ff'
  secondary: '#405c9e'
  on-secondary: '#ffffff'
  secondary-container: '#9cb7ff'
  on-secondary-container: '#294687'
  tertiary: '#515864'
  on-tertiary: '#ffffff'
  tertiary-container: '#6a707d'
  on-tertiary-container: '#f2f5ff'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#dbe1ff'
  primary-fixed-dim: '#b5c4ff'
  on-primary-fixed: '#00174d'
  on-primary-fixed-variant: '#003cac'
  secondary-fixed: '#dae2ff'
  secondary-fixed-dim: '#b1c5ff'
  on-secondary-fixed: '#001947'
  on-secondary-fixed-variant: '#264485'
  tertiary-fixed: '#dce2f1'
  tertiary-fixed-dim: '#c0c7d5'
  on-tertiary-fixed: '#151c26'
  on-tertiary-fixed-variant: '#404753'
  background: '#f9f9ff'
  on-background: '#171c26'
  surface-variant: '#dee2f1'
typography:
  headline-xl:
    fontFamily: Inter
    fontSize: 32px
    fontWeight: '600'
    lineHeight: 40px
    letterSpacing: -0.02em
  headline-xl-mobile:
    fontFamily: Inter
    fontSize: 26px
    fontWeight: '600'
    lineHeight: 34px
    letterSpacing: -0.01em
  headline-lg:
    fontFamily: Inter
    fontSize: 24px
    fontWeight: '600'
    lineHeight: 32px
    letterSpacing: -0.01em
  headline-md:
    fontFamily: Inter
    fontSize: 18px
    fontWeight: '600'
    lineHeight: 26px
    letterSpacing: -0.005em
  body-lg:
    fontFamily: Inter
    fontSize: 15px
    fontWeight: '400'
    lineHeight: 24px
  body-md:
    fontFamily: Inter
    fontSize: 13px
    fontWeight: '400'
    lineHeight: 20px
  body-sm:
    fontFamily: Inter
    fontSize: 12px
    fontWeight: '400'
    lineHeight: 18px
  mono-metric:
    fontFamily: JetBrains Mono
    fontSize: 22px
    fontWeight: '500'
    lineHeight: 28px
    letterSpacing: -0.02em
  mono-code:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: '400'
    lineHeight: 18px
  mono-tag:
    fontFamily: JetBrains Mono
    fontSize: 11px
    fontWeight: '500'
    lineHeight: 14px
    letterSpacing: 0.02em
  label-caps:
    fontFamily: Inter
    fontSize: 11px
    fontWeight: '600'
    lineHeight: 16px
    letterSpacing: 0.06em
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  gutter: 1rem
  gutter-desktop: 1.5rem
  margin: 1rem
  margin-tablet: 1.5rem
  margin-desktop: 2rem
  space-xs: 0.25rem
  space-sm: 0.5rem
  space-md: 0.75rem
  space-lg: 1.25rem
  space-xl: 2rem
---

## Brand & Style

This design system translates distributed systems observability into the tangible clarity of a precision laboratory notebook or engineering whiteboard. It rejects the dark, high-glow "hacker console" tropes in favor of an intentional, legible daylight environment. 

The aesthetic synthesizes:
- **Laboratory Minimalist Paper:** High-contrast, tactile technical readability reminiscent of engineering graph pads, archival technical reports, and wet-erase topology whiteboards.
- **Architectural Drafting Precision:** Crisp 1px rule boundaries, ruled-grid telemetry backgrounds, and hairline schematic connectors instead of blurred glow effects or soft floating elevations.
- **Tactile Ink Metaphor:** Structural components interact like ink deposited on cellulose—crisp, stable, high-legibility typographic anchors paired with purposeful technical blue annotations for real-time Kafka offsets, partition throughput, and broker telemetry.

## Colors

The palette establishes an authentic paper and ink medium calibrated for extended analytical focus:

- **Canvas & Paper Foundations:**
  - `bg-paper`: `#FAFAF7` (Warm drafting paper base; global viewport backdrop)
  - `bg-canvas`: `#FFFFFF` (Pure white workspace for telemetry cards, canvas panels, and inspection sheets)
  - `bg-surface-secondary`: `#F3F3EF` (Tonal recessed regions, inactive tabs, code block gutters)
  - `border-grid`: `#E8E8E2` (Hairline millimeter grid background lines)
  - `border-rule`: `#E2E2DC` (1px structural dividers, panel perimeters, table rules)

- **Ink Typography & Accents:**
  - `ink-primary`: `#252A35` (Deep graphite-black ink for headers, primary values, and body text)
  - `ink-muted`: `#686E7C` (Secondary annotations, timestamps, dormant telemetry labels)
  - `tech-blue`: `#4269D8` (Primary ink; active routes, cursor highlights, selected nodes, broker linkages)
  - `tech-blue-secondary`: `#7893D9` (Secondary telemetry series, inactive partition pins)
  - `tech-blue-wash`: `#EAF0FF` (Highlight wash, active selection fills, focus rings)

- **Semantic Telemetry Indicators:**
  - `status-success`: `#39845D` (Muted archival green; healthy consumer lag, in-sync replicas)
  - `status-warning`: `#B98132` (Muted drafting amber; rebalancing, high disk utilization)
  - `status-error`: `#C65353` (Muted archival red; partition under-replicated, offline broker)

## Typography

The typographic hierarchy enforces a disciplined division between contextual interface copy and technical metrics:

- **Primary UI (Inter):** Handles section heads, explanations, controls, and administrative actions. Letter spacing is slightly tightened on larger display tiers to reflect technical manual typesetting.
- **Telemetry & Engine Layer (JetBrains Mono):** Applies to all Kafka offsets, broker IDs, byte counters, log end offsets (LEO), lag figures, latency measurements, and interactive CLI prompts. Monospace figures ensure tabular numbers never shift horizontal layouts during streaming data updates.
- **Labels & Micro-Copy:** Header tags, table headings, and axis annotations use uppercase micro-labels (`label-caps`) with deliberate wide tracking for instant identification across busy stream matrices.

## Layout & Spacing

Layouts adhere to an architectural drafting grid based on precise 4px/8px increments.

- **Grid Architecture:** 
  - Desktop (1280px+): 12-column layout with 24px (`gutter-desktop`) spacing and 32px canvas margins.
  - Tablet (768px-1279px): 8-column layout with 16px gutters and 24px canvas margins.
  - Mobile (320px-767px): 4-column flow with 16px gutters and 16px lateral padding. Telemetry monitors stack vertically; complex matrix tables pivot to segmented offset cards.
- **Drafting Whiteboard Background Pattern:**
  - Canvases, cluster topologies, and timeline panels render an optional CSS background grid: 1px by 1px intersecting lines using `#E8E8E2` on a 16px by 16px repetition pattern.
- **Spacing Rhythm:**
  - Component internals use `space-xs` (4px) and `space-sm` (8px) for tightly grouped telemetry stats.
  - Card margins and card-to-card gaps utilize `space-lg` (20px) to balance high data density with clear breathing room.

## Elevation & Depth

This design system avoids drop shadows, diffusion blurs, and neon halos. Visual depth is established through physical, draftsmanship-inspired stratification:

- **Base Layer (Level 0):** `#FAFAF7` warm paper sheet. Grid lines are etched directly onto this plane.
- **Drawn Surfaces (Level 1):** Pure `#FFFFFF` panels defined strictly by a 1px solid `#E2E2DC` rule. There is no box shadow.
- **Embedded Regions (Inset):** Sub-monitors, consumer group tables, and code snippets use `#F3F3EF` with a 1px interior `#E2E2DC` stroke.
- **Active Technical Focus:** Active elements, focused nodes, or pinned broker items are brought forward via a 1px or 2px technical ink boundary `#4269D8` and a 2px `#EAF0FF` focus outline offset.
- **Overlays & Drawers (Level 2):** Modals, flyout telemetry diagnostics, and context inspectors use `#FFFFFF` with a crisp 1px `#252A35` outline and a flat 2px parallel mechanical offset shadow (`2px 2px 0px rgba(37, 42, 53, 0.08)`), preserving paper rigidity without blur.

## Shapes

The shape system expresses technical sheet metal and trimmed cardstock:

- **Corner Geometry:** A uniform 0.25rem (4px) corner radius across standard controls, metric containers, and panel borders provides subtle structural softening while maintaining the discipline of squared graph paper.
- **Badges & Terminal Chips:** System tags, partition status indicators, and consumer group pills maintain strict 2px to 4px corners; rounded pill shapes are banned to prevent playful or consumer-app associations.
- **Dividers & Hairlines:** Crisp 1px strokes (`#E2E2DC`) frame every container without exception.

## Components

### Buttons
- **Primary Technical Ink:** Background `#4269D8`, text `#FFFFFF`, border 1px solid `#4269D8`. Hover transitions to `#2F52B8`. Focus presents a 2px `#EAF0FF` halo.
- **Secondary Drafting Button:** Background `#FFFFFF`, text `#252A35`, border 1px solid `#E2E2DC`. Hover shifts background to `#F3F3EF` and border to `#252A35`.
- **Destructive/Halt Action:** Background `#FFFFFF`, text `#C65353`, border 1px solid `#C65353`. Hover transitions background to `#FDF2F2`.

### Chips & Telemetry Tags
- Compact inline items using `mono-tag` (JetBrains Mono, 11px).
- **Partition/Broker Tag:** Background `#F3F3EF`, text `#252A35`, border 1px solid `#E2E2DC`.
- **Healthy Tag:** Background `#FFFFFF`, text `#39845D`, border 1px solid `#39845D`. Accompanied by a 6px solid green indicator dot.
- **Lag Alert Tag:** Background `#FFFFFF`, text `#B98132`, border 1px solid `#B98132`.

### Input Fields & Search Bars
- Background `#FFFFFF`, border 1px solid `#E2E2DC`, text `#252A35`, font Inter 13px.
- Placeholder text in `#686E7C`.
- Focus state switches border directly to 1px solid `#4269D8` with an inner 1px box shadow highlight in `#EAF0FF`.
- CLI and filter inputs switch to `JetBrains Mono` with leading monospace prompt characters (`>`).

### Metric & Telemetry Cards
- Surface `#FFFFFF` encased in 1px `#E2E2DC`.
- Top sub-header carries `label-caps` typography (`#686E7C`) followed by a primary numeric block in `mono-metric` (`#252A35`).
- Secondary delta indicators (+4.2k msg/sec) sit flush along the lower margin with hair-rule dividers separating metric cells.

### Lists & Partition Tables
- Alternating row styling is replaced by clean 1px bottom rules (`#E2E2DC`).
- Row hover states shift row background subtly to `#F3F3EF`.
- Headers are set in uppercase `label-caps`, fixed height of 36px, with sticky containment against grid scrolls.

### Checkboxes & Toggle Switches
- Checkboxes: 16px square, 2px corner radius, 1px solid `#E2E2DC` boundary. Checked state fills `#4269D8` with a sharp, geometric white checkmark.
- Toggles: Mechanical drafting switches. Recessed trough in `#E8E8E2`, 14px square white thumb with a 1px border `#252A35`, snapping without bounce effects.

### Isometric & Topology Schematic Connectors
- Kafka broker nodes, partition paths, and consumer routing sketches use crisp 1px technical lines in `#4269D8`.
- Inactive routes use dotted 1px lines in `#E2E2DC`.
- Vector directional arrows are sharp, unrounded drafting carats.