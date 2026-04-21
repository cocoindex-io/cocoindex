# Docs Diagrams

Inline, componentized SVG diagrams for the docs site. This directory is the
**single source of truth** for any diagram embedded in docs pages — old
`/public/img/**/*.svg` files should be treated as legacy and replaced here
over time.

## Why components, not static SVGs

Static SVGs exported from tools (Excalidraw, Figma, Sketch) are opaque —
editing requires round-tripping through the tool, and there's no shared
style. Homepage-style inline SVG (`cocoindex.github.io/src/pages/index.astro`)
is the model we follow: authored directly in `.astro`, styled with shared
CSS classes, animated with `@keyframes` + hover selectors.

Benefits we get for free:

- Single palette (CSS vars) — change once, update everywhere
- Animations via CSS `:hover` — no JS, SSR-friendly
- Labels/props typed at the component boundary
- Diffs are readable (git review works)

## Shape semantics

Shape carries meaning. Pick the primitive by what the element *is*, not
by how it looks:

| Shape | Meaning | Primitive |
|---|---|---|
| Sharp rectangle | **Data** (a file, a chunk, a row) | `DataBox` |
| Round-cornered rectangle | **Subsystem / logic** (Split, Embed, Drive Folder, Vector Database) | `LogicBox` |
| Peach round-cornered container | A **CocoIndex App** (hosts other elements) | `AppContainer` |
| Cream round-cornered container with header | A **Processing Component** (hosts other elements) | `ProcessingComponent` |
| Bullet / capsule (flat left, rounded right) | **Target state** (vector, output file, db row) | `TargetBullet` |

Annotations (attached to the above, not standalone):

| Glyph | Prop | Position | Meaning |
|---|---|---|---|
| Maroon ribbon | `memoized={true}` on `LogicBox` / `ProcessingComponent` | top-right, bisecting top edge | Memoized function/component |
| Green disc + check | `status="cache-ready"` on `LogicBox` / `ProcessingComponent` | top-left, bisecting both top and left edges | Memo hit — result reused |
| Coral disc + arrow | `status="refreshing"` on `LogicBox` / `ProcessingComponent` | top-left, bisecting both top and left edges | Memo miss — re-executing |
| Thick palm-green border | `highlight={true}` on any shape primitive | whole box | "The thing the surrounding prose is talking about" |

## Directory layout

```
src/components/diagrams/
├── README.md                       # this file
├── diagrams.css                    # palette vars, shared classes, @keyframes
├── primitives/
│   ├── DiagramFrame.astro          # outer <svg> wrapper (viewBox, aria-label)
│   ├── ShapeGroup.astro            # shared base for shape primitives (see below)
│   ├── DataBox.astro               # sharp rectangle — data
│   ├── LogicBox.astro              # round rectangle — subsystem / logic
│   ├── TargetBullet.astro          # bullet — target state
│   ├── ProcessingComponent.astro   # round container, slot in local coords
│   ├── AppContainer.astro          # peach container, CocoIndex App
│   ├── MemoMark.astro              # memo glyph (via `memoized` prop)
│   ├── StatusBadge.astro           # cache-ready / refreshing glyph (via `status` prop)
│   ├── FlowArrow.astro             # animated dashed arrow
│   └── Connector.astro             # static binding line (no arrowhead)
└── concepts/
    ├── AppOverview.astro           # Source → App → Target state (core_concepts)
    ├── AppExample.astro            # PDF → markdown overview (shared)
    ├── AppDef.astro                # App binds main function with args (quickstart)
    ├── ComponentPerFile.astro      # Drive → per-file PC (Convert → a.md) → Drive
    ├── FileProcess.astro           # thin wrapper around ComponentPerFile w/ highlight
    ├── ComponentsFanout.astro      # thin wrapper around ComponentPerFile w/ highlight
    └── ComponentWithChunks.astro   # Drive → App(PC→Split→chunks→Embed→vectors) → VDB
                                    #   accepts `memoized` and `scenario` props
```

## The `ShapeGroup` base

Every shape primitive (`DataBox`, `LogicBox`, `TargetBullet`,
`ProcessingComponent`) wraps its content in a shared `ShapeGroup` that
handles:

- The outer `<g transform="translate(x, y)">`
- Base class + state modifier + highlight modifier composition
- Native `<title>deleted</title>` tooltip when `state="removed"`
- Optional `StatusBadge` (top-left) and `MemoMark` (top-right), both
  auto-suppressed when `state="removed"` — a deleted thing has no
  cache status and nothing to memoize

When adding a new shape primitive, compose it on top of `ShapeGroup`
rather than re-implementing the wrapper `<g>` + state logic. Example:

```astro
---
import ShapeGroup from './ShapeGroup.astro';
// ...
---
<ShapeGroup x={x} y={y} w={w} baseClass="dg-myshape"
            state={state} highlight={highlight}
            memoized={memoized} status={status}>
  <rect class="dg-box" x="0" y="0" width={w} height={h} rx="8" />
  <foreignObject ...><div class="dg-fo-label">{label}</div></foreignObject>
</ShapeGroup>
```

## Delta states

The `state` prop is how primitives indicate "what happened to me
during this run". Applied by `ShapeGroup` as a class on the wrapper
`<g>`. All three delta states share the same motion vocabulary — a
border pulse on `.dg-root:hover`.

| `state` | Fill / stroke | Label treatment | Hover tooltip | Purpose |
|---|---|---|---|---|
| `idle` (default) | cream + maroon | normal | — | Unchanged |
| `new` | palm-green | normal | — | Inserted this run |
| `updated` | gold | normal | — | Changed in place this run |
| `removed` | pink | struck through + muted | `deleted` | Deleted this run |
| `changed` | thin pink (no fill pulse) | normal | — | Fingerprint-propagation signal (narrow, for `FingerprintPropagation.astro` only) |

**Critical: state rules use the direct-child combinator (`> .dg-box`)
in `diagrams.css`.** If you ever write a new state rule, use `>`, not
descendant — otherwise a state class on a container (e.g. a PC with
`pcState="updated"`) will cascade into every nested `.dg-box` inside
it (chunks, embeds, vectors) and paint them all gold. Each shape
primitive owns its own state on its own wrapper.

Gold is a diagram-local var (`--dg-gold: #D4A835`) defined on
`.dg-root` in `diagrams.css` so it doesn't leak outside diagrams.

## Cache status badges

`status` on `LogicBox` / `ProcessingComponent` marks memoization
behavior for a specific element within a run:

- `cache-ready` — green disc with cream check. On hover, the check
  **writes itself in from left to right** (stroke-dasharray draw,
  ~0.6s), holds ~1.2s, snaps back for the next loop.
- `refreshing` — coral disc with cream circular arrow. On hover, the
  arrow **spins continuously**. Static page stays calm.

Both suppressed when `state="removed"` (since a deleted thing has no
live cache status). Badges sit at the top-left corner, bisected by
both the top and left edges — mirror of `MemoMark` at top-right.

## The `scenario` pattern for multi-state diagrams

`ComponentWithChunks` takes a `scenario?: Scenario` prop describing
per-row / per-chunk overrides:

```mdx
<ComponentWithChunks memoized={true} scenario={{ rows: [
  { file: 'a.md', pcStatus: 'cache-ready', chunks: [
    { label: 'chunk1', vectorLabel: 'vector1', embedStatus: 'cache-ready' },
    { label: 'chunk2', vectorLabel: 'vector2', embedStatus: 'cache-ready' },
  ] },
  { file: 'b.md', fileState: 'updated', pcStatus: 'refreshing', pcState: 'updated', chunks: [
    { label: 'chunk3', vectorLabel: 'vector3', embedStatus: 'cache-ready' },
    { label: 'chunk4', vectorLabel: 'vector4', state: 'removed' },
    { label: 'chunk5', vectorLabel: 'vector5', state: 'new' },
  ] },
]}} />
```

Inline the scenario directly in the `.mdx` next to the prose that
describes it. Do NOT create one `.astro` wrapper per scenario — the
scenario is content, not a reusable component.

## Palette

Pulled from `src/styles/globals.css` (already matches the brand guidelines):

| CSS var | Hex | Use |
|---|---|---|
| `--coral` | `#BE5133` | Flow arrows, peach containers, "refreshing" badge |
| `--peach` | `#E59A63` | App/Processing Component fills (tinted) |
| `--palm` | `#27E62B` | `new` state, `cache-ready` badge, `highlight` prop |
| `--pink` | `#FB6A76` | `removed`/`changed` state, fingerprint invalidation |
| `--maroon` | `#532638` | Primary strokes, `MemoMark` fill |
| `--maroon-ink` | `#2A121B` | Body ink |
| `--cream` | `#FCF3D8` | Default fills, badge inner marks |
| `--dg-gold` | `#D4A835` | `updated` state (diagram-local) |
| `--paper` | `#FBF6E8` | Diagram background |

Do not hardcode hex values in diagrams. Always use the vars.

## Shared CSS classes (quick reference)

- `dg-root` — outermost `<svg>` wrapper; container for `:hover` rules
- `dg-box` — base stroked rect/path (cream + maroon)
- `dg-box--component` — peach-tinted fill + coral stroke
- `dg-box--app` — App container fill
- `dg-box--muted` — faint dashed container for "outer frame" illustrations
- `dg-label` / `dg-fo-label` — SVG text / foreignObject HTML label
- `dg-state-{new,updated,removed,changed}` — delta states (direct-child scoped)
- `dg-status-badge--ok` / `--refresh` — status badge variants
- `dg-highlight` — thick palm border for "currently discussed"
- `dg-flow` — animated dashed arrow
- `dg-connector` — static binding line
- `dg-pulse` — pulsing dot
- `dg-step-N` (N=1–6) — progressive-reveal stagger

## Animation discipline

All diagrams are **idle by default**; motion lives behind
`.dg-root:hover`. This matches the homepage pattern and keeps the
static page calm.

Active animations on hover:

- `dg-flow` — dashed arrow drift
- `dg-pulse` — pulsing dots
- `dg-state-{new,updated,removed}` — border-width pulse
  (`dg-delta-pulse`), shared so all three deltas read with one
  vocabulary
- `dg-status-badge--ok` — check writes in left-to-right, holds, resets
- `dg-status-badge--refresh` — spinning arrow

`@media (prefers-reduced-motion: reduce)` zeros all of the above.

## Using a diagram in an .mdx page

```mdx
---
title: Core Concepts
---
import ComponentWithChunks from '/src/components/diagrams/concepts/ComponentWithChunks.astro';

## Processing Component

<ComponentWithChunks />
```

Astro supports component imports in `.mdx` natively (via `@astrojs/mdx`
in `astro.config.mjs`). Prefer absolute import paths rooted at
`/src/...` for stability across docs pages at any depth.

## Writing a new diagram

1. **Layout first.** Sketch positions in a config object at the top of
   the `.astro` file. Prefer `viewBox` width ~720 (matches docs content
   column). Derive container sizes from content, not the other way
   around (keeps padding balanced).
2. **Compose primitives inside `DiagramFrame`.** Layout math stays in
   the composing diagram file, not in primitives.
3. **Use CSS vars and shared classes** for colors, strokes, typography.
   No inline `fill="#..."` unless semantically one-off.
4. **Use the `state` / `status` / `highlight` props** for anything
   animated or state-dependent. Never re-implement wrapper `<g>` +
   state class logic — compose on `ShapeGroup` instead.
5. **Native tooltips via `<title>`** for hover hints — Astro passes
   through to SVG; browsers render it as a native tooltip.
6. **Accessibility.** `DiagramFrame` accepts `title` and `desc` props
   that become `<title>` / `<desc>` children of `<svg>`.

## Discipline

- **Keep per-diagram SVG under ~30 lines** of inline markup. Larger →
  factor a repeated group into a primitive.
- **No always-on animations.** If it moves, it moves on `.dg-root:hover`.
  (See `dg-flow`, `dg-pulse`, `dg-status-badge--*`, `dg-state-*` for
  the canonical pattern.)
- **No runtime JS** unless the diagram genuinely needs interaction
  beyond hover (scrubber, click-to-step). In that case, make only that
  one diagram a React island and reuse the same SVG primitives by
  porting them — do not load a React tree for a hover animation.
- **`prefers-reduced-motion`.** Any new `@keyframes` rule must be added
  to the reduced-motion media query at the bottom of `diagrams.css`.

## Legacy

Old `/public/img/concept/*.svg` and `/public/img/quickstart/*.svg`
should be deleted as their component replacements are built. Do not
edit those SVGs going forward.
