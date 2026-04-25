---
name: cocoindex-diagrams
description: This skill should be used when creating, editing, or reviewing inline SVG diagrams for the CocoIndex docs site (anything under docs/src/content/docs/**). It encodes the component-based primitive system under docs/src/components/diagrams/, the palette + shape semantics, the preview-and-verify loop using headless Chrome with the /docs base path, and the pitfalls that cost iterations to discover. Trigger phrases include "add a diagram to the docs", "edit this diagram", "replace the SVG in core_concepts", "make a diagram for processing_component", "check how the diagram looks".
---

# CocoIndex Docs Diagrams

Inline SVG diagrams in the docs are authored as Astro components under
`docs/src/components/diagrams/`, composed from shape-semantic primitives
that share a palette and styling. Static `.svg` files exported from
design tools are legacy — always build new diagrams with the primitives.

## When this skill applies

Apply this skill when:

- Adding a new diagram to a docs page (`.mdx` under `docs/src/content/docs/`).
- Editing or replacing an existing diagram component.
- Reviewing a rendered diagram for layout or style issues.
- Reworking a legacy `<img src="/docs/img/...svg">` reference.

Do not apply for:

- Marketing-site diagrams under `cocoindex.github.io/` (different style, different primitives).
- General SVG editing outside `docs/src/components/diagrams/`.

## The canonical reference

Read `docs/src/components/diagrams/README.md` **first**. It documents the
shape vocabulary, palette vars, shared CSS classes, animation conventions,
directory layout, and MDX embedding pattern. This SKILL.md assumes that
reference is current — do not duplicate it here.

Then consult the three references bundled with this skill for the
session-specific knowledge that isn't in the project README:

- [references/workflow.md](references/workflow.md) — the preview loop (headless Chrome + base-path gotcha), how to crop and view output, rebuild discipline.
- [references/pitfalls.md](references/pitfalls.md) — the traps that repeatedly cost iterations (dg-step opacity, color unification, magic-number insets, labels without auto-wrap).
- [references/layout-patterns.md](references/layout-patterns.md) — layout idioms (horizontal arrows, symmetric padding, compactness, bindings vs arrows).

## Core procedure

When asked to create or edit a diagram, proceed in this order:

### 1. Read the reference materials

Load `docs/src/components/diagrams/README.md` plus the three references
above. Skim, don't memorize — refer back when designing.

### 2. Design the layout before writing code

Sketch positions with concrete numbers in a config object at the top of
the `.astro` file. Prefer absolute coordinates with a `viewBox` sized to
content, and use named constants for box dimensions and column offsets.

**Derive container size from content, not the other way around.** Start
from inner widths, gaps, and pad; compute `APP_W = sum(cols) + (n-1) *
GAP + 2 * APP_PAD_X`. Downstream siblings (Target System, Drive Folder
right) reference `APP.x + APP_W`, not a hardcoded x. Same for vertical:
pick row centers so top-pad == bottom-pad. This keeps padding balanced
on all four sides as you iterate. See [references/layout-patterns.md](references/layout-patterns.md).

### 3. Compose from primitives

Use the shape-semantic primitives:

| Shape | Meaning | Primitive |
|---|---|---|
| Sharp rectangle | Data (file, chunk, row) | `DataBox` |
| Round-cornered rectangle | Subsystem / logic | `LogicBox` (`memoized` + `status` props, slot) |
| Cream container with header | A Processing Component | `ProcessingComponent` (slot in local coords; `memoized` + `status`) |
| Peach container with header | A CocoIndex App | `AppContainer` (slot in local coords) |
| Bullet (flat left, rounded right) | Target state | `TargetBullet` |
| Animated dashed arrow with head | Flow / causation | `FlowArrow` |
| Static dashed line, no head | Binding / identity | `Connector` |

Delta state + cache status annotations (orthogonal, on any shape):

| Prop | Values | Visual |
|---|---|---|
| `state` | `new`, `updated`, `removed`, `changed`, `idle` | Palm-green / gold / pink / thin-pink fill (state cascade is prevented by direct-child CSS combinator — see pitfalls). |
| `status` | `cache-ready`, `refreshing` | Top-left badge. Cache-ready writes its check in on hover; refreshing spins on hover. Both suppressed when `state="removed"`. |
| `highlight` | `true` | Thick palm border for "currently-discussed element" |

New shape primitives should **compose on top of `ShapeGroup`**, not
re-implement the wrapper `<g>` + state class + title + badge logic.

### 4. Render labels with foreignObject

All label-bearing primitives (`DataBox`, `LogicBox`, `ProcessingComponent`,
`TargetBullet`) use `<foreignObject>` with a flex-centered `<div
class="dg-fo-label">` so the browser auto-wraps labels against the box
width. Just pass `label="..."` — never pre-split into manual lines.

### 5. Preview and verify before reporting done

Run the preview script bundled with this skill:

```bash
scripts/preview.sh <docs-slug>
# Example: scripts/preview.sh programming_guide/core_concepts
```

The script handles: building Astro, mirroring `dist/` into a temp path
that matches the site's `/docs` base URL (critical — see pitfalls),
spinning up a local HTTP server on a free port, screenshotting with
headless Chrome, and printing paths to PNG crops. Read the PNGs with
the `Read` tool to self-check before reporting the diagram done.

Do NOT report a diagram complete based solely on "the build compiled".
Always render and look. Small issues like overlapping labels, dimmed
rows, or wrong arrow styles are invisible from code alone.

### 6. Iterate on visual feedback

Common iterations (see pitfalls for full details):

- Elements faded at ~35% opacity → `dg-step` leaking onto static rows; remove it.
- Boxes rendering black in the preview → CSS base-path mismatch (not a code bug).
- Labels overlapping → `<foreignObject>` not used, or box too narrow for the string.
- Arrows to wrong targets → mixing absolute vs. PC-local coordinate space.

## MDX embedding pattern

Import with an absolute path from the site root so it works at any docs
depth:

```mdx
---
title: Core Concepts
---
import ComponentWithChunks from '/src/components/diagrams/concepts/ComponentWithChunks.astro';

## Processing Component

<ComponentWithChunks />
```

## Discipline

- **Shape carries meaning.** Pick primitives by semantics, not "what looks right".
- **No manual line-splitting.** If a label overflows, widen the box or shorten the label.
- **Absolute coords for outer layout; local coords inside slotted containers.** `ProcessingComponent`'s slot renders children with (0, 0) = container top-left.
- **All flow arrows use the same color.** Default coral; `variant="palm"` / `"muted"` only when semantically meaningful.
- **Bindings are silent.** `Connector` (static, dashed, no arrowhead) for "X is bound to Y" — Drive Folder → file, vector → Vector Database.
- **Padding balanced on ALL sides.** Left == right inside every container, and top == bottom. Derive the container's width/height from its content (`APP_W = sum(cols) + (n-1)*GAP + 2*APP_PAD_X`); downstream siblings reference `APP.x + APP_W`, not a hardcoded x. See [references/layout-patterns.md](references/layout-patterns.md) "Balanced padding on all sides".
- **Prefer compactness.** Diagrams should read well at docs column width; stretch only when a visual story demands it.
- **Never use `dg-step` for static content.** It's opacity-35% by default and only lights up on hover — reserved for progressive-reveal narratives (e.g. "panel 1 → 2 → 3").
- **All animations are idle by default, active on `.dg-root:hover`.** Flow-drift, delta-pulse, check-draw, refresh-spin — each gated the same way. No always-on motion on a static page. When adding a new `@keyframes` rule, register it in the `@media (prefers-reduced-motion: reduce)` block at the bottom of `diagrams.css`.
- **State rules in CSS use the direct-child combinator (`.dg-state-X > .dg-box`)**, not descendant. A state class on a container (PC with `pcState="updated"`) would otherwise cascade into every nested `.dg-box`. Each shape primitive owns its own state on its own `ShapeGroup`-wrapped `<g>`.
- **Compose new shape primitives on `ShapeGroup`**, not from scratch. It owns the wrapper `<g>`, state/highlight class composition, `<title>deleted</title>` tooltip, and the `MemoMark`/`StatusBadge` badges (with suppression when removed). Never re-implement any of that.
- **Multi-state scenario diagrams take a `scenario` prop**, inlined in the MDX next to the prose. Don't create a `.astro` wrapper per scenario — the scenario is content, not a reusable component. See the `<ComponentWithChunks scenario={{ rows: [...] }} />` pattern.

## Iteration expectations

Expect 2–4 preview cycles for any non-trivial diagram. After the first
render, opacity/color/overlap issues almost always surface that weren't
visible from code inspection. Budget for the loop — don't try to land
the diagram in one shot.

## Starter template

See [assets/starter.astro](assets/starter.astro) for a minimal
shape-semantic diagram skeleton. Copy and adapt rather than writing
from scratch.
