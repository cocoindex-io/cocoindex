# Pitfalls that cost iterations

A record of mistakes encountered while building the current diagram set.
Each entry: the symptom, the cause, and the fix.

## All diagram boxes render as solid black

**Symptom**: Diagrams look like black rectangles in screenshots; text is
dimly visible but all fills are black.

**Cause**: The docs site uses `base: '/docs-v1'` in `astro.config.mjs`,
so built HTML references CSS at `/docs-v1/_astro/*.css`. A naïve
`python3 -m http.server` from `dist/` serves CSS at `/_astro/...`
instead, returning 404. With no stylesheet, `var(--cream)`, `var(--coral)`,
etc. are undefined; SVG `fill` falls back to black.

**Fix**: Serve from a parent dir containing a `docs-v1/` symlink or
copy of `dist/`, so `/docs-v1/_astro/...` resolves. The
`scripts/preview.sh` script handles this automatically.

## Rows/content faded at 35% opacity

**Symptom**: Multiple rows in a diagram all appear dim; only show at
full opacity when the user hovers.

**Cause**: `dg-step` + `dg-step-N` classes. These are designed for
progressive-reveal narratives (e.g., a 3-panel "step 1 → 2 → 3") where
each step should fade in on hover with stagger. If applied to parallel
rows that should all be visible statically, they fade by default.

**Fix**: Remove `dg-step` from the `<g>` wrapping the row. Only use
`dg-step` when hover-driven reveal is intentional.

## Labels overflow / clip inside narrow boxes

**Symptom**: "Split into chunks" sticks out past the box edges.

**Cause**: Early versions used SVG `<text>` which does not wrap. Manual
line-splitting via `lines={['Split into', 'chunks']}` worked but was
awkward.

**Fix**: All label-bearing primitives (`DataBox`, `LogicBox`,
`ProcessingComponent`, `TargetBullet`) now use `<foreignObject>` with a
flex-centered `<div class="dg-fo-label">`. The browser wraps against
the box width. Just pass `label="..."`.

## Magic-number insets scatter across call sites

**Symptom**: `<MemoMark x={PC_X + 14} y={row.pcY + 4} size={12} />` and
`<MemoMark x={embedX + 6} y={mid - EMBED_H / 2 - 2} size={10} />` —
different offsets, different sizes, invisible coupling to container
dimensions.

**Cause**: Inset values baked into call sites instead of the primitive.

**Fix**: The primitive owns its inset + size. Callers pass the
container's reference corner (top-right for `MemoMark`). The primitive
internally does `translate(x - INSET_X - w, y)`. Same for
`ProcessingComponent`'s header and memo mark — the container primitive
knows its own dimensions and handles all internal positioning.

## Source vs target color accidentally diverged

**Symptom**: Drive Folder in palm-green, Vector Database in coral —
suggests a distinction that doesn't exist in CocoIndex.

**Cause**: Early `LogicBox` had `variant="source"` / `variant="target"`
modifiers with different fills. Kept around from homepage conventions
that don't apply to docs diagrams.

**Fix**: One neutral cream + maroon fill for all non-container logic
boxes. Shape (not color) carries the semantic distinction —
`TargetBullet` is visually different from `LogicBox` because of its
bullet shape, not because of color. Only App containers get the peach
tint for visual grouping.

## Arrows at different heights hit the wrong target

**Symptom**: `vector1` at y=93 points to Vector Database, but the arrow
line crosses over `vector2` at y=147 because both converge to VDB
center.

**Cause**: Drawing all arrows to the single y-center of the destination
box.

**Fix**: Keep arrows horizontal — draw each to the destination's left
edge at the *source's* y-coordinate. This only works when destinations
are tall enough to accept multiple horizontal entries. See
[layout-patterns.md](./layout-patterns.md).

## Arrowheads on binding lines

**Symptom**: Drive Folder → file and vector → Vector Database both have
arrowheads and moving dashes, making them feel like causal flow.

**Cause**: Using `FlowArrow` everywhere instead of distinguishing flow
from binding.

**Fix**: `Connector` (static, dashed, no arrowhead) for bindings;
`FlowArrow` only for causal flow. A binding is "X is bound to Y" —
Drive Folder → file (the file IS from the folder), vector → Vector
Database (the vector IS written to the DB).

## MemoMark fills solid black, looks too heavy

**Symptom**: Memo marks render as dark solid maroon ribbons that
visually dominate small boxes.

**Fix**: Use coral outline + translucent coral fill
(`fill: color-mix(in oklab, var(--coral) 22%, transparent)`), not a
solid color. Reads over any background and matches the CocoIndex accent
palette.

## Asymmetric padding inside a container

**Symptom**: Content hugs the left side of an App/ProcessingComponent
container with a big empty gap on the right. Or the first row hugs the
top while the last row has a large gap below.

**Cause**: Hardcoding `APP.w` / `APP.h` to a round number and then
placing content starting at a small `APP_PAD`. The content fits but
leaves whatever remainder on the right/bottom.

**Fix**: Derive the container size from the content, not the other way
around:

```astro
const APP_PAD_X = 24;
const APP_W = FILE_W + GAP + PC_W + APP_PAD_X * 2;  // left pad == right pad
const APP = { x: 140, y: TOP_Y, w: APP_W, h: TOP_H };
```

Any downstream sibling (Target System, Drive Folder right) must then be
positioned relative to `APP.x + APP_W`, not a hardcoded x. Same
principle vertically: compute row y-centers so top-pad == bottom-pad.

See [layout-patterns.md](./layout-patterns.md) "Balanced padding on all
sides" for the full idiom.

## State class on a container cascades into its children

**Symptom**: A `ProcessingComponent` with `pcState='updated'` paints
*every* nested `.dg-box` (chunks, embeds, vectors) gold — even the
ones explicitly left at `state='idle'`.

**Cause**: The state CSS rules were written with the descendant
combinator: `.dg-state-updated .dg-box { stroke: gold; }`. The
container's state class matches, and its descendant `.dg-box`
elements inherit the style.

**Fix**: Use the direct-child combinator `>` so the rule only applies
to the box element on the same wrapper `<g>` that owns the state:

```css
.dg-state-new > .dg-box    { ... }
.dg-state-updated > .dg-box { ... }
.dg-state-removed > .dg-box { ... }
.dg-state-changed > .dg-box { ... }
```

Each shape primitive (built on `ShapeGroup`) has its own wrapper
`<g>` with its own state class and its own direct-child `.dg-box`.
With `>`, states never cascade.

## New state visuals that don't read against a stateful container

**Symptom**: A `state="new"` chunk inside a `state="updated"` PC
looks identical to its sibling idle chunks — palm-green stroke is
invisible against gold-tinted PC background.

**Cause**: Default stroke-width (1.4) makes colored state strokes too
thin to pop against a tinted parent.

**Fix**: The `dg-state-*` rules in `diagrams.css` bump
`stroke-width` to 2.2 for all delta states, and the hover
`dg-delta-pulse` keyframes push it to 4.2 for extra visibility. If
you add a new delta state, match the pattern.

## Always-on animations distract on static pages

**Symptom**: A spinning icon (refresh badge) keeps spinning even when
the user isn't looking at that diagram — makes the page feel busy.

**Fix**: Every animation in `diagrams.css` is **idle by default,
active only on `.dg-root:hover`**. Includes `dg-flow`, `dg-pulse`,
`dg-delta-pulse`, `dg-spin` (refresh badge), `dg-check-draw`
(cache-ready check). When adding a new animation, gate it the same
way — and register it in the `@media (prefers-reduced-motion: reduce)`
override at the bottom of the stylesheet.

## Reporting complete without looking

**Symptom**: "The build succeeded, diagram done." Then user screenshots
show overlaps / black boxes / faded content.

**Fix**: Always run `scripts/preview.sh` and `Read` the PNG before
reporting. A clean `npm run build` proves only that the Astro
components compile.
