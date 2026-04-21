# Layout patterns

Idioms for composing legible diagrams from the shape-semantic primitives.

## Config block at top

Every non-trivial diagram starts with a config block of named constants.
Don't sprinkle magic numbers.

```astro
---
const VB_W = 980, VB_H = 380;
const TOP_Y = 30, TOP_H = 330;

const DRIVE = { x: 20,  y: TOP_Y, w: 110, h: TOP_H };
const APP   = { x: 160, y: TOP_Y, w: 664, h: TOP_H };
const VDB   = { x: 844, y: TOP_Y, w: 110, h: TOP_H };

const PC_W = 510, PC_H = 128;
const ROW1_CY = 130, ROW2_CY = 268;
// ...
---
```

Changing one constant re-flows the diagram; no hunting through JSX.

## Horizontal arrows by default

When source and destination both have a vertical extent, route arrows
horizontally rather than angled. Looks more polished; reads faster.

To enable this, align sibling containers (Drive Folder, CocoIndex App,
Vector Database) at the same `y` and `height`. Then row-specific
arrows enter/exit at the row's y-coordinate and stay level.

```astro
<FlowArrow d={`M ${DRIVE.x + DRIVE.w} ${row.rowCY} L ${FILE_X} ${row.rowCY}`} />
```

## Bindings vs flow

- **FlowArrow**: causal flow (A produces B, A transforms to B). Coral
  dashed arrow with arrowhead and subtle drift animation.
- **Connector**: binding / identity (A is bound to B, A writes into B).
  Static, dashed, no arrowhead.

Concrete rules:

- Drive Folder → file: binding (the file IS from the folder). `Connector`.
- file → Processing Component: flow (the PC processes the file). `FlowArrow`.
- Split → chunk: flow. `FlowArrow`.
- chunk → Embed: flow. `FlowArrow`.
- Embed → vector: flow. `FlowArrow`.
- vector → Vector Database: binding (the vector IS written to the DB).
  `Connector`.

## Balanced padding on all sides

Inside every container — AppContainer, ProcessingComponent, or any
LogicBox with a slot — keep **padding equal on all four sides**: left ==
right, and top == bottom. Asymmetric padding is the single most common
visual smell. The fix is always the same: derive the container size
from content, not the other way around.

### Horizontal: derive container width from content

**Wrong** (guess container width, scatter content inside):

```astro
const APP   = { x: 140, y: TOP_Y, w: 700, h: TOP_H };  // guessed
const FILE_DX = 24;
const PC_DX   = FILE_DX + FILE_W + 34;
const PC_W    = 360;
// Right padding: 700 - (PC_DX + PC_W) = 256. Way more than left 24.
```

**Right** (compute container width from what it actually holds):

```astro
const APP_PAD_X = 24;
const FILE_W = 62;
const PC_W   = 360;
const FILE_TO_PC_GAP = 34;
const APP_W = FILE_W + FILE_TO_PC_GAP + PC_W + APP_PAD_X * 2;
// APP_W = 484. Left pad 24 = right pad 24 by construction.

const APP     = { x: 140,         y: TOP_Y, w: APP_W, h: TOP_H };
const DRIVE_R = { x: APP.x + APP_W + 20, y: TOP_Y, w: 100, h: TOP_H };
```

For a horizontal content row:

```
[pad_x] col1 [gap] col2 [gap] … coln [pad_x]
```

Set `container_w = sum(cols) + (n-1) * gap + 2 * pad_x`. Then left == right by
construction; adding or removing columns stays balanced without re-tuning.

### Vertical: same rule, stacked axis

If a container has a single content row, vertically center it:
`content_y_top = (container_h - content_h) / 2`.

If it has multiple rows (e.g. two ProcessingComponents inside an App),
choose `ROW1_CY` and `ROW2_CY` so the first row's top padding equals the
last row's bottom padding:

```
top_pad == row1_top - container_top  ==  container_bottom - rowN_bottom
```

Don't forget the container's own top header label (~28px high) eats into
the usable top area — count it toward "top padding" or keep row content
below it.

### Downstream siblings reference the derived width

When the container sits among siblings (Drive Folder, App, Vector
Database), place each sibling relative to `APP.x + APP_W`, not a hard-
coded x. Otherwise, changing inner layout silently breaks the outer
spacing.

```astro
const SIBLING_GAP = 20;
const TGT = { x: APP.x + APP_W + SIBLING_GAP, y: TOP_Y, w: 100, h: TOP_H };
const VB_W = TGT.x + TGT.w + 20;  // viewBox also derived
```

This prevents the "content hugs left edge, right side floats in space"
look seen when container widths are guessed independently of content.

## Compactness

Default to compact. The docs content column is ~720px, so most diagrams
render at `maxWidth` between 720 and 960. Extra vertical whitespace
between sibling elements distances them conceptually — only add padding
when the spacing conveys meaning.

When stacking two rows (e.g., two Processing Components), the inter-row
gap should be small enough that they feel like variations of the same
thing, not separate ideas.

## Slotted containers use local coordinates

`ProcessingComponent` places its rect at `translate(x y)` and renders
`<slot />` inside. Children in the slot use coordinates relative to the
container's top-left.

```astro
<ProcessingComponent x={PC_X} y={row.pcY} w={PC_W} h={PC_H} memoized={true}>
  {/* (0,0) here = container's top-left */}
  <LogicBox x={PC_PAD} y={(PC_H - SPLIT_H) / 2} ... />
</ProcessingComponent>
```

Anything that visually crosses out of the container (e.g., a vector →
Vector Database connector) must be drawn OUTSIDE the ProcessingComponent
tag, in absolute coords.

## Two coord spaces in one loop

A typical row iteration touches both:

```astro
{rows.map((row) => (
  <g>
    {/* absolute coords: outer layout */}
    <Connector d={`M ${DRIVE.x + DRIVE.w} ${row.rowCY} L ${FILE_X} ${row.rowCY}`} />
    <DataBox x={FILE_X} y={row.rowCY - FILE_H/2} ... />
    <FlowArrow d={`M ${FILE_X + FILE_W} ${row.rowCY} L ${PC_X} ${row.rowCY}`} />

    <ProcessingComponent x={PC_X} y={row.pcY} w={PC_W} h={PC_H} memoized={true}>
      {/* local coords: inside the PC */}
      <LogicBox x={PC_PAD} y={(PC_H - SPLIT_H)/2} ... />
      {/* ... */}
    </ProcessingComponent>

    {/* absolute coords again: exits from PC to outside destinations */}
    {chunkCYs.map((cy) => (
      <Connector d={`M ${PC_X + VECT_DX + VECT_W} ${cy} L ${VDB.x} ${cy}`} dashed={true} />
    ))}
  </g>
))}
```

Keep external-space and internal-space blocks visually separated in the
source for readability.

## Row centers drive child positions

Define `ROW_CY` for each row up front, then derive file y, chunk y,
embed y, etc. from it. This way shifting a row vertically only requires
changing `ROW_CY`, not every child.

## Memo marks are declarative

Never place `<MemoMark>` directly unless you're writing a brand new
container primitive. Instead pass `memoized={true}` to `LogicBox` or
`ProcessingComponent`, and the primitive renders the mark at the top-
right corner with consistent inset and size.

## Delta state + cache status

Every shape primitive (`DataBox`, `LogicBox`, `TargetBullet`,
`ProcessingComponent`) accepts two orthogonal annotation props:

- `state` — one of `new` (palm-green fill, inserted), `updated`
  (gold fill, changed in place), `removed` (pink fill +
  struck-through label + hover tooltip "deleted"), or `changed`
  (thin pink stroke, for fingerprint-invalidation). Default `idle`.
- `status` — `cache-ready` (green check badge, top-left) or
  `refreshing` (coral spinning arrow, top-left). Only valid on
  `LogicBox` / `ProcessingComponent`.

The two can combine: e.g. a memoized function still being re-run
would have `status="refreshing"`; one that hit cache would have
`status="cache-ready"`. A `state="removed"` element auto-suppresses
both `MemoMark` and `StatusBadge` — nothing to memoize, nothing to
cache-check.

## The scenario pattern for multi-state diagrams

Don't create one `.astro` wrapper per "what if" scenario. Instead,
parametrize the base diagram with a `scenario` prop and inline the
scenario in the `.mdx` right next to the prose that describes it:

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

The scenario is *content* (which file changed? which embed was
cached?), not a reusable component. Inlining it in the MDX keeps the
"what am I showing" and "what am I saying" together, so future edits
don't drift.

## Build new shape primitives on `ShapeGroup`

If you find yourself writing a new shape primitive, compose it on top
of `ShapeGroup` (not from scratch). `ShapeGroup` absorbs the wrapper
`<g transform="translate(x y)">`, base-class + state + highlight
class composition, the native `<title>deleted</title>` tooltip for
removed elements, and the optional `MemoMark` / `StatusBadge`
rendering with removed-state suppression. Each primitive just slots
in its own shape + label.
