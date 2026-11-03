# Example scenes

Each example gets a picture of what it builds, drawn in HTML from the
example's real inputs and outputs. Two files per example, named by slug
(the slug in `src/data/examples.ts`):

- `<slug>.astro` renders the scene. Props: `size: 'hero' | 'card'`.
  - `hero`: the walkthrough head, about 640–700px wide on desktop, no fixed
    height. Show the whole story: the real source items, the transform, the
    target rows or graph, and the payoff (a query and its hits, a JSON record,
    a graph answer). Rows built with `.sc-row` stack below 560px by themselves.
  - `card`: the catalog thumbnail, 176px tall and about 360–440px wide. One
    clear before → after. Text at least 11px. Decorative (aria-hidden).
- `<slug>.json`: `alt` (one sentence describing the hero), `needs` (services
  and keys), `run` (shell steps after `cd cocoindex/examples/<dir>`; the clone
  and cd lines are added for you).

Build with the kit in `../scene/scene.css` (`.sc-row`, `.sc-col`, `.sc-panel`,
`.sc-li`, `.sc-chip`, `.sc-table`, `.sc-node`, `.sc-query`, `.sc-hit`,
`.sc-code`, `.sc-to`, `.sc-ico i-*`, `.sc-delta`, `.sc-dmark`, `.sc-live`).
Scene-specific layout goes in the component's own `<style>` (scoped).

Truth rules: every file name, field, model, table, command and quoted text
comes from the example's source folder (`examples/<dir>/` in the repo), its
README, or its walkthrough post. Do not invent numbers (scores, timings,
counts, vector values). Periwinkle (`.sc-delta`) marks only the one edited
input and the outputs it re-derives; green (`.sc-live`) only live mode.

See `text-embedding.astro` / `.json` for the reference.
