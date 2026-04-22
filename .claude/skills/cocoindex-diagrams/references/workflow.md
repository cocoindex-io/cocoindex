# Preview-and-verify workflow

Diagrams are visual artifacts. "The build compiled" is not sufficient
evidence that they render correctly — always screenshot and look.

## The base-path gotcha

The docs site configures `base: '/docs'` in `astro.config.mjs`. As a
result, the built HTML references CSS at `/docs/_astro/...`. If the
local preview server is rooted at `dist/` without mirroring that prefix,
the CSS returns 404, all CSS variables are undefined, and every SVG
`fill: var(--cream)` resolves to black.

**Symptom**: All diagram boxes render as solid black rectangles, but
text labels are visible. This is NOT a code bug. Fix the server, not
the diagram.

The `scripts/preview.sh` script handles this correctly by rsyncing the
`dist/` output into a `docs/` subdirectory before serving.

## Running the preview

```bash
scripts/preview.sh <docs-slug> [crop-y-top]
# Example: scripts/preview.sh programming_guide/core_concepts
# Example: scripts/preview.sh programming_guide/core_concepts 3300
```

The script:

1. Runs `npm run build` inside `docs/`.
2. Rsyncs `docs/dist/` → a temp directory under `docs/`.
3. Kills any stale server on port 8765, starts fresh on next free port.
4. Screenshots the page with headless Chrome at `1400x5200`, scale 1.
5. Saves full-page PNG + an optional crop.
6. Prints the paths.

Read the PNGs back with the `Read` tool. Claude Code is multimodal — it
can see and critique the rendered output.

## Locating a specific diagram in the page

Full-page screenshots are tall. To find your diagram:

1. Start with a wide crop covering a plausible range:
   ```bash
   magick /tmp/dg-preview/full.png -crop 1400x500+0+3300 /tmp/dg-preview/crop.png
   ```
2. Read the crop. If the target diagram isn't there, adjust the y-offset
   (e.g. `+2500`, `+3500`, `+4000`).
3. For layout scrutiny, crop tight and omit surrounding prose.

## Iteration loop

1. Edit the `.astro` file.
2. Run `scripts/preview.sh <slug>`.
3. `Read` the PNG.
4. Compare with intent; identify specific issues (overlap, opacity,
   wrong shape, wrong color).
5. Edit again, repeat.

Expect 2–4 cycles for any non-trivial diagram. First render almost always
surfaces issues invisible from code (e.g., labels overlapping icons).

## When to skip preview

Minor text/label edits that don't affect layout (e.g. fixing a typo in
a label string) can skip preview if the diagram otherwise passed a recent
review. Everything that touches coordinates, shape choices, or new
primitives needs a preview.

## Cleanup

The preview script writes to `/tmp/dg-preview/` and starts a local HTTP
server. Both clean up on next run. If a port gets stuck:

```bash
lsof -ti:8765 | xargs kill -9
```
