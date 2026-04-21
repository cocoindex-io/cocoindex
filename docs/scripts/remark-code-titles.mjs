// Remark plugin: wrap every fenced code block in the design-guideline
// `.code-figure` shell — three "traffic-light" dots on the left of a top
// bar, optional filename (from ```lang title="..."` metadata) on the right.
// Renders shiki's <pre> inside so syntax highlighting is unchanged.
//
// Ported verbatim from cocoindex-io.github.io/scripts/remark-code-titles.mjs
// so docs and blog code blocks share identical markup and can reuse the same
// `.code-figure` styling.

import { visit } from 'unist-util-visit';

const TITLE_RE = /\btitle\s*=\s*"([^"]+)"/;

const escapeHtml = (s) =>
  s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

const DOTS = '<span class="dots"><span></span><span></span><span></span></span>';

// Copy button — plain <button> with two labels; JS swaps visibility on click.
// Delegation handler lives in the layout/Topbar so a single listener serves
// every .code-figure on the page.
const COPY_ICON =
  '<svg class="copy-i" width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
  '<rect x="5" y="5" width="9" height="9" rx="1.5"/>' +
  '<path d="M3.5 10.5H3A1.5 1.5 0 0 1 1.5 9V3A1.5 1.5 0 0 1 3 1.5h6A1.5 1.5 0 0 1 10.5 3v.5"/>' +
  '</svg>';
const CHECK_ICON =
  '<svg class="copy-ok" width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
  '<path d="M3 8.5 7 12l6-8"/>' +
  '</svg>';
const COPY_BTN =
  '<button class="copy" type="button" aria-label="Copy code">' +
    COPY_ICON + CHECK_ICON +
    '<span class="copy-lbl">Copy</span>' +
  '</button>';

export default function remarkCodeTitles() {
  return (tree) => {
    visit(tree, 'code', (node, index, parent) => {
      if (!parent || typeof index !== 'number') return;

      let title = null;
      if (node.meta) {
        const m = node.meta.match(TITLE_RE);
        if (m) {
          // Strip directory prefix — the bar shows only the filename.
          title = m[1].trim().replace(/^.*\//, '');
          node.meta = node.meta.replace(TITLE_RE, '').replace(/\s+/g, ' ').trim() || null;
        }
      }

      const fileSlot = title
        ? `<span class="file">${escapeHtml(title)}</span>`
        : node.lang
          ? `<span class="file">${escapeHtml(node.lang)}</span>`
          : '<span class="file"></span>';

      // [dots] ................ [copy · file]  — copy reveals on hover and
      // sits just before the filename so the filename stays rightmost.
      const right = `<div class="right">${COPY_BTN}${fileSlot}</div>`;

      const open = {
        type: 'html',
        value: `<figure class="code-figure"><div class="bar">${DOTS}${right}</div>`,
      };
      const close = { type: 'html', value: '</figure>' };
      parent.children.splice(index, 1, open, node, close);
      return index + 3;
    });
  };
}
