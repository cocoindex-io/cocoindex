// Turn Docusaurus-style admonition blocks into the docs `.callout` layout
// (styled in src/styles/site.css §5). Shape:
//
//   :::info Prerequisite
//   Make sure your Postgres server is running.
//   :::
//
// …becomes:
//
//   <div class="callout info">
//     <div class="ico" aria-hidden="true"><svg …/></div>
//     <div class="body"><b class="callout-label">Prerequisite</b> Make sure your …</div>
//   </div>
//
// Pair with `remark-directive` (it's what parses the `:::name [label]`
// block into a containerDirective node).
import { visit } from 'unist-util-visit';
import { toString } from 'mdast-util-to-string';

// One 16px stroke icon per kind, drawn in the same 1.75px weight so the four
// kinds read as one family. Colour comes from CSS (gray; warnings in ink).
const STROKE = 'fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"';
const svg = (inner) => `<svg viewBox="0 0 16 16" width="16" height="16" ${STROKE} aria-hidden="true">${inner}</svg>`;
const ICONS = {
  // pencil-free "note": a page with a folded corner
  note: svg('<path d="M4 2.5h5.5L13 6v7.5a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1v-10a1 1 0 0 1 1-1Z"/><path d="M9.5 2.5V6H13"/>'),
  // info: a circle with an i
  info: svg('<circle cx="8" cy="8" r="6.25"/><path d="M8 7.25v3.5"/><path d="M8 5.1v.1"/>'),
  // tip: a circle with a check
  tip: svg('<circle cx="8" cy="8" r="6.25"/><path d="m5.4 8.2 1.8 1.8 3.5-3.9"/>'),
  // warning: a triangle with a mark
  warn: svg('<path d="M8 2.6 14 13H2L8 2.6Z"/><path d="M8 6.5v3"/><path d="M8 11.4v.1"/>'),
};

//   note    — neutral aside
//   info    — context the reader needs before continuing
//   tip     — a recommended way
//   warning — something that can go wrong
//   caution — alias for warning
//   danger  — alias for warning
const SPECS = {
  note: { defaultLabel: 'Note', cls: 'note' },
  info: { defaultLabel: 'Info', cls: 'info' },
  tip: { defaultLabel: 'Tip', cls: 'tip' },
  warning: { defaultLabel: 'Warning', cls: 'warn' },
  caution: { defaultLabel: 'Caution', cls: 'warn' },
  danger: { defaultLabel: 'Warning', cls: 'warn' },
};

export default function remarkAdmonitions() {
  return (tree) => {
    visit(tree, (node) => {
      if (node.type !== 'containerDirective') return;
      const spec = SPECS[node.name];
      if (!spec) return;

      // If the author wrote `:::info Prerequisite`, remark-directive parses
      // "Prerequisite" as a directiveLabel paragraph at position 0. Extract
      // it as the callout title so the rest of the children keep rendering
      // as normal mdast.
      let label = spec.defaultLabel;
      if (node.children.length > 0 && node.children[0].data?.directiveLabel) {
        const first = node.children.shift();
        const extracted = toString(first).trim();
        if (extracted) label = extracted;
      }

      const body = node.children;
      const data = node.data || (node.data = {});
      data.hName = 'div';
      data.hProperties = { className: ['callout', spec.cls] };

      node.children = [
        {
          type: 'paragraph',
          data: { hName: 'div', hProperties: { className: ['ico'], 'aria-hidden': 'true' } },
          children: [{ type: 'html', value: ICONS[spec.cls] }],
        },
        {
          type: 'paragraph',
          data: { hName: 'div', hProperties: { className: ['body'] } },
          children: [
            {
              type: 'strong',
              data: { hName: 'b', hProperties: { className: ['callout-label'] } },
              children: [{ type: 'text', value: label }],
            },
            ...body,
          ],
        },
      ];
    });
  };
}
