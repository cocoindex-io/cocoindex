// Turn Docusaurus-style admonition blocks into the design mock's `.callout`
// layout. Shape:
//
//   :::info Prerequisite
//   Make sure your Postgres server is running.
//   :::
//
// …becomes:
//
//   <div class="callout note">
//     <div class="ico">i</div>
//     <div class="body"><b>Prerequisite</b> Make sure your …</div>
//   </div>
//
// Pair with `remark-directive` (it's what parses the `:::name [label]`
// block into a containerDirective node). The CSS for `.callout`, `.tip`,
// `.warn`, `.note` lives in src/styles/globals.css.
import { visit } from 'unist-util-visit';
import { toString } from 'mdast-util-to-string';

// Each admonition kind gets a distinct class + default label. The icon
// glyph itself is drawn in CSS via a per-kind mask-image data URI — that
// way the icon colour stays in sync with the bubble (currentColor) and we
// don't need to inject raw SVG through the MDX pipeline.
//   note    — neutral annotation (document-with-lines, berry bubble)
//   info    — important context (info-circle, coral bubble)
//   tip     — helpful suggestion (lightbulb, palm bubble)
//   warning — heads up (triangle !, pink bubble)
//   caution — alias for warning
//   danger  — critical (octagon !, coral bubble, deep pink bg)
const SPECS = {
  note:    { defaultLabel: 'Note',    cls: 'note'   },
  info:    { defaultLabel: 'Info',    cls: 'info'   },
  tip:     { defaultLabel: 'Tip',     cls: 'tip'    },
  warning: { defaultLabel: 'Warning', cls: 'warn'   },
  caution: { defaultLabel: 'Caution', cls: 'warn'   },
  danger:  { defaultLabel: 'Danger',  cls: 'danger' },
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
          // Empty by design — `.ico::before` draws the SVG via mask-image
          // so we can react to variant-specific colours via CSS only.
          type: 'paragraph',
          data: { hName: 'div', hProperties: { className: ['ico'], 'aria-hidden': 'true' } },
          children: [],
        },
        {
          type: 'paragraph',
          data: { hName: 'div', hProperties: { className: ['body'] } },
          children: [
            {
              type: 'strong',
              data: { hName: 'b' },
              children: [{ type: 'text', value: label }],
            },
            ...body,
          ],
        },
      ];
    });
  };
}
