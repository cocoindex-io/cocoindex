// Turn Docusaurus-style admonition blocks into the design system's `.co`
// layout (see design_guidelines/ui/admonitions.html). Shape:
//
//   :::info Prerequisite
//   Make sure your Postgres server is running.
//   :::
//
// …becomes:
//
//   <div class="co info">
//     <div class="ico">i</div>
//     <div class="body"><b>Prerequisite</b> Make sure your …</div>
//   </div>
//
// Pair with `remark-directive` (it's what parses the `:::name [label]`
// block into a containerDirective node). The CSS for `.co` + variants
// lives in src/styles/globals.css and matches
// design_guidelines/ui/admonitions.html verbatim.
import { visit } from 'unist-util-visit';
import { toString } from 'mdast-util-to-string';

// Each admonition kind gets a distinct icon + class so readers can
// distinguish at a glance. Colors are defined in src/styles/globals.css.
//   note    — neutral annotation (berry pin, plain cream bg)
//   info    — important context (coral i, peach-tinted cream)
//   tip     — helpful suggestion (palm ✓, green-tinted cream)
//   warning — heads up (pink !, pink-tinted cream)
//   caution — alias for warning
//   danger  — critical (coral !, deeper pink-tinted cream)
const SPECS = {
  note: { defaultLabel: 'Note', cls: 'note', icon: 'i' },
  info: { defaultLabel: 'Info', cls: 'info', icon: 'i' },
  tip: { defaultLabel: 'Tip', cls: 'tip', icon: '\u2713' },
  warning: { defaultLabel: 'Warning', cls: 'warn', icon: '!' },
  caution: { defaultLabel: 'Caution', cls: 'warn', icon: '!' },
  danger: { defaultLabel: 'Danger', cls: 'danger', icon: '!' },
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
      data.hProperties = { className: ['co', spec.cls] };

      node.children = [
        {
          type: 'paragraph',
          data: { hName: 'div', hProperties: { className: ['ico'], 'aria-hidden': 'true' } },
          children: [{ type: 'text', value: spec.icon }],
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
