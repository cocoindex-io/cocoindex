import { visit } from 'unist-util-visit';

const escapeHtml = (s) =>
  s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

export default function remarkMermaid() {
  return (tree) => {
    visit(tree, 'code', (node, index, parent) => {
      if (!parent || typeof index !== 'number') return;
      if (node.lang !== 'mermaid') return;

      parent.children.splice(index, 1, {
        type: 'html',
        value:
          '<div class="mermaid-figure">' +
          `<pre class="mermaid" role="img" aria-label="Mermaid diagram">${escapeHtml(node.value)}</pre>` +
          '</div>',
      });
    });
  };
}
