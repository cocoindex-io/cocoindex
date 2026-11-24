import type { DocSearchProps } from '@docsearch/js';

/** Render links and saved-search actions as separate cells in a result grid. */
export const accessibleSearchHit: NonNullable<DocSearchProps['hitComponent']> = (
  { hit, children }, helpers,
) => {
  const { html } = helpers!;
  // DocSearch 4.6.2 passes its standard Hit-Container, ending with the action slot.
  // Keep those VNodes intact so DocSearch owns their events and reconciliation.
  const container = children as unknown as { props: { children: unknown[] } };
  const parts = container.props.children;
  return html`<div className="docs-search-hit" role="row">
    <div className="docs-search-link" role="gridcell">
      <a href=${hit.url}>
        <div className="DocSearch-Hit-Container">${parts.slice(0, -1)}</div>
      </a>
    </div>
    <div className="docs-search-actions" role="gridcell">${parts.at(-1)}</div>
  </div>`;
};
