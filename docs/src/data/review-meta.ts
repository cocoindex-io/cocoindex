// Single source of truth for the per-page "Last reviewed" stamp and the docs
// version. Both the intro-meta strip (DocsLayout / examples) and the right-rail
// badge (Toc) derive their display from here, so they can never disagree.
//
// `reviewedTs` is a Unix timestamp (seconds) set explicitly when a human
// reviews a page against a CocoIndex release; see src/data/docs-meta.json.
import docsMeta from './docs-meta.json';

const data = docsMeta as {
  _version?: string | null;
  files: Record<string, { reviewedTs: number }>;
};

/** Current docs version, formatted as `v X.Y.Z` (null when unset). */
export const docsVersion = data._version ? `v ${data._version}` : null;

/** Raw review timestamp (seconds) for a slug, or 0 when none is recorded. */
export function reviewedTs(slug: string): number {
  return data.files?.[slug]?.reviewedTs ?? 0;
}

/**
 * Formatted absolute review date for a slug (e.g. "May 27, 2026"), or null
 * when the page has no stamp. This is the one place the date is formatted —
 * every "Last reviewed" surface renders this exact string.
 */
export function reviewedDate(slug: string): string | null {
  const ts = reviewedTs(slug);
  return ts
    ? new Date(ts * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
    : null;
}
