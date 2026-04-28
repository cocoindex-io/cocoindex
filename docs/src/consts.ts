export const SITE_URL = 'https://cocoindex.io';
export const GITHUB_REPO = 'https://github.com/cocoindex-io/cocoindex';
export const DISCORD_URL = 'https://discord.com/invite/zpA9S2DR7s';
export const SITE_MAIN = 'https://cocoindex.io';
export const SITE_BLOG = 'https://cocoindex.io/blogs/';
// `import.meta.env.BASE_URL` reflects `base` in astro.config.mjs (e.g. `/docs-v0/`).
export const SITE_EXAMPLES = `${import.meta.env.BASE_URL.replace(/\/$/, '')}/examples/`;
// GitHub web-editor URL prefix for the "Edit this page" link.
export const DOCS_EDIT_BASE = 'https://github.com/cocoindex-io/cocoindex/edit/main/docs/docs';

// A content-collection id for `sources/index.md` is `sources/index`; the URL
// slug we want is just `sources`. Mirrors the blog-site helper pattern.
export const docSlug = (id: string) => id.replace(/\/index$/, '');

// Titles can mark 1–2 words with *asterisks* to italicize them in coral,
// matching the design mock: `Build your first flow in *10 minutes.*`.
// titleText strips the markers for metadata; titleMarkup emits safe HTML.
const HTML_ESCAPES: Record<string, string> = {
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
};
const escapeHtml = (s: string) => s.replace(/[&<>"']/g, (c) => HTML_ESCAPES[c]);

export const titleText = (s: string): string => s.replace(/\*([^*]+)\*/g, '$1');

export const titleMarkup = (s: string): string =>
  s.replace(/\*([^*]+)\*|([^*]+)/g, (_m, em, rest) =>
    em ? `<em>${escapeHtml(em)}</em>` : escapeHtml(rest),
  );
