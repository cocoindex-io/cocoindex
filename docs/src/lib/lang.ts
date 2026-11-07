// SDK language of a docs page: the site has a Python tree at /docs/<slug>/ and
// a Rust tree at /docs/rust/<slug>/, rendered from one content collection.
// See docs/CONTENT_LANGUAGES.md for the authoring contract.
//
// Frontmatter keys read here:
//   languages: [python, rust]   trees the page appears in (default: both)
//   route: <slug>               the page's slug inside its trees, when it
//                               differs from its file path (a Rust-only page
//                               that takes the slot of a Python page, e.g.
//                               getting_started/rust_quickstart → route
//                               getting_started/quickstart)
//   python_counterpart / rust_counterpart: <slug>
//                               where the language switch goes from this page
//                               when the page has no copy in that language
//   neutral: true               the page reads the same in both languages; the
//                               Rust copy's canonical URL is the Python one
import type { CollectionEntry } from 'astro:content';
import { getCollection } from 'astro:content';
import { DOCS_BASE, SITE_URL, docSlug } from '../consts';
import type { SidebarItem } from '../data/docs-sidebar';

export type Lang = 'python' | 'rust';
export const LANGS: readonly Lang[] = ['python', 'rust'];
export const DEFAULT_LANG: Lang = 'python';
export const LANG_LABEL: Record<Lang, string> = { python: 'Python', rust: 'Rust' };

/** Path prefix of a language's tree, after the docs base: '' or 'rust/'. */
export const langPrefix = (lang: Lang): string => (lang === DEFAULT_LANG ? '' : `${lang}/`);

/** Site-relative URL of a page (`slug` '' is the tree's root). */
export const langHref = (lang: Lang, slug: string): string =>
  `${DOCS_BASE}/${langPrefix(lang)}${slug ? `${slug}/` : ''}`;

/** Absolute URL of a page. */
export const langPageUrl = (lang: Lang, slug: string): string =>
  new URL(langHref(lang, slug), SITE_URL).toString();

/** Site-relative URL of a page's raw-Markdown twin. */
export const langMarkdownHref = (lang: Lang, slug: string): string =>
  `${DOCS_BASE}/${langPrefix(lang)}${slug}.md`;

type Doc = CollectionEntry<'docs'>;

export interface PageEntry {
  doc: Doc;
  /** Slug from the file path: what source links and old URLs use. */
  fileSlug: string;
  /** Slug inside the page's trees. */
  route: string;
  languages: Lang[];
  neutral: boolean;
  counterpart: Partial<Record<Lang, string>>;
}

const isLang = (v: unknown): v is Lang => v === 'python' || v === 'rust';

export function pageEntry(doc: Doc): PageEntry {
  const data = doc.data as Record<string, unknown>;
  const fileSlug = docSlug(doc.id);
  const listed = Array.isArray(data.languages) ? data.languages.filter(isLang) : [];
  const counterpart: Partial<Record<Lang, string>> = {};
  for (const lang of LANGS) {
    const c = data[`${lang}_counterpart`];
    if (typeof c === 'string' && c) counterpart[lang] = c.replace(/^\/+|\/+$/g, '');
  }
  return {
    doc,
    fileSlug,
    route: typeof data.route === 'string' && data.route ? data.route.replace(/^\/+|\/+$/g, '') : fileSlug,
    languages: listed.length ? listed : [...LANGS],
    neutral: data.neutral === true,
    counterpart,
  };
}

export interface PageRegistry {
  /** The page at `route` in a language's tree. */
  at(lang: Lang, route: string): PageEntry | undefined;
  /** The page whose file slug is `fileSlug`. */
  byFile(fileSlug: string): PageEntry | undefined;
  /** URL of the page a source link names (by file slug), in `lang`'s tree
   *  when the page is there, else in the page's own language. */
  resolve(target: string, lang: Lang): { href: string; lang: Lang; route: string } | undefined;
  /** Where the language switch goes from `route` in `from` to `to`. */
  switchTarget(from: Lang, route: string, to: Lang): string;
  entries: PageEntry[];
}

let registry: Promise<PageRegistry> | undefined;

export function pageRegistry(): Promise<PageRegistry> {
  registry ??= build();
  return registry;
}

async function build(): Promise<PageRegistry> {
  const entries = (await getCollection('docs')).map(pageEntry);
  const trees: Record<Lang, Map<string, PageEntry>> = { python: new Map(), rust: new Map() };
  const files = new Map<string, PageEntry>();
  for (const e of entries) {
    files.set(e.fileSlug, e);
    for (const lang of e.languages) {
      const taken = trees[lang].get(e.route);
      if (taken) {
        throw new Error(
          `docs: two pages take route "${e.route}" in the ${lang} tree: ` +
            `${taken.fileSlug} and ${e.fileSlug} (set \`languages\` or \`route\`)`,
        );
      }
      trees[lang].set(e.route, e);
    }
  }

  const at = (lang: Lang, route: string) => trees[lang].get(route);

  // A page in `lang`'s tree that stands for `e`, which may live elsewhere.
  const standIn = (e: PageEntry, lang: Lang): string | undefined => {
    if (e.languages.includes(lang)) return e.route;
    const c = e.counterpart[lang];
    if (c && at(lang, c)) return c;
    if (at(lang, e.route)) return e.route; // another file takes the slot
    return undefined;
  };

  // A link names a file: it opens that file's page in the reader's language
  // when the page has one, else in the language it has.
  const resolve: PageRegistry['resolve'] = (target, lang) => {
    const t = target.replace(/^\/+|\/+$/g, '');
    const e = files.get(t);
    if (!e) return undefined;
    const l = e.languages.includes(lang) ? lang : e.languages[0];
    return { href: langHref(l, e.route), lang: l, route: e.route };
  };

  const switchTarget: PageRegistry['switchTarget'] = (from, route, to) => {
    const e = at(from, route);
    if (e) {
      const there = standIn(e, to);
      if (there !== undefined) return langHref(to, there);
    }
    // Section index, else the tree's root.
    const parts = route.split('/');
    for (let n = parts.length - 1; n > 0; n--) {
      const up = parts.slice(0, n).join('/');
      if (at(to, up)) return langHref(to, up);
    }
    return langHref(to, '');
  };

  return { at, byFile: (s) => files.get(s), resolve, switchTarget, entries };
}

/** The language of the page being rendered (set by the page route). */
export function currentLang(astro: { locals: unknown }): Lang {
  const v = (astro.locals as { lang?: unknown } | undefined)?.lang;
  return isLang(v) ? v : DEFAULT_LANG;
}

// ---------------------------------------------------------------------------
// Source-level language filtering, shared by the heading filter (TOC) and the
// raw-Markdown twins. Works on the MDX source: `<Lang only="x">…</Lang>` and
// LangTabs' `<Fragment slot="x">…</Fragment>` regions of the other language
// are removed. Fenced code must already be protected by the caller when the
// regions can contain fences with literal tags (raw-markdown does this); for
// heading extraction the plain regexes are enough.
// ---------------------------------------------------------------------------

const otherLang = (lang: Lang): Lang => (lang === 'python' ? 'rust' : 'python');

export function stripOtherLanguage(source: string, lang: Lang): string {
  const other = otherLang(lang);
  let s = source;
  // Language-only regions. `Lang` does not nest, so a lazy match is exact.
  s = s.replace(
    new RegExp(`<Lang\\s+only=["']${other}["'][^>]*>[\\s\\S]*?</Lang>`, 'g'),
    '',
  );
  // The other language's tab of a LangTabs pair.
  s = s.replace(
    new RegExp(`<Fragment\\s+slot=["']${other}["']\\s*>[\\s\\S]*?</Fragment>`, 'g'),
    '',
  );
  // Rust-only callouts.
  if (lang !== 'rust') s = s.replace(/<RustNote\b[^>]*>[\s\S]*?<\/RustNote>/g, '');
  if (lang !== 'python') s = s.replace(/<PythonNote\b[^>]*>[\s\S]*?<\/PythonNote>/g, '');
  return s;
}

/** Headings of the page as one language sees it (drops the other's). */
export function headingsFor<H extends { depth: number; text: string }>(
  headings: H[],
  source: string,
  lang: Lang,
): H[] {
  const kept = stripOtherLanguage(source, lang);
  const removed = source.length !== kept.length;
  if (!removed) return headings;
  // A heading is kept when its line survives the filter. Compare on the
  // heading text as Markdown writes it (`## Text`); headings rendered by
  // components keep their place since they are not in the source.
  const lines = (s: string) =>
    s
      .split('\n')
      .map((l) => /^\s*(#{1,6})\s+(.+?)\s*#*\s*$/.exec(l))
      .filter((m): m is RegExpExecArray => !!m)
      .map((m) => `${m[1].length}\u0000${plain(m[2])}`);
  const before = lines(source);
  const after = new Map<string, number>();
  for (const k of lines(kept)) after.set(k, (after.get(k) ?? 0) + 1);
  const dropped = new Map<string, number>();
  for (const k of before) {
    const n = after.get(k) ?? 0;
    if (n > 0) after.set(k, n - 1);
    else dropped.set(k, (dropped.get(k) ?? 0) + 1);
  }
  if (!dropped.size) return headings;
  return headings.filter((h) => {
    const k = `${h.depth}\u0000${plain(h.text)}`;
    const n = dropped.get(k) ?? 0;
    if (n > 0) {
      dropped.set(k, n - 1);
      return false;
    }
    return true;
  });
}

// Heading text as Astro reports it: no Markdown emphasis or code ticks.
const plain = (s: string) =>
  s
    .replace(/`([^`]*)`/g, '$1')
    .replace(/\*\*([^*]+)\*\*|\*([^*]+)\*|__([^_]+)__|_([^_]+)_/g, '$1$2$3$4')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/\s+/g, ' ')
    .trim();

// ---------------------------------------------------------------------------
// Sidebar per language: the shared tree (src/data/docs-sidebar.ts, written in
// routes) keeps only the pages that exist in the language's tree; a category
// with no page left is dropped.
// ---------------------------------------------------------------------------
export function sidebarFor(items: SidebarItem[], lang: Lang, reg: PageRegistry): SidebarItem[] {
  const out: SidebarItem[] = [];
  for (const item of items) {
    if (item.type === 'doc') {
      if (reg.at(lang, item.slug)) out.push(item);
      continue;
    }
    const children = sidebarFor(item.items, lang, reg);
    const slug = item.slug && reg.at(lang, item.slug) ? item.slug : undefined;
    if (children.length || slug) out.push({ ...item, slug, items: children });
  }
  return out;
}
