// Builds /docs/llms.txt (Python) and /docs/rust/llms.txt (Rust) — a
// machine-readable index of one SDK language's docs for LLMs and agents (see
// https://llmstxt.org/). Built from the same sidebar tree and per-page
// descriptions that drive the site, so it stays in sync automatically.
import { DOCS_BASE, GITHUB_REPO, SITE_URL, SKILL_MD_URL, docTitle } from '../consts';
import { flatten, sidebar, type SidebarDoc } from '../data/docs-sidebar';
import { getCollection } from 'astro:content';
import { mdxToMarkdown, docSrcDir } from './raw-markdown';
import { EXAMPLE_CATALOG, EXAMPLE_CATALOG_GROUPS, examples, findExample } from '../data/examples';
import { langMarkdownHref, langPageUrl, langPrefix, pageRegistry, sidebarFor, type Lang } from './lang';

const oneLine = (s?: string) => (s ?? '').replace(/\s+/g, ' ').trim();

export async function llmsTxt(lang: Lang): Promise<string> {
  const reg = await pageRegistry();
  const tree = sidebarFor(sidebar, lang, reg);
  const url = (slug: string) => langPageUrl(lang, slug);
  const markdownUrl = (slug: string) => new URL(langMarkdownHref(lang, slug), SITE_URL).toString();
  const LLMS_FULL_TXT_URL = new URL(`${DOCS_BASE}/${langPrefix(lang)}llms-full.txt`, SITE_URL).toString();
  const desc = (slug: string) => {
    const e = reg.at(lang, slug);
    if (!e) return '';
    const data = e.doc.data as Record<string, unknown>;
    const d = lang === 'rust' && typeof data.rust_description === 'string' ? data.rust_description : e.doc.data.description;
    return oneLine(d);
  };
  const sidebarRoot = tree;

  const line = (slug: string, label?: string) => {
    const d = desc(slug);
    return `- [${label ?? slug}](${url(slug)})${d ? `: ${d}` : ''}`;
  };

  const out: string[] = [
    lang === 'rust' ? '# CocoIndex Docs — Rust SDK' : '# CocoIndex Docs',
    '',
    '> CocoIndex is an ultra-performant framework for building data pipelines for AI, ' +
      'with built-in incremental processing. Declare what your target should look like ' +
      'as a function of your source — the Rust engine keeps it in sync, reprocessing ' +
      'only what changed.',
    '',
    lang === 'rust'
      ? `> This index lists the Rust SDK pages (/docs/rust/). The Rust SDK is experimental: any release may change or remove its API, and it is not yet published on crates.io. The Python SDK pages are indexed at ${new URL(`${DOCS_BASE}/llms.txt`, SITE_URL)}.`
      : `> This index lists the Python SDK pages. The Rust SDK pages are indexed at ${new URL(`${DOCS_BASE}/rust/llms.txt`, SITE_URL)}.`,
    '',
    '> Full docs text in one file: ' + LLMS_FULL_TXT_URL +
      `. Docs pages and example walkthroughs have raw Markdown twins by replacing the trailing slash with \`.md\`, e.g. ${markdownUrl('programming_guide/core_concepts')} — and they are all included in llms-full.txt.`,
    '',
    '> Coding agents: install the CocoIndex skill before writing v1 code — ' +
      SKILL_MD_URL +
      ' (teaches the correct v1 API; without it, LLMs tend to hallucinate the deprecated v0 flow-builder DSL).',
    '',
  ];

  // Standalone top-level docs (Core Concepts, CLI, FAQ) surfaced first.
  const standalone = sidebarRoot.filter((i): i is SidebarDoc => i.type === 'doc');
  if (standalone.length) {
    out.push('## Key pages');
    for (const d of standalone) out.push(line(d.slug, d.label));
    out.push('');
  }

  for (const item of sidebarRoot) {
    if (item.type !== 'category') continue;
    out.push(`## ${item.label}`);
    if (item.slug) out.push(line(item.slug, item.label));
    for (const sub of item.items) {
      if (sub.type === 'doc') out.push(line(sub.slug, sub.label));
      else for (const s2 of sub.items) if (s2.type === 'doc') out.push(line(s2.slug, s2.label));
    }
    out.push('');
  }

  if (lang === 'rust') {
    out.push('## Examples');
    out.push(`> Runnable Rust SDK examples: ${GITHUB_REPO}/tree/main/examples/rust — each is a standalone Cargo workspace; run it with \`cargo run\` or the Rust CLI (\`cocoindex update\`).`);
    out.push('');
    return out.join('\n');
  }

  // Every runnable example in the monorepo — the on-page listing curates a few,
  // but agents should see the whole set (and how to clone any one) in one fetch.
  // Documented examples link to their walkthrough; the rest link to source.
  out.push('## Examples');
  out.push(
    `> ${EXAMPLE_CATALOG.length} runnable examples in the monorepo. ` +
      'Clone the repo with `git clone ' +
      GITHUB_REPO +
      '`, then `cd cocoindex/examples/<dir>`, copy `.env.example` if present, install with `pip install -e .`, and run the command shown for that example.',
  );
  out.push('');
  for (const group of EXAMPLE_CATALOG_GROUPS) {
    out.push(`### ${group.title}`);
    out.push(`> ${group.blurb}`);
    out.push('');
    for (const ex of group.entries) {
      const href = ex.docs
        ? url(`examples/${ex.docs}`)
        : `${GITHUB_REPO}/tree/main/examples/${ex.dir}`;
      out.push(`- [${ex.title}](${href}): ${oneLine(ex.description)} (examples/${ex.dir}; run: \`${ex.run ?? 'cocoindex update main'}\`)`);
    }
    out.push('');
  }

  return out.join('\n');
}

// Builds /docs/llms-full.txt (Python) and /docs/rust/llms-full.txt (Rust):
// the full text of one SDK language's docs pages (and, for Python, the example
// walkthroughs) in one file, for LLMs and agents that want everything in a
// single fetch. The llms.txt of the same language is the lighter index.

export async function llmsFullTxt(lang: Lang): Promise<string> {
  const reg = await pageRegistry();
  const indexUrl = new URL(`${DOCS_BASE}/${langPrefix(lang)}llms.txt`, SITE_URL).toString();
  const out: string[] = [
    lang === 'rust' ? '# CocoIndex Docs — Rust SDK, full text' : '# CocoIndex Docs — full text',
    '',
    (lang === 'rust'
      ? '> The complete CocoIndex Rust SDK documentation concatenated into one file for LLMs and agents. '
      : '> The complete CocoIndex documentation and example walkthroughs concatenated ' +
        'into one file for LLMs and agents. ') +
      'Each section below is one docs page' + (lang === 'rust' ? '' : ' or example page') + ', in reading order. ' +
      `For a lighter index of pages${lang === 'rust' ? '' : ' and examples'}, see ${indexUrl}.`,
  ];

  const seen = new Set<string>();
  const emit = (route: string) => {
    const e = reg.at(lang, route);
    if (!e || seen.has(route)) return;
    seen.add(route);
    const data = e.doc.data as Record<string, unknown>;
    const raw = lang === 'rust' && typeof data.rust_title === 'string' ? data.rust_title : e.doc.data.title;
    const title = docTitle(e.doc.id, raw);
    out.push('', '---', '', `# ${title}`, '', `Source: ${langPageUrl(lang, route)}`, '', mdxToMarkdown(e.doc.body ?? '', docSrcDir(e.doc), lang, (s) => reg.resolve(s, lang)?.href));
  };

  // Sidebar order first, then any stray pages of the language not in the tree.
  for (const e of flatten(sidebarFor(sidebar, lang, reg))) emit(e.slug);
  for (const e of reg.entries) if (e.languages.includes(lang)) emit(e.route);

  if (lang === 'rust') return out.join('\n') + '\n';

  const exampleUrl = (slug: string) => langPageUrl('python', `examples/${slug}`);
  const examplePosts = await getCollection('examplePosts');
  const byExampleSlug = new Map(examplePosts.map((e) => [e.id.replace(/\.mdx?$/, ''), e]));
  const emittedExamples = new Set<string>();
  const emitExample = (slug: string) => {
    const post = byExampleSlug.get(slug);
    if (!post || emittedExamples.has(slug)) return;
    emittedExamples.add(slug);
    const meta = findExample(slug);
    const title = docTitle(slug, meta?.title);
    out.push('', '---', '', `# Example: ${title}`, '', `Source: ${exampleUrl(slug)}`, '', mdxToMarkdown(post.body ?? '', 'examples'));
  };

  out.push('', '---', '', '# Example Walkthroughs');
  for (const e of examples) emitExample(e.slug);
  for (const slug of byExampleSlug.keys()) emitExample(slug);

  return out.join('\n') + '\n';
}
