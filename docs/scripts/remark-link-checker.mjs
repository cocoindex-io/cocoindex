// Validate (and optionally rewrite) relative links in Markdown / MDX during
// the build.
//
// Three checks per `link` node:
//
//   1. **No `.md` / `.mdx` suffix.** Links resolve to URL paths, not files —
//      a stray `.mdx` renders a literal href that 404s in the browser.
//      Astro's renderer doesn't catch this; this plugin does.
//   2. **Target exists.** A relative link must resolve to a file under
//      `src/content/docs/` (`<path>.mdx`, `<path>.md`, or
//      `<path>/index.{mdx,md}`).
//   3. **Fragment exists** (when the URL has `#fragment`). The fragment
//      must match a heading id in the target file. Slugification follows
//      `github-slugger` (the algorithm Astro / rehype-slug also use), so
//      this matches the IDs Astro renders into the HTML.
//
// In-page fragment links (`#section`) are validated against the current
// file's headings.
//
// **Rewrite (default on).** After a relative link's target is resolved, the
// link's `url` is replaced with an absolute path of the form
// `<base>/<slug>` (e.g. `./app` from `programming_guide/core_concepts.mdx`
// becomes `/docs/programming_guide/app`). This is needed under Astro's
// `build.format: 'directory'` mode: source-relative links and URL-relative
// links diverge once each page lives at `<slug>/index.html`. Authors keep
// writing the natural source-relative form; the plugin emits absolute hrefs
// that resolve identically regardless of trailing-slash quirks.
//
// In-page fragment links (`#section`) are NOT rewritten — they target the
// current page and need no base prefix.
//
// Skipped entirely: absolute URLs (`https://`), root-relative URLs
// (`/docs/foo` — already canonical), and `mailto:` links.
//
// Plugin option:
//   - `base` (default `''`): URL prefix to prepend when rewriting. Pass the
//     same value as Astro's `base` config. Trailing slashes normalized away.
//
// On any check failure the plugin throws a single combined error per file,
// which fails the Astro build.
import { readFileSync, existsSync } from 'node:fs';
import { dirname, extname, posix, resolve, sep } from 'node:path';
import GithubSlugger from 'github-slugger';
import { visit } from 'unist-util-visit';

const ABSOLUTE_URL = /^[a-z][a-z0-9+\-.]*:/i;
const CONTENT_ROOT_MARKER = `${sep}src${sep}content${sep}docs${sep}`;

// Cache parsed heading slugs per target file. Build-time only, so a simple
// in-memory map is fine — and the same file can be linked from many places.
const headingSlugCache = new Map();

// Markdown ATX headings: `#`–`######` followed by a space and the heading
// text. We strip trailing whitespace and any trailing `#` markers (a
// CommonMark allowance). Leading whitespace up to 3 spaces is allowed.
const HEADING_RE = /^[ ]{0,3}(#{1,6})[ \t]+(.+?)(?:[ \t]+#+)?[ \t]*$/;
// MDX/HTML headings with an explicit `id="..."` attribute. Captures the id.
const HTML_HEADING_ID_RE = /<h[1-6][^>]*\sid=["']([^"']+)["']/gi;
// Strip a few markdown wrappers from heading text before slugifying so the
// input matches the rendered text github-slugger would see. Important: do
// NOT strip underscores wholesale — they're literal inside code spans
// (e.g. `mount_each()`), and github-slugger preserves them as `mount_each`.
function cleanHeadingText(text) {
  return text
    // Strip inline code backticks
    .replace(/`+/g, '')
    // Strip `*` emphasis (bold/italic). Underscores intentionally left alone.
    .replace(/\*+/g, '')
    // Drop link wrappers `[text](url)` → `text`
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .trim();
}

function loadHeadingSlugs(filePath) {
  const cached = headingSlugCache.get(filePath);
  if (cached) return cached;

  const slugs = new Set();
  let inFence = false;
  let fenceMarker = '';
  let text;
  try {
    text = readFileSync(filePath, 'utf8');
  } catch {
    headingSlugCache.set(filePath, slugs);
    return slugs;
  }

  // Strip the YAML frontmatter (between leading `---` markers) so its
  // contents don't get mistaken for headings.
  const fmMatch = text.match(/^---\r?\n[\s\S]*?\r?\n---\r?\n/);
  const body = fmMatch ? text.slice(fmMatch[0].length) : text;

  const slugger = new GithubSlugger();
  for (const rawLine of body.split(/\r?\n/)) {
    const line = rawLine;
    // Track fenced code blocks (``` or ~~~). Inside a fence, ignore everything.
    const fence = line.match(/^[ \t]*(```+|~~~+)/);
    if (fence) {
      const marker = fence[1];
      if (!inFence) {
        inFence = true;
        fenceMarker = marker[0].repeat(3); // store the kind, not length
      } else if (marker.startsWith(fenceMarker)) {
        inFence = false;
      }
      continue;
    }
    if (inFence) continue;

    const m = HEADING_RE.exec(line);
    if (m) {
      const cleaned = cleanHeadingText(m[2]);
      slugs.add(slugger.slug(cleaned));
      continue;
    }
    // Also pick up explicit `<h2 id="...">`-style anchors used in MDX.
    let h;
    while ((h = HTML_HEADING_ID_RE.exec(line)) !== null) {
      slugs.add(h[1]);
    }
  }

  headingSlugCache.set(filePath, slugs);
  return slugs;
}

function resolveTargetFile(sourceFile, urlPath) {
  const sourceDir = dirname(sourceFile);
  const base = resolve(sourceDir, urlPath);
  for (const candidate of [
    `${base}.mdx`,
    `${base}.md`,
    `${base}/index.mdx`,
    `${base}/index.md`,
  ]) {
    if (existsSync(candidate)) return candidate;
  }
  return null;
}

// Convert an absolute target file path (under `src/content/docs/`) to its
// content-collection slug — the URL path Astro serves it at, *without* the
// `base` prefix. `<root>/foo/bar.mdx` → `foo/bar`; `<root>/foo/index.mdx`
// → `foo`. POSIX-separated regardless of host platform.
function fileToSlug(targetFile) {
  const ix = targetFile.indexOf(CONTENT_ROOT_MARKER);
  if (ix < 0) return null;
  let rel = targetFile.slice(ix + CONTENT_ROOT_MARKER.length);
  // Normalize to POSIX separators for URL output.
  rel = rel.split(sep).join('/');
  // Strip extension.
  rel = rel.replace(/\.(mdx|md)$/i, '');
  // index files map to the directory URL (or '' for the root index).
  rel = rel.replace(/(?:^|\/)index$/, (m) => (m === 'index' ? '' : ''));
  return rel;
}

function normalizeBase(base) {
  if (!base) return '';
  // Trim trailing slashes; ensure a single leading slash.
  let b = base.replace(/\/+$/, '');
  if (b && !b.startsWith('/')) b = `/${b}`;
  return b;
}

export default function remarkLinkChecker(options = {}) {
  const base = normalizeBase(options.base);

  return (tree, file) => {
    const sourcePath = file.path;
    if (!sourcePath) return;

    const errors = [];

    visit(tree, 'link', (node) => {
      const url = node.url;
      if (!url) return;
      if (ABSOLUTE_URL.test(url)) return;
      if (url.startsWith('mailto:')) return;
      if (url.startsWith('/')) return; // root-relative — skip

      const line = node.position?.start?.line;

      // In-page fragment link (`#section`): validate against this file. Not
      // rewritten — fragment-only URLs target the current page.
      if (url.startsWith('#')) {
        const fragment = url.slice(1);
        if (!fragment) return;
        const slugs = loadHeadingSlugs(sourcePath);
        if (!slugs.has(fragment)) {
          errors.push({
            line,
            message: `link "${url}" — fragment "#${fragment}" does not match any heading in this file`,
          });
        }
        return;
      }

      const [pathPart, fragment] = url.split('#');
      if (!pathPart) return;

      const ext = extname(pathPart).toLowerCase();
      if (ext === '.md' || ext === '.mdx') {
        errors.push({
          line,
          message: `link "${url}" has a ${ext} suffix — drop the extension (links resolve to URL paths, not files)`,
        });
        return;
      }

      const targetFile = resolveTargetFile(sourcePath, pathPart);
      if (!targetFile) {
        const sourceDir = dirname(sourcePath);
        const probeBase = resolve(sourceDir, pathPart);
        errors.push({
          line,
          message: `link "${url}" does not resolve to a file in the docs collection (looked for: ${[
            `${probeBase}.mdx`,
            `${probeBase}.md`,
            `${probeBase}/index.mdx`,
            `${probeBase}/index.md`,
          ]
            .map((c) => c.replace(/^.*\/src\/content\/docs\//, ''))
            .join(', ')})`,
        });
        return;
      }

      if (fragment) {
        const slugs = loadHeadingSlugs(targetFile);
        if (!slugs.has(fragment)) {
          errors.push({
            line,
            message: `link "${url}" — fragment "#${fragment}" does not match any heading in ${targetFile.replace(
              /^.*\/src\/content\/docs\//,
              '',
            )}`,
          });
          return;
        }
      }

      // Rewrite: replace the source-relative URL with the canonical absolute
      // one (`<base>/<slug>/[#fragment]`). The trailing slash matches the
      // canonical URL Astro emits under `build.format: 'directory'` — without
      // it, the host (e.g. GitHub Pages) issues a 301 redirect on every click.
      // Robust under directory format because the browser no longer has to
      // interpret `./` against a trailing-slash-bearing URL.
      const slug = fileToSlug(targetFile);
      if (slug !== null) {
        const joined = posix.join('/', base.replace(/^\//, ''), slug);
        const absPath = joined.endsWith('/') ? joined : `${joined}/`;
        node.url = fragment ? `${absPath}#${fragment}` : absPath;
      }
    });

    if (errors.length > 0) {
      const lines = errors
        .map((e) => `  ${sourcePath}${e.line ? `:${e.line}` : ''} — ${e.message}`)
        .join('\n');
      throw new Error(`remark-link-checker found broken links:\n${lines}`);
    }
  };
}
