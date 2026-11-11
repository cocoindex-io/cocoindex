// Heading ids per SDK language. A page renders once per language
// (src/lib/lang.ts), but MDX assigns heading ids for the whole source file at
// once, so a heading that appears in both a `<Lang only="python">` and a
// `<Lang only="rust">` block (or in both slots of a LangTabs pair) got `foo` on
// one page and `foo-1` on the other, which breaks `#foo` links on the second.
//
// This plugin gives every heading its id first, with one slugger per language:
// headings outside a language block count for both, headings inside one count
// only for that language. Astro's own heading pass keeps an id that is already
// set, so the TOC and anchors follow these ids.
import GithubSlugger from 'github-slugger';

const LANGS = ['python', 'rust'];

const attr = (node, name) =>
  node.attributes?.find((a) => a.type === 'mdxJsxAttribute' && a.name === name)?.value;

// The language a JSX element scopes its children to, if any.
function scopeOf(node) {
  if (node.type !== 'mdxJsxFlowElement' && node.type !== 'mdxJsxTextElement') return undefined;
  if (node.name === 'Lang') {
    const only = attr(node, 'only');
    return LANGS.includes(only) ? only : undefined;
  }
  if (node.name === 'Fragment') {
    const slot = attr(node, 'slot');
    return LANGS.includes(slot) ? slot : undefined;
  }
  return undefined;
}

// Heading text the way Astro reads it: the text of every text-like child.
function headingText(node) {
  let text = '';
  const walk = (n) => {
    if (n.type === 'text' || n.type === 'raw' || n.type === 'mdxTextExpression') text += n.value ?? '';
    for (const c of n.children ?? []) walk(c);
  };
  walk(node);
  return text;
}

export default function rehypeLangHeadingIds() {
  return (tree) => {
    const sluggers = Object.fromEntries(LANGS.map((l) => [l, new GithubSlugger()]));
    const visit = (node, scope) => {
      const here = scopeOf(node) ?? scope;
      if (node.type === 'element' && /^h[1-6]$/.test(node.tagName)) {
        node.properties = node.properties ?? {};
        if (typeof node.properties.id !== 'string') {
          const text = headingText(node);
          if (here) {
            node.properties.id = sluggers[here].slug(text);
          } else {
            // Shared heading: same id on both pages; advance both sluggers.
            const ids = LANGS.map((l) => sluggers[l].slug(text));
            node.properties.id = ids[0];
          }
        }
      }
      for (const c of node.children ?? []) visit(c, here);
    };
    visit(tree, undefined);
  };
}
