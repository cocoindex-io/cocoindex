import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import { test } from 'node:test';
import { createMarkdownProcessor } from '@astrojs/markdown-remark';
import remarkExampleImages from './remark-example-images.mjs';

const contentRoot = new URL('../src/content/example-posts/', import.meta.url);
const prefix = 'https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/';
const processor = await createMarkdownProcessor({ syntaxHighlight: false, remarkPlugins: [remarkExampleImages] });
const original = await createMarkdownProcessor({ syntaxHighlight: false });
const articleURL = new URL('fixture.md', contentRoot);
const render = (content, fileURL = articleURL) => processor.render(content, { fileURL });

test('article diagrams expose an accessible full-size action and reserve space before loading', async () => {
  const { code } = await render(`![Flow](${prefix}flow-v1-dark.svg)`);
  assert.match(code, /<figure class="example-diagram">/);
  assert.match(code, /width="1240" height="1620" loading="lazy" decoding="async"/);
  assert.match(code, /href="[^\"]+flow-v1-dark.svg"[^>]+aria-label="Open diagram at full size: Flow \(opens in a new tab\)"/);
  assert.match(code, />Open diagram at full size<\/a>/);
});

test('retired article cover is omitted, while non-example hero rendering is untouched', async () => {
  const markdown = `![Cover](${prefix}cover.png)\n\n![Screenshot](${prefix}search-results.png)`;
  const article = await render(markdown);
  assert.doesNotMatch(article.code, /cover\.png/);
  assert.match(article.code, /width="2958" height="1672" loading="lazy"/);
  const pageURL = new URL('../docs/fixture.md', contentRoot);
  const page = await render(markdown, pageURL);
  const baseline = await original.render(markdown, { fileURL: pageURL });
  assert.equal(page.code, baseline.code);
});

test('a missing first-party image dimension fails with the regeneration command', () => {
  const tree = { type: 'root', children: [{ type: 'paragraph', children: [
    { type: 'image', url: `${prefix}new-image.png`, alt: 'New image' },
  ] }] };
  assert.throws(() => remarkExampleImages()(tree, { path: articleURL.pathname }), /update-example-image-metadata\.mjs/);
});

test('all current articles have sized lazy images, accessible diagrams, and unchanged code fences', async () => {
  let images = 0;
  for (const name of (await readdir(contentRoot)).sort()) {
    if (!/\.mdx?$/.test(name)) continue;
    const fileURL = new URL(name, contentRoot);
    const markdown = await readFile(fileURL, 'utf8');
    const [{ code }, baseline] = await Promise.all([
      render(markdown, fileURL), original.render(markdown, { fileURL }),
    ]);
    const tags = code.match(/<img\b[^>]*>/g) ?? [];
    for (const tag of tags) {
      assert.match(tag, /width="[1-9][0-9]*"/, name);
      assert.match(tag, /height="[1-9][0-9]*"/, name);
      assert.match(tag, /loading="lazy"/, name);
      assert.doesNotMatch(tag, /\/cover\.png/, name);
    }
    const diagrams = tags.filter((tag) => /src="[^\"]+\.svg"/.test(tag));
    assert.equal((code.match(/class="example-diagram-open"/g) ?? []).length, diagrams.length, name);
    assert.deepEqual(code.match(/<pre>[\s\S]*?<\/pre>/g), baseline.code.match(/<pre>[\s\S]*?<\/pre>/g), name);
    images += tags.length;
  }
  assert.equal(images, 91, 'all current article image occurrences are covered');
});
