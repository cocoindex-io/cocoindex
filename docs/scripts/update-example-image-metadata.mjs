// Usage: node scripts/update-example-image-metadata.mjs /path/to/blobs/public
// Reads the asset checkout explicitly supplied by the maintainer. No remote
// requests or machine-specific paths are part of the docs build.
import { readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createMarkdownProcessor } from '@astrojs/markdown-remark';
import { visit } from 'unist-util-visit';
import sharp from 'sharp';

const assetRoot = process.argv[2];
if (!assetRoot) throw new Error('Pass the path to the blobs/public asset checkout.');
const docsRoot = fileURLToPath(new URL('../', import.meta.url));
const contentRoot = path.join(docsRoot, 'src/content/example-posts');
const prefix = 'https://cocoindex.io/blobs/';
const urls = new Set();
const processor = await createMarkdownProcessor({
  syntaxHighlight: false,
  remarkPlugins: [() => (tree) => {
    visit(tree, 'image', (image) => {
      if (image.url.startsWith(`${prefix}docs-v1/img/examples/`) && !/\/cover\.png$/.test(image.url)) urls.add(image.url);
    });
  }],
});
for (const name of (await readdir(contentRoot)).sort()) {
  if (!/\.mdx?$/.test(name)) continue;
  await processor.render(await readFile(path.join(contentRoot, name), 'utf8'));
}
const metadata = {};
for (const url of Array.from(urls).sort()) {
  const assetPath = path.resolve(assetRoot, url.slice(prefix.length));
  const root = path.resolve(assetRoot) + path.sep;
  if (!assetPath.startsWith(root)) throw new Error(`Image escapes asset root: ${url}`);
  const { width, height } = await sharp(assetPath).metadata();
  if (!width || !height) throw new Error(`Missing intrinsic dimensions: ${url}`);
  metadata[url] = { width, height };
}
const output = path.join(docsRoot, 'src/data/example-image-metadata.json');
await writeFile(output, JSON.stringify(metadata, null, 2) + '\n');
console.log(`Pinned dimensions for ${urls.size} example article images.`);
