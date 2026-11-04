import { readFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

const root = resolve(process.argv[2] ?? 'dist');
const origin = 'https://cocoindex.io';
const base = '/docs-v0/';
const decode = (value) => value.replaceAll('&amp;', '&').replaceAll('&quot;', '"').replaceAll('&#39;', "'");
function files(dir) {
  return readdirSync(dir).flatMap(name => {
    const path = join(dir, name);
    return statSync(path).isDirectory() ? files(path) : path.endsWith('.html') ? [path] : [];
  });
}
let checked = 0;
const failures = new Set();
for (const file of files(root)) {
  const html = readFileSync(file, 'utf8');
  const page = new URL(base + relative(root, file).replace(/index\.html$/, ''), origin);
  const links = [...html.matchAll(/\bhref="([^"]+)"/g)].map(match => decode(match[1]));
  const refresh = html.match(/http-equiv="refresh"[^>]*content="[^";]*;url=([^"]+)"/i);
  if (refresh) {
    const destination = new URL(decode(refresh[1]), page);
    if (destination.origin === origin && !destination.pathname.startsWith(base)) {
      failures.add(`${page.pathname}: redirect loses ${base}: ${destination.pathname}`);
    }
    links.push(decode(refresh[1]));
  }
  for (const link of links) {
    const url = new URL(link, page);
    if (url.href.startsWith('https://github.com/cocoindex-io/cocoindex/edit/')) {
      const prefix = 'https://github.com/cocoindex-io/cocoindex/edit/v0/docs/';
      const source = decodeURIComponent(url.href.slice(prefix.length));
      if (!url.href.startsWith(prefix) || !source.startsWith('src/content/docs/') || !existsSync(resolve(source))) {
        failures.add(`${page.pathname}: invalid legacy edit source: ${link}`);
      }
    }
    if (url.origin !== origin || !url.pathname.startsWith(base)) continue;
    let target = join(root, decodeURIComponent(url.pathname.slice(base.length)));
    if (existsSync(target) && statSync(target).isDirectory()) target = join(target, 'index.html');
    checked++;
    if (!existsSync(target)) {
      failures.add(`${page.pathname}: missing ${url.pathname}`);
      continue;
    }
    if (url.hash && target.endsWith('.html')) {
      const ids = [...readFileSync(target, 'utf8').matchAll(/\bid="([^"]+)"/g)].map(match => decode(match[1]));
      if (!ids.includes(decodeURIComponent(url.hash.slice(1)))) failures.add(`${page.pathname}: missing ${url.pathname}${url.hash}`);
    }
  }
}
if (failures.size) {
  console.error([...failures].join('\n'));
  process.exitCode = 1;
} else console.log(`check-built-links: ${checked} local links and redirect targets pass`);
