// Locates the agent skill (skills/cocoindex/) in the repo, a sibling of this
// docs project. import.meta.url is unreliable after the build bundles routes,
// so resolve from process.cwd() (the docs dir during `astro dev`/`astro build`),
// with a fallback for builds invoked from the repo root. Throwing here fails
// the build immediately instead of prerendering an error body.
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';

const SKILL_DIR = [
  resolve(process.cwd(), '../skills/cocoindex'),
  resolve(process.cwd(), 'skills/cocoindex'),
].find(existsSync);

if (!SKILL_DIR) {
  throw new Error('skills/cocoindex not found relative to the docs build cwd');
}

export const readSkillFile = (relPath: string): string =>
  readFileSync(resolve(SKILL_DIR, relPath), 'utf8');

export const skillReferenceNames = (): string[] =>
  readdirSync(resolve(SKILL_DIR, 'references'))
    .filter((f) => f.endsWith('.md'))
    .map((f) => f.replace(/\.md$/, ''));
