// Serves the CocoIndex agent skill (skills/cocoindex/SKILL.md) at /docs/skill.md
// so agents and users can fetch one file instead of cloning the repo — the
// Mintlify-style hosted skill.md. Served verbatim so it stays a valid SKILL.md
// (frontmatter intact); a source pointer is appended at the end. The skill's
// references/ live alongside it in the repo (linked below).
import type { APIRoute } from 'astro';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { GITHUB_REPO } from '../consts';

// The skill lives at <repo>/skills/cocoindex/SKILL.md, a sibling of this docs
// project. import.meta.url is unreliable after the build bundles this route, so
// resolve from process.cwd() (the docs dir during `astro dev`/`astro build`),
// with a fallback for builds invoked from the repo root.
const SKILL_PATH = [
  resolve(process.cwd(), '../skills/cocoindex/SKILL.md'),
  resolve(process.cwd(), 'skills/cocoindex/SKILL.md'),
].find(existsSync);

export const GET: APIRoute = () => {
  if (!SKILL_PATH) {
    return new Response('Skill file not found at build time.', { status: 500 });
  }
  const skill = readFileSync(SKILL_PATH, 'utf8').trimEnd();
  const note =
    `\n\n---\n\n> Hosted copy of ${GITHUB_REPO}/tree/main/skills/cocoindex (SKILL.md). ` +
    `The skill's references/ directory lives alongside it in that folder.\n`;
  return new Response(skill + note, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};
