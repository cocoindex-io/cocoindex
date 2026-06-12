// Serves the skill's references/ companions at /docs/references/<name>.md so
// the relative links inside the hosted /docs/skill.md resolve instead of 404ing.
import type { APIRoute } from 'astro';
import { readSkillFile, skillReferenceNames } from '../../lib/skill-files';

export function getStaticPaths() {
  return skillReferenceNames().map((name) => ({ params: { name } }));
}

export const GET: APIRoute = ({ params }) => {
  const body = readSkillFile(`references/${params.name}.md`);
  return new Response(body, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};
