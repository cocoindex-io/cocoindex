// Per-example scene metadata, one JSON file per slug beside its scene:
//   alt   — one sentence a screen reader hears for the hero picture.
//   needs — what the reader must have running or set (services, API keys).
//   run   — the shortest real path from clone to result, as shell steps.
// Each scenes/<slug>.json is owned by whoever draws that scene.
export type RunStep = { note?: string; cmd: string };
export type SceneMeta = { alt: string; needs?: string[]; run?: RunStep[] };

const files = import.meta.glob<{ default: SceneMeta }>('./*.json', { eager: true });

export const sceneMeta = (slug: string): SceneMeta | undefined => files[`./${slug}.json`]?.default;
export const hasScene = (slug: string): boolean => !!files[`./${slug}.json`];
