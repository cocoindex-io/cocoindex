import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';

// `sources/index.md` → id `sources/index` → URL slug `sources`.
// Docusaurus URL parity is the constraint driving schema and pattern.
const docs = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/docs' }),
  schema: z
    .object({
      // Title may include *asterisks* to mark italic-coral segments — see
      // titleMarkup/titleText in src/consts.ts. Plain metadata strips them.
      title: z.string().optional(),
      // Lede paragraph rendered under the H1 and used as <meta description>.
      description: z.string().optional(),
      // Intro-meta strip (Time / Language / Requires) shown above the body.
      meta: z
        .object({
          time: z.string().optional(),
          language: z.string().optional(),
          requires: z.string().optional(),
        })
        .optional(),
      sidebar_label: z.string().optional(),
      sidebar_position: z.number().optional(),
      toc_max_heading_level: z.number().optional(),
      slug: z.string().optional(),
    })
    .passthrough(),
});

// Example walkthroughs — ported from github.com/cocoindex-io/examples.
// One .md file per slug in src/content/example-posts, rendered by
// src/pages/examples/[slug].astro beneath the shared hero. Card metadata
// (title, description, tags, etc.) lives in src/data/examples.ts — the
// filename drives the slug.
const examplePosts = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/example-posts' }),
  schema: z
    .object({
      image: z.string().optional(),
    })
    .passthrough(),
});

export const collections = { docs, examplePosts };
