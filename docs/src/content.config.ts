import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';

// `sources/index.md` → id `sources/index` → URL slug `sources`.
// Docusaurus URL parity is the constraint driving schema and pattern.
const docs = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/docs' }),
  schema: z
    .object({
      title: z.string().optional(),
      description: z.string().optional(),
      sidebar_label: z.string().optional(),
      sidebar_position: z.number().optional(),
      toc_max_heading_level: z.number().optional(),
      slug: z.string().optional(),
    })
    .passthrough(),
});

// Example walkthroughs — ported from github.com/cocoindex-io/examples.
// One .md file per slug in src/content/example-posts, rendered by
// src/pages/examples/[slug].astro beneath the shared hero.
const examplePosts = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/example-posts' }),
  schema: z
    .object({
      title: z.string(),
      description: z.string().optional(),
      slug: z.string(),
      image: z.string().optional(),
      tags: z.array(z.string()).optional(),
      // YAML parses ISO dates into Date; accept either so hand-edited
      // string dates also work.
      last_reviewed: z.union([z.string(), z.date()]).optional(),
    })
    .passthrough(),
});

export const collections = { docs, examplePosts };
