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

export const collections = { docs };
