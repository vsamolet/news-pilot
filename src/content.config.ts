import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';

const news = defineCollection({
  loader: glob({ pattern: '**/*.md', base: './src/content/news' }),
  schema: z.object({
    title: z.string(),
    lead: z.string(),
    pubDate: z.coerce.date(),
    category: z.string(),
    image: z.string(),
    image_credit: z.string().optional(),
    image_credit_url: z.string().url().optional(),
    source_url: z.string().url(),
    featured: z.boolean().optional().default(false),
  }),
});

export const collections = { news };
