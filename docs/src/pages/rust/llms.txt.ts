// /docs/rust/llms.txt — the Rust SDK docs index. Built by src/lib/llms.ts.
import type { APIRoute } from 'astro';
import { llmsTxt } from '../../lib/llms';

export const GET: APIRoute = async () =>
  new Response(await llmsTxt('rust'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
