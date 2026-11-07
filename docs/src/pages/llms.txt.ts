// /docs/llms.txt — the Python SDK docs index. Built by src/lib/llms.ts, which
// also builds the Rust one (/docs/rust/llms.txt).
import type { APIRoute } from 'astro';
import { llmsTxt } from '../lib/llms';

export const GET: APIRoute = async () =>
  new Response(await llmsTxt('python'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
