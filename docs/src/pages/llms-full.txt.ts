// /docs/llms-full.txt — the full text of the Python SDK docs and the example
// walkthroughs. Built by src/lib/llms.ts, which also builds the Rust one
// (/docs/rust/llms-full.txt).
import type { APIRoute } from 'astro';
import { llmsFullTxt } from '../lib/llms';

export const GET: APIRoute = async () =>
  new Response(await llmsFullTxt('python'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
