// /docs/rust/llms-full.txt — the full text of the Rust SDK docs. Built by
// src/lib/llms.ts.
import type { APIRoute } from 'astro';
import { llmsFullTxt } from '../../lib/llms';

export const GET: APIRoute = async () =>
  new Response(await llmsFullTxt('rust'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
