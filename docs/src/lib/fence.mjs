// Fenced-code matcher shared by the MDX→Markdown converter (src/lib/raw-markdown.ts,
// via vite) and the post-build checker (scripts/check-agent-output.mjs, via plain
// node). Fence runs are captured as a whole (`{3,}) so a ```` opener is not closed
// by a bare ``` line inside it; the closer may be longer than the opener, per
// CommonMark. Plain .mjs so both runtimes import the one definition.
export const FENCE = /(^|\n)[ \t]*(`{3,}|~{3,})[^\n]*\n[\s\S]*?\n[ \t]*\2[`~]*[ \t]*(?=\n|$)/g;
// MDX {/* … */} comments — stripped from prose by the converter; the checker
// uses it to know which source fences legitimately disappear from output.
export const MDX_COMMENT = /\{\/\*[\s\S]*?\*\/\}/g;
