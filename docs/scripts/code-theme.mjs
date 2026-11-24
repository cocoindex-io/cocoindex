// Shiki theme for the docs, matching cocoindex.io's code palette (site.css
// --code-* tokens): near-black panel, near-white text, periwinkle keywords,
// blue functions and types, soft-green strings, amber numbers, gray comments
// and punctuation. Replaces @cocoindex/brand/code-theme for the dark docs.
const c = {
  bg: '#0C0C0C', fg: '#EDEDED', comment: '#8A8A8A', keyword: '#8E96FF',
  fn: '#79C0FF', string: '#9ED9B3', number: '#F9C78B', type: '#79C0FF',
  decorator: '#8E96FF', variable: '#EDEDED', punct: '#A1A1A1',
};

export const docsCodeTheme = {
  name: 'cocoindex-standard',
  type: 'dark',
  colors: { 'editor.background': c.bg, 'editor.foreground': c.fg },
  tokenColors: [
    { scope: ['comment', 'punctuation.definition.comment', 'string.comment'], settings: { foreground: c.comment } },
    { scope: ['keyword', 'keyword.control', 'keyword.operator.new', 'storage', 'storage.type', 'storage.modifier'], settings: { foreground: c.keyword } },
    { scope: ['entity.name.function', 'meta.function-call', 'support.function', 'variable.function'], settings: { foreground: c.fn } },
    { scope: ['string', 'string.quoted', 'string.template', 'punctuation.definition.string'], settings: { foreground: c.string } },
    { scope: ['constant.numeric', 'constant.language', 'constant.language.boolean', 'constant.language.null'], settings: { foreground: c.number } },
    { scope: ['entity.name.type', 'entity.name.class', 'support.type', 'support.class', 'meta.type.annotation'], settings: { foreground: c.type } },
    { scope: ['meta.decorator', 'variable.other.decorator', 'entity.name.decorator', 'punctuation.definition.decorator'], settings: { foreground: c.decorator } },
    { scope: ['variable', 'variable.other', 'variable.parameter'], settings: { foreground: c.variable } },
    { scope: ['punctuation', 'meta.brace', 'meta.bracket'], settings: { foreground: c.punct } },
  ],
};

export default docsCodeTheme;
