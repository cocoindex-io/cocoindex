"""Restyle the generator-made example diagrams into the dark Category Standard
world without moving anything: same boxes, arrows, positions and text.
- cream ground and fills -> near-black tones; maroon lines -> hairlines
- red and green accents -> neutral grays (periwinkle/green are reserved)
- the red-dot tracked-caps kicker is removed and the top band cropped
- tracked-caps box labels -> sentence case, sans, no tracking
- fonts -> Geist / Geist Mono with system fallbacks"""
import re, sys, pathlib

ACR = {'pdf','llm','csv','json','api','url','html','id','sql','ast','ocr','tts','gpu','cpu','s3','oci','hn','md','png','jpg','uuid','rrf','ann','kv','pk','db','ui','http','baml','dspy','clip','xbrl','sec','10-k'}
PROPER = {'colpali':'ColPali','recursivesplitter':'RecursiveSplitter','gemini':'Gemini','piper':'Piper','doris':'Doris','qdrant':'Qdrant','postgres':'Postgres','neo4j':'Neo4j','lancedb':'LanceDB','kafka':'Kafka','openai':'OpenAI','surrealdb':'SurrealDB','turbopuffer':'Turbopuffer','sentencetransformer':'SentenceTransformer'}
def sentence(s):
    words = s.lower().split(' ')
    out = []
    for i, w in enumerate(words):
        core = re.sub(r'[^a-z0-9\-]', '', w)
        if core in ACR: w = w.upper() if w.lower() == core else w.replace(core, core.upper())
        if core in PROPER: w = w.replace(core, PROPER[core])
        out.append(w)
    s = ' '.join(out)
    first = s.split(' ')[0]
    if re.fullmatch(r'[a-z][a-z\-]*', first):
        s = s[0].upper() + s[1:]
    return s

COL = [
    ('#FAE9CB', '#141414'), ('#F6F4E9', '#121212'), ('#f8f3ed', '#121212'),
    ('#2A121B', '#EDEDED'), ('rgba(42,18,27,0.58)', '#A1A1A1'), ('rgba(42,18,27,0.16)', 'rgba(255,255,255,0.12)'),
    ('#D28D6F', 'rgba(255,255,255,0.22)'), ('#27E62B', 'rgba(255,255,255,0.55)'), ('#16A534', 'rgba(255,255,255,0.55)'),
]
MONO = "'Geist Mono', ui-monospace, SFMono-Regular, Menlo, monospace"
SANS = "Geist, system-ui, -apple-system, 'Segoe UI', sans-serif"

def restyle(src):
    s = src
    W, H = [float(x) for x in re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', s).groups()]
    # kicker: red dot + tracked caps text
    crop = 0
    m = re.search(r'<circle cx="[\d.]+" cy="([\d.]+)" r="6" fill="#BE5133"/>(<text[^>]*letter-spacing="3\.8"[^>]*>[^<]*</text>)', s)
    if m:
        crop = float(m.group(1)) + 27
        s = s.replace(m.group(0), '')
    # background rect (first full-size rect)
    s = re.sub(r'<rect width="%s" height="%s" fill="#FCF3D8"/>' % (re.escape(f'{W:g}'), re.escape(f'{H:g}')),
               f'<rect x="0" y="{crop:g}" width="{W:g}" height="{H-crop:g}" fill="#0C0C0C"/>', s, count=1)
    s = s.replace('fill="#FCF3D8"', 'fill="#111111"').replace('fill: #FCF3D8', 'fill: #111111')
    # red: container strokes and labels -> neutral
    s = s.replace('stroke="#BE5133"', 'stroke="rgba(255,255,255,0.4)"').replace('fill="#BE5133"', 'fill="#A1A1A1"')
    # maroon lines and arrowheads
    s = s.replace('stroke="#532638"', 'stroke="rgba(255,255,255,0.3)"').replace('stroke: #532638', 'stroke: rgba(255,255,255,0.3)')
    s = s.replace('fill="#532638"', 'fill="#8A8A8A"')
    for a, b in COL:
        s = s.replace(f'"{a}"', f'"{b}"').replace(f': {a}', f': {b}')
    # tracked caps labels -> sentence case sans
    def lab(mt):
        attrs, txt = mt.group(1), mt.group(2)
        attrs = re.sub(r'\s*letter-spacing="[^"]*"', '', attrs)
        attrs = re.sub(r'font-family="[^"]*"', f'font-family="{SANS}"', attrs)
        attrs = re.sub(r'font-weight="600"', 'font-weight="500"', attrs)
        return f'<text{attrs}>{sentence(txt)}</text>'
    s = re.sub(r'<text([^>]*letter-spacing="[\d.]+"[^>]*)>([^<]*)</text>', lab, s)
    s = s.replace("'JetBrains Mono', ui-monospace, 'SF Mono', Menlo, monospace", MONO)
    s = s.replace("Inter, 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif", SANS)
    if crop:
        s = re.sub(r'(<svg[^>]*?)width="[\d.]+" height="[\d.]+" viewBox="0 0 ([\d.]+) ([\d.]+)"',
                   lambda mm: f'{mm.group(1)}width="{W:g}" height="{H-crop:g}" viewBox="0 {crop:g} {W:g} {H-crop:g}"', s, count=1)
    return s

if __name__ == '__main__':
    src, dst = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    for f in sorted(src.glob('*.svg')):
        t = f.read_text()
        if 'JetBrains Mono' in t and 'fill="#FCF3D8"' in t:
            (dst / f.name).write_text(restyle(t))
    print(len(list(dst.glob('*.svg'))), 'restyled')
