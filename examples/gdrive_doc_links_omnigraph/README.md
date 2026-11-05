<h1 align="center">Turn Google Docs that link to each other into a <em>self-updating</em> graph in Omnigraph.</h1>

<p align="center">
  <b>Every Doc becomes a node, every link to another Doc becomes an edge — no LLM, no hand-written schema, about 100 lines of async Python.</b><br/>
  Edit, add or trash a Doc, re-run, and only that Doc is re-read; the graph follows.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/connectors/omnigraph/" title="Read the Omnigraph connector docs"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A team wiki in Google Docs is already a graph: the onboarding Doc links to the billing Doc, which links to the runbook. Drive just can't tell you what links where, which Docs nothing points to, or which links lead outside the folder. This example syncs a Drive folder into [Omnigraph](https://github.com/ModernRelay/omnigraph) — a lakehouse-native property-graph database served over HTTP by `omnigraph-server` — so you can ask the graph instead. It's the smallest end-to-end Omnigraph pipeline in this repo: no LLM, one node type, one edge type. For LLM extraction plus entity resolution, see [`meeting_notes_graph_omnigraph`](../meeting_notes_graph_omnigraph).

## How it works

- **`Doc`** nodes — one per Google Doc, keyed by its Drive file ID, carrying the Doc's name as `title` and a word count. Keying by ID means renaming a Doc keeps every link to it.
- **`LinksTo`** edges — `Doc -> Doc`, one per distinct other Doc it links to. A link is any `docs.google.com/document/d/<file id>` URL in the Doc: a link inserted with Insert → Link, a file chip inserted with `@`, or a URL pasted as text.

The whole pipeline is one function per Doc — read it in [`main.py`](main.py). The source exports each Doc as Markdown, so links arrive as `[text](https://docs.google.com/document/d/<id>/edit)`:

```python
@coco.fn(memo=True)
async def process_doc(file, docs, links) -> None:
    text = await file.read_text()
    doc_id = file.file_path.resolve()  # the Drive file ID
    docs.declare_node(node=Doc(doc_id=doc_id, title=file.file_path.path.name, words=len(text.split())))
    for target in sorted(set(_DOC_LINK_RE.findall(text)) - {doc_id}):
        links.declare_edge(from_id=doc_id, to_id=target)
```

What happens on each kind of change:

| You do this in Drive | The graph does this |
|---|---|
| Edit a Doc | Only that Doc is re-read; its `words` and outgoing edges are updated |
| Rename a Doc | Its `title` changes; edges to and from it stay |
| Link to a Doc outside the folder | The target appears as a stub node with only `doc_id`, so the edge still lands |
| Trash a Doc | Its node and outgoing edges go; if other Docs still link to it, it stays as a stub |

A stub stays in the graph after the last link to it is gone; `cocoindex drop main` removes it with everything else.

## Run it

**1. Start Omnigraph 0.14** with a graph named `doc-links` (its `doc-links.pg` may start empty), registered as the CLI target `local`. The [connector docs](https://cocoindex.io/docs/connectors/omnigraph/#local-development) have the full recipe; it's the same setup as [`meeting_notes_graph_omnigraph`](../meeting_notes_graph_omnigraph#run-it) with the graph id changed.

**2. Share a Drive folder** of Google Docs with a service account (see [Setting up a service account](https://cocoindex.io/docs/connectors/google_drive/#setting-up-a-service-account)). Only native Google Docs are read.

**3. Configure & install:**

```sh
cp .env.example .env     # set GOOGLE_SERVICE_ACCOUNT_CREDENTIAL, GOOGLE_DRIVE_ROOT_FOLDER_IDS
pip install -e .
```

The `OMNIGRAPH_*` variables point at the server, the graph and the cluster directory the connector deploys `Doc` and `LinksTo` through — the server picks the new types up live, no restart.

**4. Build the graph:**

```sh
cocoindex update main
```

The Google Drive source has no live mode, so re-run it after you edit the Docs, or poll:

```sh
while true; do cocoindex update main; sleep 30; done
```

**5. Ask the graph:**

```sh
# Every link, by title (a stub comes back without one)
omnigraph query --target local --graph doc-links --json -e \
  'query q() { match { $a: Doc $a linksTo $b } return { $a.title, $b.title, $b.doc_id } }'

# Backlinks: which Docs link to the Doc named "Onboarding"
omnigraph query --target local --graph doc-links --json -e \
  'query q() { match { $a: Doc $b: Doc { title: "Onboarding" } $a linksTo $b } return { $a.title } }'

# Every Doc
omnigraph query --target local --graph doc-links --json -e \
  'query q() { match { $n: Doc } return { $n.title, $n.words, $n.doc_id } }'
```

To remove everything this example put in the graph, types included, run `cocoindex drop main`.

---

<p align="center">
  If this turned your Drive into a graph, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/connectors/omnigraph/">Omnigraph connector</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/gdrive_doc_links_omnigraph" alt="" width="1" height="1" />
