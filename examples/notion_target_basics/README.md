# Notion target — basics

Minimal example for the cocoindex Notion target connector. Declares three rows
of `Person` data; cocoindex creates the Notion database for you
(`managed_by="system"`, the default) and syncs the rows — creating new pages,
patching changed ones, and archiving pages whose source row goes away.

## What it shows

| Behavior | How to trigger | What you see in Notion |
|---|---|---|
| Initial insert | First `cocoindex update` | Three pages created |
| Idempotent re-run | Re-run with no changes | ~0.2s, zero Notion writes |
| Field update | Change a field value on one row | That page PATCHed |
| Automatic archive | Remove a row from `PEOPLE` and re-run | That page archived in Notion |
| Re-add | Add the row back | New page (or revived) |

The archive step is the key win over hand-rolled `notion-client` plumbing —
cocoindex tracks what it wrote on the previous run and reconciles it against
what's declared this run.

## Setup

1. **Create (or pick) a Notion page** to hold the database. CocoIndex creates
   the database — titled `People`, with the `Name` / `Email` / `Role` /
   `Active` properties this example declares — under it on the first run.

   The sandbox page used while developing this example looked like this:

   ![Sandbox parent page](https://cocoindex.io/blobs/docs/img/examples/notion_target_basics/test-sandbox-page.png)

2. **Share the page** with your Notion integration: top-right `···` →
   Connections → `+ Add connections` → select your integration. Every parent
   page in the path must be shared — Notion checks access at the page level.

3. **Grab the page ID** from the page URL (the 32-char hex after the title).

4. **Export tokens** and run:

   ```sh
   export NOTION_TOKEN=ntn_...
   export NOTION_PARENT_PAGE=<your-parent-page-id>
   cocoindex update main.py:NotionTargetBasics
   ```

After the first run, the database fills up:

![Demo database after first run, with Alan archived](https://cocoindex.io/blobs/docs/img/examples/notion_target_basics/demo-database-after-archive.png)

(Pictured: after the `Alan Turing` row was removed from `PEOPLE` and the example
was re-run — his page was automatically archived, leaving only Ada and Grace.)

## Try the lifecycle

Edit `main.py`'s `PEOPLE` list and re-run `cocoindex update` after each change:

```python
# 1. Add a new row -> CocoIndex creates a new page
Person(name="Margaret Hamilton", email="margaret@example.com",
       role="Engineer", active=True),

# 2. Change a value -> CocoIndex PATCHes that page
Person(name="Ada Lovelace", email="ada@new.example.com", ...),

# 3. Remove a row -> CocoIndex archives that page
# (delete the line)
```

## Switch the delete behavior

Pass `on_delete=...` to change what happens when a row is removed:

```python
target = await notion.mount_database_target(
    notion_client,
    schema=schema,
    parent_page_id=os.environ["NOTION_PARENT_PAGE"],
    title="People",
    on_delete=notion.OnDelete.HARD,    # send page to trash
    # on_delete=notion.OnDelete.IGNORE  # leave page alone
)
```

Default is `OnDelete.ARCHIVE` — reversible, matches what Notion users expect.

## Image files to add (developer note)

The two screenshots referenced above live in the `cocoindex-io/blobs` repo:

| README link → file path (in blobs repo) |
|---|
| `public/docs/img/examples/notion_target_basics/test-sandbox-page.png` |
| `public/docs/img/examples/notion_target_basics/demo-database-after-archive.png` |

Drop the images at those paths, then `git add . && git commit && git push` from
the blobs repo — the cocoindex.io Pages workflow will publish them.
