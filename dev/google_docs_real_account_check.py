"""Check the Google Docs target against a real Google account, end to end.

Run it once you have signed in with ``cocoindex auth sign-in google-docs``
(``--token-path`` if you signed in elsewhere):

    uv run python dev/google_docs_real_account_check.py

It writes one doc in your My Drive and, at the end, moves it to the trash. On
the way it asks you, in the browser, to comment on one section and edit
another; then it changes a third section and checks:

1. the doc it created reads back as exactly what was declared (nothing left
   to write), which is what makes "a person's edit" detectable at all;
2. a rerun with unchanged content makes no request to Google;
3. a change to one section rewrites that section only: the commented section
   keeps its text, and the comment is still there, quoting the same text;
4. the section you edited keeps your text, and the run warns about it;
5. dropping the declaration moves the doc to the trash (not deleted for good).

The state database lives in a temporary directory, so every run starts fresh.
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import tempfile
import time
from typing import Any

import cocoindex as coco
from cocoindex.connectors import google_docs
from cocoindex.connectors.google_docs import _docs, _markdown, _target

GDOCS = coco.ContextKey[google_docs.UserAccount]("google_docs_real_account_check")

V1 = """\
# CocoIndex real-account check

## Alpha

The alpha section, version one.

## Beta

Comment on this sentence in Beta.

- a bullet with **bold**, *italic*, `code` and [a link](https://cocoindex.io)
  - a nested bullet, https://example.com/x

## Gamma

Edit a word in this Gamma sentence.
"""

V2 = V1.replace("version one.", "version two.").replace(
    "Edit a word in this Gamma sentence.", "Edit a word in this Gamma sentence, v2."
)

COMMENTED = "Comment on this sentence in Beta."


class _Counting:
    """An ``httplib2.Http`` that counts the requests made through it."""

    count = 0

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def request(self, *args: Any, **kwargs: Any) -> Any:
        _Counting.count += 1
        return self._inner.request(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _pause(prompt: str) -> None:
    input(f"\n{prompt}\nPress Enter when done. ")


def _yes(prompt: str) -> bool:
    return input(f"\n{prompt} [y/n] ").strip().lower().startswith("y")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--token-path", default=None, help="the sign-in to use")
    args = parser.parse_args()

    account = google_docs.UserAccount(args.token_path)
    if not account.is_signed_in():
        print("No Google Docs sign-in. Run: cocoindex auth sign-in google-docs")
        return 2
    print(f"Signed in as {account.email}.")

    warnings: list[str] = []

    class _Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.levelno >= logging.WARNING:
                warnings.append(record.getMessage())

    logging.getLogger(_target.__name__).addHandler(_Collect())
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    plain_http = _target._new_http
    _target._new_http = lambda: _Counting(plain_http())  # type: ignore[assignment]

    name = f"CocoIndex real-account check {time.strftime('%Y-%m-%d %H:%M:%S')}"
    content: dict[str, str | None] = {"content": V1}

    @coco.fn
    def app_main() -> None:
        if content["content"] is not None:
            google_docs.declare_doc(GDOCS, name, content["content"])

    results: list[tuple[str, bool]] = []

    def check(what: str, ok: bool) -> None:
        results.append((what, ok))
        print(f"{'PASS' if ok else 'FAIL'}  {what}")

    with tempfile.TemporaryDirectory() as tmp:
        settings = coco.Settings.from_env(db_path=pathlib.Path(tmp) / "db")
        env = coco.Environment(settings)
        env.context_provider.provide(GDOCS, account)
        app = coco.App(
            coco.AppConfig(name="GoogleDocsRealAccountCheck", environment=env), app_main
        )

        app.update_blocking()
        google = _target._Google(account)
        files = (
            google._drive.files()
            .list(q=f"name = '{name}' and trashed = false", fields="files(id)")
            .execute()
        )["files"]
        if len(files) != 1:
            print(f"Expected one doc named {name!r}, found {len(files)}.")
            return 1
        doc_id = files[0]["id"]
        print(f"\nThe doc: {_target.doc_url(doc_id)}")

        state = _docs.read_document(google.get(doc_id))
        blocks = _markdown.parse_blocks(V1)
        leftover = _docs.plan_write(state, blocks, None).requests
        check(
            "the new doc reads back as declared (nothing left to write)", not leftover
        )
        if leftover:
            print("  left to write:", leftover[:5])

        _Counting.count = 0
        app.update_blocking()
        check("an unchanged rerun makes no request", _Counting.count == 0)

        _pause(
            f"In the doc, add a comment on the sentence {COMMENTED!r} (section "
            "Beta), and change one word in the Gamma sentence. Leave Alpha alone."
        )
        before = _docs.read_document(google.get(doc_id))
        beta_id = blocks[2].block_id
        beta_before = before.content(before.ranges[beta_id])
        comments_before = (
            google._drive.comments()
            .list(
                fileId=doc_id, fields="comments(id,quotedFileContent,deleted,resolved)"
            )
            .execute()
            .get("comments", [])
        )
        if not comments_before:
            print("No comment found on the doc; add one and run the check again.")
            return 1

        content["content"] = V2
        app.update_blocking()
        after = _docs.read_document(google.get(doc_id))
        alpha_text = [p.text for p in after.content(after.ranges[blocks[1].block_id])]
        check("Alpha was rewritten", "The alpha section, version two." in alpha_text)
        check(
            "Beta, the commented section, kept its text and named range",
            after.content(after.ranges[beta_id]) == beta_before
            and after.ranges[beta_id].named_range_id
            == before.ranges[beta_id].named_range_id,
        )
        comments_after = {
            c["id"]: c
            for c in google._drive.comments()
            .list(
                fileId=doc_id, fields="comments(id,quotedFileContent,deleted,resolved)"
            )
            .execute()
            .get("comments", [])
        }
        check(
            "the comment is still there, quoting the same text",
            all(
                c["id"] in comments_after
                and not comments_after[c["id"]].get("deleted")
                and comments_after[c["id"]].get("quotedFileContent")
                == c.get("quotedFileContent")
                for c in comments_before
            ),
        )
        gamma_text = [p.text for p in after.content(after.ranges[blocks[3].block_id])]
        check(
            "Gamma kept your edit (the flow's v2 was not written)",
            not any("v2" in t for t in gamma_text)
            and "Edit a word in this Gamma sentence." not in gamma_text,
        )
        check("the run warned about Gamma", any("Gamma" in w for w in warnings))
        check(
            "in the browser, the comment is still anchored to its sentence",
            _yes("Look at the doc: is the comment still on its sentence in Beta?"),
        )

        content["content"] = None
        app.update_blocking()
        trashed = (
            google._drive.files()
            .get(fileId=doc_id, fields="trashed")
            .execute()["trashed"]
        )
        check("dropping the declaration moved the doc to the trash", trashed is True)

    failed = [what for what, ok in results if not ok]
    print(f"\n{len(results) - len(failed)} of {len(results)} checks passed.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
