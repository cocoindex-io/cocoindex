#!/usr/bin/env python3
"""
Write CocoIndex's built-in Google OAuth client into the package before a build.

The release workflow runs this in every job that builds a wheel or the sdist,
with the repository's Actions secrets GOOGLE_OAUTH_CLIENT_ID and
GOOGLE_OAUTH_CLIENT_SECRET in the environment. It writes the client in the
shape of the JSON Google downloads for a Desktop app client, which the Google
Drive connector reads when `cocoindex auth sign-in google-drive` gets no
client file.

- Neither secret set: writes nothing, and the build has no built-in client.
  This is the state until Google has verified the client.
- One set without the other: fails, rather than ship half a client.

A Desktop client's secret is not confidential (Google: "Desktop apps are public
clients and cannot keep secrets confidential"); it is kept out of git because
Google's OAuth policies forbid committing client credentials to a public
repository. The file is git-ignored and packed by pyproject.toml's
[tool.maturin] include. Never prints either value.

Assumes current working directory is the repository root.
Works on macOS, Linux, and Windows.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

CLIENT_FILE = Path("python/cocoindex/connectors/google_drive/_builtin_client.json")


def main() -> int:
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    if not client_id and not client_secret:
        print("No built-in Google OAuth client: the Actions secrets are not set.")
        return 0
    if not client_id or not client_secret:
        print(
            "Only one of GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET is "
            "set; set both, or neither to build without the built-in client.",
            file=sys.stderr,
        )
        return 1
    document = {"installed": {"client_id": client_id, "client_secret": client_secret}}
    CLIENT_FILE.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote the built-in Google OAuth client to {CLIENT_FILE}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
