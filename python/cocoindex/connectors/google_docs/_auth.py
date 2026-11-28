"""The account the Google Docs target writes with: ``drive.file`` only."""

from __future__ import annotations

from cocoindex.connectors.google_drive import _oauth

DRIVE_FILE_SCOPE = "https://www.googleapis.com/auth/drive.file"

DOCS_GRANT = _oauth.Grant(
    provider="google-docs",
    scope=DRIVE_FILE_SCOPE,
    token_file_name="google_docs_token.json",
    notice=(
        "CocoIndex asks Google for access to only the files it creates in your "
        "Google Drive (drive.file): it creates Google Docs, and edits and trashes "
        "only the docs it created. It cannot see or change any other file."
    ),
    not_granted=(
        "The consent did not grant access to the files CocoIndex creates in "
        "Google Drive; sign in again and keep that permission selected."
    ),
    memo_namespace="cocoindex.google_docs.user_account",
)


class UserAccount(_oauth.UserAccountBase):
    """A personal Google account signed in to keep Google Docs, kept in a token file.

    Experimental. The sign-in asks for ``drive.file`` only: access to the files
    CocoIndex creates, nothing else in the Drive. Run ``sign_in`` once, outside
    any CocoIndex run (``cocoindex auth sign-in google-docs`` does the same from
    the shell); its token file is separate from the Google Drive source's, so
    signing in for one never changes the other. (Signing out revokes the OAuth
    client's access for the account at Google, which ends both.) A run provides
    ``UserAccount()`` under a ``ContextKey`` that ``declare_doc`` names.
    """

    _grant = DOCS_GRANT


__all__ = ["UserAccount"]
