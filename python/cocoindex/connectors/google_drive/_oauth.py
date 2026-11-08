"""OAuth sign-in for a personal Google account (experimental).

A ``UserAccount`` is a token file. ``sign_in`` fills it through the flow Google
recommends for command-line applications: the authorization code grant with
PKCE, redirected to a listener on the loopback interface, from a Desktop-app
OAuth client: CocoIndex's own when the release carries one, or one the user
created in their own Google Cloud project. A host with no browser prints the
authorization link instead and accepts the address the browser lands on,
pasted back. ``GoogleDriveSource`` then signs its requests with the stored
access token and refreshes it with the refresh token as it expires.

What the file holds is a credential, so this module keeps it out of
everything that persists or prints: the file is created ``0600`` and refused
when another user can read it; no token enters a memo key (an account is
keyed by its address), a log line, an exception message or the state
database. The flow itself validates ``state`` in constant time, binds the
listener before it opens the browser, and never sends a token over plain
HTTP to anything but the loopback listener.
"""

from __future__ import annotations

import base64
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import queue
import secrets
import socketserver
import stat
import sys
import threading
import time
import urllib.parse
import webbrowser
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from google.auth import credentials as google_credentials  # type: ignore

__all__ = ["SignInError", "SignInRequiredError", "UserAccount"]

_logger = logging.getLogger(__name__)

DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

AUTHORIZATION_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
REVOCATION_ENDPOINT = "https://oauth2.googleapis.com/revoke"
# ``about.get`` is the cheapest call that names the signed-in account and
# proves the Drive API is enabled for the client's project.
ABOUT_ENDPOINT = "https://www.googleapis.com/drive/v3/about"

# Bumped on an incompatible change to the file's shape; an older file means
# one sign-in again, never a migration.
TOKEN_FILE_VERSION = 1

# The whole interactive sign-in, including the person at the browser.
SIGN_IN_TIMEOUT_SECONDS = 300.0
HTTP_TIMEOUT = httpx.Timeout(30.0, connect=10.0)

# CocoIndex's own Desktop-app OAuth client, so that a sign-in needs no client
# file. The release workflow writes it from Actions secrets
# (.github/scripts/write_google_oauth_client.py) and it is not in git, so a
# build from source has none: its sign-in takes the user's own client.
BUILTIN_CLIENT_PATH = Path(__file__).with_name("_builtin_client.json")

# How to create a client of one's own, for a build without the built-in one.
OWN_CLIENT_DOCS = (
    "https://cocoindex.io/docs/connectors/google_drive#setting-up-the-oauth-client"
)

# The privacy policy's section on Google user data, which the sign-in links.
PRIVACY_POLICY = "https://cocoindex.io/privacy-policy/#google-user-data"


class SignInError(Exception):
    """A sign-in, sign-out or token refresh failed; the message says what to do."""


class SignInRequiredError(SignInError):
    """A run needs a signed-in account and has none that works: sign in again."""


class _Refusal(SignInError):
    """Google answered a token request with an OAuth error; ``code`` is its ``error``."""

    def __init__(self, code: str, description: str) -> None:
        detail = f" ({description})" if description else ""
        super().__init__(f"Google refused the request: {code}{detail}.")
        self.code = code
        self.description = description


def default_token_path() -> Path:
    """Where ``UserAccount()`` keeps its token: under the user's config directory."""
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
    else:
        xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
        base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "cocoindex" / "google_drive_token.json"


@dataclass
class _Tokens:
    """What the token file holds. The repr names the account only."""

    client_id: str
    client_secret: str
    refresh_token: str
    access_token: str
    expires_at: float  # POSIX seconds
    email: str

    def __repr__(self) -> str:
        return f"_Tokens(email={self.email!r})"


@dataclass(frozen=True)
class _Client:
    """A Desktop-app OAuth client, as its downloaded JSON describes it."""

    client_id: str
    client_secret: str
    # CocoIndex's own client (``BUILTIN_CLIENT_PATH``), not one the user created.
    builtin: bool = False

    def __repr__(self) -> str:
        return f"_Client(client_id={self.client_id!r})"


def _load_client(path: Path) -> _Client:
    """Read the client JSON the Google Cloud console downloads for a Desktop app."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SignInError(f"No OAuth client file at {path}.") from None
    except (OSError, ValueError) as exc:
        raise SignInError(f"Cannot read the OAuth client file {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise SignInError(f"{path} is not an OAuth client file.")
    if "web" in document and "installed" not in document:
        raise SignInError(
            f"{path} describes a Web application client; the sign-in needs a "
            "Desktop app client (create one under APIs & Services > Credentials)."
        )
    installed = document.get("installed")
    if not isinstance(installed, dict):
        raise SignInError(
            f"{path} is not the JSON of a Desktop app OAuth client "
            "(it has no 'installed' section)."
        )
    client_id = installed.get("client_id")
    client_secret = installed.get("client_secret")
    if not isinstance(client_id, str) or not isinstance(client_secret, str):
        raise SignInError(f"{path} has no client_id and client_secret.")
    return _Client(client_id=client_id, client_secret=client_secret)


def _resolve_client(path: Path | None) -> _Client:
    """The client a sign-in uses: the file at ``path``, or the built-in one for ``None``."""
    if path is not None:
        return _load_client(path)
    if not BUILTIN_CLIENT_PATH.is_file():
        raise SignInError(
            "This build of CocoIndex has no built-in OAuth client. Sign in with "
            "a Desktop app OAuth client of your own: pass the JSON file you "
            f"downloaded for it (see {OWN_CLIENT_DOCS})."
        )
    return replace(_load_client(BUILTIN_CLIENT_PATH), builtin=True)


# --- the token file ---------------------------------------------------------


def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
    """Write ``payload`` to ``path`` as JSON readable by this user only, atomically.

    The file is created 0600 from the start (never written and then chmod'ed),
    in a temp file beside the target that replaces it in one step, so a reader
    never sees a half-written file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _check_private(path: Path) -> None:
    """Refuse a token file another user can access (POSIX only: Windows has ACLs)."""
    if os.name == "nt":
        return
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise SignInError(
            f"The token file {path} is accessible to other users (mode {mode:04o}); "
            "run `chmod 600` on it, or delete it and sign in again."
        )


def _load_tokens(path: Path) -> _Tokens | None:
    """The stored tokens, or ``None`` when no usable sign-in is stored at ``path``."""
    if not path.exists():
        return None
    _check_private(path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SignInError(f"Cannot read the token file {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("version") != TOKEN_FILE_VERSION:
        return None
    try:
        return _Tokens(
            client_id=str(document["client_id"]),
            client_secret=str(document["client_secret"]),
            refresh_token=str(document["refresh_token"]),
            access_token=str(document["access_token"]),
            expires_at=float(document["expires_at"]),
            email=str(document["email"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _save_tokens(path: Path, tokens: _Tokens) -> None:
    _write_private_json(
        path,
        {
            "version": TOKEN_FILE_VERSION,
            "client_id": tokens.client_id,
            "client_secret": tokens.client_secret,
            "refresh_token": tokens.refresh_token,
            "access_token": tokens.access_token,
            "expires_at": tokens.expires_at,
            "email": tokens.email,
        },
    )


# --- talking to Google ------------------------------------------------------


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=HTTP_TIMEOUT)


def _oauth_error(response: httpx.Response) -> tuple[str, str]:
    """The ``error`` and ``error_description`` of a failed token request."""
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        return f"HTTP {response.status_code}", ""
    return (
        str(body.get("error") or f"HTTP {response.status_code}"),
        str(body.get("error_description") or ""),
    )


def _token_request(client: httpx.Client, form: dict[str, str]) -> dict[str, Any]:
    """POST ``form`` to the token endpoint; the error says what Google refused, never what was sent."""
    try:
        response = client.post(TOKEN_ENDPOINT, data=form)
    except httpx.HTTPError as exc:
        raise SignInError(f"Cannot reach Google's token endpoint: {exc}") from exc
    if response.status_code >= 400:
        raise _Refusal(*_oauth_error(response))
    try:
        payload = response.json()
    except ValueError as exc:
        raise SignInError("Google's token endpoint returned no JSON.") from exc
    if not isinstance(payload, dict) or not isinstance(
        payload.get("access_token"), str
    ):
        raise SignInError("Google's token endpoint returned no access token.")
    return payload


def _expires_at(payload: Mapping[str, Any], now: float) -> float:
    expires_in = payload.get("expires_in")
    seconds = float(expires_in) if isinstance(expires_in, (int, float)) else 3600.0
    return now + seconds


def _account_email(client: httpx.Client, access_token: str) -> str:
    """The signed-in account's address, from the Drive API.

    Also the first call that needs the Drive API enabled in the client's
    project, so its failure is explained here.
    """
    try:
        response = client.get(
            ABOUT_ENDPOINT,
            params={"fields": "user(emailAddress)"},
            headers={"Authorization": f"Bearer {access_token}"},
        )
    except httpx.HTTPError as exc:
        raise SignInError(f"Cannot reach the Google Drive API: {exc}") from exc
    if response.status_code == 403:
        raise SignInError(
            "The Google Drive API refused the signed-in account (HTTP 403). "
            "Enable the Google Drive API for the OAuth client's project "
            "(APIs & Services > Library) and sign in again."
        )
    if response.status_code >= 400:
        raise SignInError(
            f"The Google Drive API refused the signed-in account (HTTP {response.status_code})."
        )
    try:
        email = response.json()["user"]["emailAddress"]
    except (ValueError, KeyError, TypeError) as exc:
        raise SignInError("The Google Drive API did not name the account.") from exc
    if not isinstance(email, str) or not email:
        raise SignInError("The Google Drive API did not name the account.")
    return email


# What the token endpoint answers when the client itself is the problem
# (deleted, disabled, a wrong secret), not the user's grant.
_CLIENT_REFUSALS = frozenset({"invalid_client", "unauthorized_client"})


def _refresh(client: httpx.Client, tokens: _Tokens) -> _Tokens:
    """A new access token from the refresh token. Google does not rotate refresh tokens."""
    try:
        payload = _token_request(
            client,
            {
                "grant_type": "refresh_token",
                "refresh_token": tokens.refresh_token,
                "client_id": tokens.client_id,
                "client_secret": tokens.client_secret,
            },
        )
    except _Refusal as refusal:
        detail = f" ({refusal.description})" if refusal.description else ""
        if refusal.code == "invalid_grant":
            raise SignInRequiredError(
                f"Google refused the stored sign-in: invalid_grant{detail}. The "
                "consent may have expired (seven days for a client in Testing "
                "status), or the access was revoked. Sign in again."
            ) from None
        if refusal.code in _CLIENT_REFUSALS:
            raise SignInRequiredError(
                f"Google no longer accepts the OAuth client this sign-in used: "
                f"{refusal.code}{detail}. It was deleted or disabled, or its secret "
                "changed. Sign in again: with an up-to-date CocoIndex if the "
                "sign-in used its built-in client, or with a working Desktop app "
                "client of your own."
            ) from None
        raise
    now = time.time()
    return _Tokens(
        client_id=tokens.client_id,
        client_secret=tokens.client_secret,
        refresh_token=str(payload.get("refresh_token") or tokens.refresh_token),
        access_token=str(payload["access_token"]),
        expires_at=_expires_at(payload, now),
        email=tokens.email,
    )


def _revoke(client: httpx.Client, token: str) -> bool:
    """Ask Google to revoke the refresh token; whether it accepted. Best effort."""
    try:
        response = client.post(REVOCATION_ENDPOINT, data={"token": token})
    except httpx.HTTPError:
        return False
    return response.status_code < 400


# --- the interactive flow ---------------------------------------------------


def _say(message: str) -> None:
    """Print a line the person must act on, flushed: the flow is blocking while they read it."""
    print(message, flush=True)


def _read_stdin_line() -> str | None:
    """One line from stdin, or ``None`` when nothing more can be read from it."""
    stream = sys.stdin
    if stream is None:
        return None
    try:
        line = stream.readline()
    except (OSError, ValueError):
        return None
    return line or None


def has_browser(
    environ: Mapping[str, str] | None = None, platform: str = sys.platform
) -> bool:
    """Whether ``webbrowser.open`` would show the sign-in page to the person at this terminal.

    ``BROWSER`` set means they wired one up (VS Code's remote terminal does, and
    forwards the loopback port with it). Over SSH, or on Linux with no display,
    the host's browser would open on no screen they can see, or as a text
    browser inside the terminal; the flow then prints the link instead.
    """
    env = os.environ if environ is None else environ
    if env.get("BROWSER"):
        return True
    displayed = bool(env.get("DISPLAY") or env.get("WAYLAND_DISPLAY"))
    if any(key in env for key in ("SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY")):
        return displayed
    if platform == "darwin" or platform.startswith("win"):
        return True
    return displayed


@dataclass
class _Redirect:
    """The query of the authorization redirect, once it arrived."""

    params: dict[str, list[str]] | None = None


class _LoopbackServer(HTTPServer):
    """The redirect listener, bound to the loopback interface without a DNS lookup.

    ``HTTPServer.server_bind`` looks up the fully qualified name of the address
    it bound (a reverse DNS query for 127.0.0.1), which a slow resolver can
    stall for tens of seconds before the sign-in even starts: seen on macOS
    CI runners. Nothing here uses the name.
    """

    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)


class _RedirectHandler(BaseHTTPRequestHandler):
    """Answers the one redirect the flow waits for; everything else is a 404."""

    redirect: _Redirect

    def do_GET(self) -> None:  # noqa: N802 - the handler API
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        if "code" in params or "error" in params:
            self.redirect.params = params
            status, body = 200, b"CocoIndex: signed in. You can close this tab."
        else:
            status, body = 404, b"CocoIndex: waiting for Google's redirect."
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return  # the flow prints its own instructions


def _parse_pasted_redirect(text: str) -> dict[str, list[str]] | None:
    """The query of a pasted redirect address, or ``None`` when the line is not one."""
    text = text.strip()
    query = urllib.parse.urlsplit(text).query
    if not query and "://" not in text:
        query = text.lstrip("?")
    params = urllib.parse.parse_qs(query)
    return params if "code" in params or "error" in params else None


def _pump_lines(
    read_line: Callable[[], str | None], lines: queue.SimpleQueue[str]
) -> None:
    while (line := read_line()) is not None:
        lines.put(line)


def _await_redirect(
    server: _LoopbackServer,
    redirect: _Redirect,
    *,
    timeout: float,
    read_line: Callable[[], str | None],
    say: Callable[[str], None],
) -> dict[str, list[str]]:
    """Serve until the redirect arrives at the listener or is pasted, or time out.

    stdin is read on a daemon thread because the read cannot be cancelled; a
    pasted line that is not the redirect gets a hint and the wait goes on.
    """
    pasted: queue.SimpleQueue[str] = queue.SimpleQueue()
    threading.Thread(target=_pump_lines, args=(read_line, pasted), daemon=True).start()
    deadline = time.monotonic() + timeout
    server.timeout = 0.5
    while time.monotonic() < deadline:
        if redirect.params is not None:
            return redirect.params
        try:
            line = pasted.get_nowait()
        except queue.Empty:
            server.handle_request()  # returns after ``server.timeout`` when idle
            continue
        if not line.strip():
            continue
        params = _parse_pasted_redirect(line)
        if params is None:
            say(
                "That is not the address the sign-in ended on; it starts with "
                "http://127.0.0.1:... and contains code= and state=. Paste the whole address:"
            )
            continue
        return params
    raise SignInError("The sign-in timed out before Google redirected back.")


def _data_notice(token_path: Path) -> str:
    """What the sign-in asks Google for, and where it keeps what Google returns."""
    return (
        "CocoIndex asks Google for read-only access to your Google Drive.\n"
        f"The sign-in is stored at {token_path}, on this computer only; "
        "nothing is sent to CocoIndex.\n"
        f"How CocoIndex handles Google user data: {PRIVACY_POLICY}\n"
    )


def _instructions(url: str, port: int, browser_opened: bool) -> str:
    if browser_opened:
        return (
            "Finish the sign-in in the browser. If none opened, visit:\n"
            f"\n  {url}\n\n"
            "If the sign-in ends on a page that does not load, copy that page's "
            "address from the address bar and paste it here. If Google shows an "
            "error page instead of asking for consent, press Ctrl+C: the sign-in "
            "cannot finish."
        )
    return (
        "No browser can open on this host, so use one on another machine.\n"
        "\n"
        "1. Open this link there and sign in:\n"
        f"\n  {url}\n\n"
        f"   The browser then lands on http://127.0.0.1:{port}/?code=...&state=... "
        "and fails to load:\n"
        "   that address is the other machine, not this host.\n"
        "\n"
        "   If Google shows an error page instead of asking for consent (for\n"
        "   example 'Access blocked'), the sign-in cannot finish here: press\n"
        "   Ctrl+C. 'admin_policy_enforced' means a Google Workspace administrator\n"
        "   blocks this app.\n"
        "\n"
        "2. Copy the whole address from the address bar and paste it here.\n"
        "   (If instead the page says the sign-in completed, this port was "
        "forwarded to this host and nothing is left to paste.)\n"
        "\n"
        "Paste the address here:"
    )


def _run_consent(
    client_cfg: _Client,
    *,
    http_client: httpx.Client,
    open_browser: Callable[[str], object],
    read_line: Callable[[], str | None],
    say: Callable[[str], None],
    browser_available: bool,
    timeout: float,
) -> dict[str, Any]:
    """The authorization code grant with PKCE over a loopback redirect; the token response."""
    verifier = secrets.token_urlsafe(64)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    state = secrets.token_urlsafe(32)
    redirect = _Redirect()
    handler = type("_Handler", (_RedirectHandler,), {"redirect": redirect})
    # Bound before the browser opens, so the redirect URI names a port this
    # process already holds.
    server = _LoopbackServer(("127.0.0.1", 0), handler)
    try:
        port = server.server_address[1]
        redirect_uri = f"http://127.0.0.1:{port}/"
        url = (
            AUTHORIZATION_ENDPOINT
            + "?"
            + urllib.parse.urlencode(
                {
                    "response_type": "code",
                    "client_id": client_cfg.client_id,
                    "redirect_uri": redirect_uri,
                    "scope": DRIVE_READONLY_SCOPE,
                    "state": state,
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                    # A refresh token on every sign-in, not only the first consent.
                    "access_type": "offline",
                    "prompt": "consent",
                }
            )
        )
        if browser_available:
            open_browser(url)
        say(_instructions(url, port, browser_available))
        params = _await_redirect(
            server, redirect, timeout=timeout, read_line=read_line, say=say
        )
    finally:
        server.server_close()

    if "error" in params:
        error = params["error"][0]
        if error == "access_denied":
            raise SignInError("The sign-in was cancelled on the consent screen.")
        raise SignInError(f"Google refused the sign-in: {error}.")
    returned_state = params.get("state", [""])[0]
    if not secrets.compare_digest(returned_state, state):
        raise SignInError(
            "The redirect does not belong to this sign-in (its state does not "
            "match); nothing was stored. Start the sign-in again."
        )
    code = params.get("code", [""])[0]
    if not code:
        raise SignInError("Google's redirect carried no authorization code.")
    try:
        return _token_request(
            http_client,
            {
                "grant_type": "authorization_code",
                "code": code,
                "code_verifier": verifier,
                "client_id": client_cfg.client_id,
                "client_secret": client_cfg.client_secret,
                "redirect_uri": redirect_uri,
            },
        )
    except _Refusal as refusal:
        if refusal.code not in _CLIENT_REFUSALS:
            raise
        if client_cfg.builtin:
            raise SignInError(
                f"Google refused CocoIndex's built-in OAuth client: {refusal.code}. "
                "Upgrade CocoIndex and sign in again, or sign in with a Desktop app "
                f"OAuth client of your own (see {OWN_CLIENT_DOCS})."
            ) from None
        raise SignInError(
            f"Google refused the OAuth client in the client file: {refusal.code}. "
            "Download the current JSON of the Desktop app client (its secret may "
            "have been reset) and sign in again."
        ) from None


def run_sign_in(
    client_secrets_path: Path | None,
    token_path: Path,
    *,
    http_client: httpx.Client | None = None,
    open_browser: Callable[[str], object] = webbrowser.open,
    read_line: Callable[[], str | None] = _read_stdin_line,
    say: Callable[[str], None] = _say,
    browser_available: bool | None = None,
    timeout: float = SIGN_IN_TIMEOUT_SECONDS,
) -> str:
    """Sign in and store the tokens at ``token_path``; the account's address.

    ``client_secrets_path`` is the downloaded JSON of the user's own Desktop app
    client; ``None`` takes the client built into this release, and fails when
    the build has none.

    Everything the flow touches outside this process is a parameter, so a
    test can drive it against fake endpoints: ``http_client`` for Google, ``open_browser``
    for the browser, ``read_line`` for the pasted redirect, ``say`` for what
    the person reads, and ``browser_available`` to force the no-browser path.
    """
    client_cfg = _resolve_client(client_secrets_path)
    # Said before the browser opens or the link prints: Google's verification
    # of a restricted scope wants the app itself to disclose how it uses the
    # data, not only the privacy policy.
    say(_data_notice(token_path))
    if browser_available is None:
        browser_available = has_browser()
    owned = http_client is None
    http_client = _http_client() if http_client is None else http_client
    try:
        payload = _run_consent(
            client_cfg,
            http_client=http_client,
            open_browser=open_browser,
            read_line=read_line,
            say=say,
            browser_available=browser_available,
            timeout=timeout,
        )
        granted = str(payload.get("scope") or "").split()
        if granted and DRIVE_READONLY_SCOPE not in granted:
            raise SignInError(
                "The consent did not grant read access to Google Drive; sign in "
                "again and keep the Drive permission selected."
            )
        refresh_token = payload.get("refresh_token")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise SignInError(
                "Google returned no refresh token, so the sign-in could not be "
                "kept. Remove the app at https://myaccount.google.com/permissions "
                "and sign in again."
            )
        now = time.time()
        access_token = str(payload["access_token"])
        tokens = _Tokens(
            client_id=client_cfg.client_id,
            client_secret=client_cfg.client_secret,
            refresh_token=refresh_token,
            access_token=access_token,
            expires_at=_expires_at(payload, now),
            email=_account_email(http_client, access_token),
        )
    finally:
        if owned:
            http_client.close()
    _save_tokens(token_path, tokens)
    return tokens.email


# --- the public handle -------------------------------------------------------


class UserAccount:
    """A personal Google account signed in with OAuth, kept in a token file.

    Experimental. ``sign_in`` is the interactive step; run it once, outside any
    CocoIndex run (``cocoindex google-drive sign-in`` does the same from the
    shell). A run then passes ``UserAccount()`` to ``GoogleDriveSource``, which
    reads the file and raises ``SignInRequiredError`` when it holds no usable
    sign-in.
    """

    def __init__(self, token_path: str | os.PathLike[str] | None = None) -> None:
        self._token_path = (
            Path(token_path).expanduser()
            if token_path is not None
            else default_token_path()
        )

    def __repr__(self) -> str:
        return f"UserAccount(token_path={str(self._token_path)!r})"

    @property
    def token_path(self) -> Path:
        return self._token_path

    def sign_in(self, client_secrets_path: str | os.PathLike[str] | None = None) -> str:
        """Sign in interactively with a Desktop-app OAuth client; the account's address.

        ``client_secrets_path`` is the JSON downloaded for a client of your own;
        without it the sign-in uses the client built into this CocoIndex
        release, and raises ``SignInError`` when the build has none. Opens the
        consent page in the browser, or prints the link when this host has
        none, and stores the tokens readable by this user only.
        """
        path = (
            Path(client_secrets_path).expanduser()
            if client_secrets_path is not None
            else None
        )
        return run_sign_in(path, self._token_path)

    def sign_out(self) -> bool:
        """Revoke the stored sign-in at Google and delete the token file; whether one existed.

        The file is deleted even when Google could not be reached: the stored
        tokens must not outlive the sign-out.
        """
        tokens = _load_tokens(self._token_path)
        if tokens is not None:
            with _http_client() as http_client:
                revoked = _revoke(http_client, tokens.refresh_token)
            if not revoked:
                _logger.warning(
                    "Google did not confirm revoking the sign-in of %s; the token file "
                    "is deleted anyway. To be sure, remove the app at "
                    "https://myaccount.google.com/permissions.",
                    tokens.email,
                )
        existed = self._token_path.exists()
        self._token_path.unlink(missing_ok=True)
        return existed

    @property
    def email(self) -> str:
        """The signed-in account's address; ``SignInRequiredError`` when none is signed in."""
        return self._tokens().email

    def is_signed_in(self) -> bool:
        return _load_tokens(self._token_path) is not None

    def __coco_memo_key__(self) -> object:
        # The account, never its tokens: a refreshed token must not invalidate
        # memoized work, a different account must.
        return ("cocoindex.google_drive.user_account", self.email)

    def _tokens(self) -> _Tokens:
        tokens = _load_tokens(self._token_path)
        if tokens is None:
            raise SignInRequiredError(
                f"No usable Google sign-in is stored at {self._token_path}. "
                "Sign in once with `cocoindex google-drive sign-in <client_secret.json>` "
                "or `UserAccount().sign_in(...)`."
            )
        return tokens

    def _credentials(self) -> _UserCredentials:
        """What signs Drive API requests; refreshed as it expires and persisted."""
        return _UserCredentials(self._token_path, self._tokens())


class _UserCredentials(google_credentials.Credentials):  # type: ignore[misc]
    """google-auth credentials over the token file, so ``googleapiclient`` can use them.

    google-auth calls ``refresh`` before a request when the access token is
    about to expire, and again on a 401. The refresh goes through this module
    (``httpx``) and writes the new token back to the file, so the next process
    starts without one.
    """

    def __init__(self, token_path: Path, tokens: _Tokens) -> None:
        super().__init__()  # type: ignore[no-untyped-call]
        self._token_path = token_path
        self._tokens = tokens
        self._lock = threading.Lock()
        self._set_tokens(tokens)

    def _set_tokens(self, tokens: _Tokens) -> None:
        self._tokens = tokens
        self.token = tokens.access_token
        # google-auth compares a naive UTC datetime.
        self.expiry = datetime.fromtimestamp(
            tokens.expires_at, tz=timezone.utc
        ).replace(tzinfo=None)

    def refresh(self, request: object) -> None:
        stale = self.token
        with self._lock:
            # Another thread may have refreshed while this one waited for the
            # lock; a refresh after that one is a wasted round trip.
            if self._tokens.access_token != stale:
                return
            with _http_client() as http_client:
                refreshed = _refresh(http_client, self._tokens)
            _save_tokens(self._token_path, refreshed)
            self._set_tokens(refreshed)
