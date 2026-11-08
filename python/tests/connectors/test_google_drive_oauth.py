"""The Google Drive connector's OAuth sign-in, against a fake Google.

No network and no real account: ``httpx.MockTransport`` plays Google's token,
revocation and Drive ``about`` endpoints, a fake browser follows the
authorization link to the loopback listener (or the redirect is pasted back),
and the token file lives under ``tmp_path``.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import pathlib
import socket
import stat
import threading
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import pytest
from click.testing import CliRunner

import cocoindex as coco
from tests.common import create_test_env

try:
    import httpx

    from cocoindex.connectors.google_drive import (
        GoogleDriveSource,
        GoogleDriveSourceSpec,
        SignInError,
        SignInRequiredError,
        UserAccount,
        list_files,
    )
    from cocoindex.connectors.google_drive import _oauth

    HAS_GOOGLE_DRIVE = True
except ImportError:
    HAS_GOOGLE_DRIVE = False

pytestmark = pytest.mark.skipif(
    not HAS_GOOGLE_DRIVE,
    reason="google-auth / google-api-python-client / httpx are not installed",
)

CLIENT_ID = "123-fake.apps.googleusercontent.com"
CLIENT_SECRET = "fake-client-secret"
EMAIL = "someone@example.com"


@dataclass
class FakeGoogle:
    """Google's side of the flow: the consent page, the token endpoint, revocation, Drive ``about``."""

    codes: dict[str, str] = field(default_factory=dict)  # code -> PKCE challenge
    refresh_tokens: set[str] = field(default_factory=set)
    revoked: list[str] = field(default_factory=list)
    issued: int = 0
    token_requests: list[dict[str, str]] = field(default_factory=list)
    redirect_state: str | None = None  # a wrong state to send back, when set
    refresh_error: tuple[int, dict[str, str]] | None = None
    about_status: int = 200
    grant_scope: str = _oauth.DRIVE_READONLY_SCOPE if HAS_GOOGLE_DRIVE else ""
    with_refresh_token: bool = True
    unreachable: bool = False
    client_refused: bool = False  # the client was deleted, disabled or its secret reset

    def consent(self, url: str) -> str:
        """What the browser does with the authorization link: the redirect it lands on."""
        parsed = urllib.parse.urlsplit(url)
        assert parsed.scheme == "https" and parsed.netloc == "accounts.google.com"
        params = dict(urllib.parse.parse_qsl(parsed.query))
        assert params["client_id"] == CLIENT_ID
        assert params["code_challenge_method"] == "S256"
        assert params["access_type"] == "offline"
        assert params["prompt"] == "consent"
        assert params["scope"] == _oauth.DRIVE_READONLY_SCOPE
        assert params["redirect_uri"].startswith("http://127.0.0.1:")
        code = f"code-{len(self.codes)}"
        self.codes[code] = params["code_challenge"]
        state = (
            self.redirect_state if self.redirect_state is not None else params["state"]
        )
        return (
            params["redirect_uri"]
            + "?"
            + urllib.parse.urlencode(
                {"code": code, "state": state, "scope": params["scope"]}
            )
        )

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.unreachable:
            raise httpx.ConnectError("no network")
        url = str(request.url).split("?")[0]
        if url == _oauth.TOKEN_ENDPOINT:
            return self._token(dict(urllib.parse.parse_qsl(request.content.decode())))
        if url == _oauth.REVOCATION_ENDPOINT:
            form = dict(urllib.parse.parse_qsl(request.content.decode()))
            self.revoked.append(form["token"])
            return httpx.Response(200)
        if url == _oauth.ABOUT_ENDPOINT:
            auth = request.headers.get("Authorization", "")
            assert auth.startswith("Bearer access-"), auth
            if self.about_status != 200:
                return httpx.Response(self.about_status, json={"error": {}})
            return httpx.Response(200, json={"user": {"emailAddress": EMAIL}})
        return httpx.Response(404)

    def _token(self, form: dict[str, str]) -> httpx.Response:
        self.token_requests.append(form)
        if self.client_refused:
            return httpx.Response(
                401,
                json={"error": "invalid_client", "error_description": "Unauthorized"},
            )
        assert form["client_id"] == CLIENT_ID and form["client_secret"] == CLIENT_SECRET
        if form["grant_type"] == "authorization_code":
            challenge = self.codes.pop(form["code"], None)
            expected = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(form["code_verifier"].encode()).digest()
                )
                .rstrip(b"=")
                .decode()
            )
            if challenge != expected:
                return httpx.Response(400, json={"error": "invalid_grant"})
            payload = {
                "access_token": self._access_token(),
                "expires_in": 3599,
                "scope": self.grant_scope,
                "token_type": "Bearer",
            }
            if self.with_refresh_token:
                refresh = f"refresh-{self.issued}"
                self.refresh_tokens.add(refresh)
                payload["refresh_token"] = refresh
            return httpx.Response(200, json=payload)
        if form["grant_type"] == "refresh_token":
            if self.refresh_error is not None:
                status, body = self.refresh_error
                return httpx.Response(status, json=body)
            if form["refresh_token"] not in self.refresh_tokens:
                return httpx.Response(
                    400,
                    json={"error": "invalid_grant", "error_description": "Bad Request"},
                )
            return httpx.Response(
                200,
                json={
                    "access_token": self._access_token(),
                    "expires_in": 3599,
                    "token_type": "Bearer",
                },
            )
        return httpx.Response(400, json={"error": "unsupported_grant_type"})

    def _access_token(self) -> str:
        self.issued += 1
        return f"access-{self.issued}"


@dataclass
class Flow:
    google: FakeGoogle
    http: httpx.Client
    client_secrets: pathlib.Path
    token_path: pathlib.Path
    said: list[str] = field(default_factory=list)
    opened: list[str] = field(default_factory=list)

    def browser(self, url: str) -> None:
        """A browser on this host: follows the link, then the redirect to the listener."""
        self.opened.append(url)
        landing = self.google.consent(url)
        threading.Thread(target=lambda: httpx.get(landing), daemon=True).start()

    def sign_in(self, **overrides: object) -> str:
        return self.sign_in_with(self.client_secrets, **overrides)

    def sign_in_with(self, client: pathlib.Path | None, **overrides: object) -> str:
        """The interactive sign-in against the fake; ``None`` takes the built-in client."""
        kwargs: dict[str, object] = dict(
            http_client=self.http,
            open_browser=self.browser,
            read_line=lambda: None,
            say=self.said.append,
            browser_available=True,
            timeout=10,
        )
        kwargs.update(overrides)
        return _oauth.run_sign_in(client, self.token_path, **kwargs)  # type: ignore[arg-type]

    def account(self) -> UserAccount:
        return UserAccount(self.token_path)

    def stored(self) -> dict[str, object]:
        return json.loads(self.token_path.read_text())  # type: ignore[no-any-return]


@pytest.fixture
def flow(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> Flow:
    google = FakeGoogle()

    def no_dns(name: str = "") -> str:
        # The listener must not resolve its own address: a slow resolver
        # stalled every sign-in on macOS CI runners for over 30 seconds.
        raise AssertionError(f"reverse DNS lookup of {name!r} during sign-in")

    monkeypatch.setattr(socket, "getfqdn", no_dns)

    def http_client() -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(google.handle))

    # Every client the module opens itself (refresh, sign-out) talks to the fake too.
    monkeypatch.setattr(_oauth, "_http_client", http_client)
    client_secrets = tmp_path / "client_secret.json"
    client_secrets.write_text(
        json.dumps(
            {
                "installed": {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "redirect_uris": ["http://localhost"],
                }
            }
        )
    )
    return Flow(
        google, http_client(), client_secrets, tmp_path / "state" / "token.json"
    )


def _mode(path: pathlib.Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


# --- the consent round trip --------------------------------------------------


def test_sign_in_round_trip_stores_a_private_token_file(flow: Flow) -> None:
    assert flow.sign_in() == EMAIL
    assert len(flow.opened) == 1
    _notice, instructions = flow.said
    assert flow.opened[0] in instructions

    stored = flow.stored()
    assert stored["email"] == EMAIL
    assert stored["refresh_token"] == "refresh-1"
    assert stored["access_token"] == "access-1"
    assert stored["client_id"] == CLIENT_ID
    if os.name != "nt":
        assert _mode(flow.token_path) == 0o600
    # The exchange carried the PKCE verifier and the registered redirect, once.
    (exchange,) = flow.google.token_requests
    assert exchange["grant_type"] == "authorization_code"
    assert exchange["redirect_uri"].startswith("http://127.0.0.1:")

    account = flow.account()
    assert account.is_signed_in()
    assert account.email == EMAIL


def test_a_redirect_with_another_state_is_refused_and_nothing_is_stored(
    flow: Flow,
) -> None:
    flow.google.redirect_state = "someone-elses"
    with pytest.raises(SignInError, match="state does not match"):
        flow.sign_in()
    assert not flow.token_path.exists()
    assert flow.google.token_requests == []  # the code was never exchanged


def test_a_cancelled_consent_is_reported(flow: Flow) -> None:
    def browser(url: str) -> None:
        redirect = flow.google.consent(url).split("?")[0]
        landing = redirect + "?" + urllib.parse.urlencode({"error": "access_denied"})
        threading.Thread(target=lambda: httpx.get(landing), daemon=True).start()

    with pytest.raises(SignInError, match="cancelled"):
        flow.sign_in(open_browser=browser)
    assert not flow.token_path.exists()


def test_a_consent_without_the_drive_scope_or_refresh_token_is_refused(
    flow: Flow,
) -> None:
    flow.google.grant_scope = "openid"
    with pytest.raises(SignInError, match="did not grant read access"):
        flow.sign_in()
    flow.google.grant_scope = _oauth.DRIVE_READONLY_SCOPE
    flow.google.with_refresh_token = False
    with pytest.raises(SignInError, match="no refresh token"):
        flow.sign_in()
    assert not flow.token_path.exists()


def test_a_project_without_the_drive_api_is_explained(flow: Flow) -> None:
    flow.google.about_status = 403
    with pytest.raises(SignInError, match="Enable the Google Drive API"):
        flow.sign_in()
    assert not flow.token_path.exists()


def test_a_web_client_or_a_missing_client_file_is_refused(
    flow: Flow, tmp_path: pathlib.Path
) -> None:
    web = tmp_path / "web.json"
    web.write_text(json.dumps({"web": {"client_id": "x", "client_secret": "y"}}))
    with pytest.raises(SignInError, match="Desktop app client"):
        _oauth.run_sign_in(
            web, flow.token_path, http_client=flow.http, browser_available=True
        )
    with pytest.raises(SignInError, match="No OAuth client file"):
        _oauth.run_sign_in(
            tmp_path / "missing.json",
            flow.token_path,
            http_client=flow.http,
            browser_available=True,
        )


# --- the notice on Google user data ------------------------------------------


def _assert_notice(text: str, token_path: pathlib.Path) -> None:
    assert "CocoIndex asks Google for read-only access to your Google Drive." in text
    assert f"The sign-in is stored at {token_path}, on this computer only" in text
    assert "nothing is sent to CocoIndex" in text
    assert "https://cocoindex.io/privacy-policy/#google-user-data" in text


def test_the_sign_in_discloses_its_use_of_google_data_before_the_consent(
    flow: Flow, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Google's verification of a restricted scope wants the app itself to say this."""
    from cocoindex.cli import cli

    said_when_the_browser_opened: list[str] = []

    def browser(url: str) -> None:
        said_when_the_browser_opened.extend(flow.said)
        flow.browser(url)

    flow.sign_in(open_browser=browser)
    (notice,) = said_when_the_browser_opened
    _assert_notice(notice, flow.token_path)

    # Printed to the terminal, from Python and from the CLI.
    _script_sign_in(flow, monkeypatch)
    flow.token_path.unlink()
    assert flow.account().sign_in(flow.client_secrets) == EMAIL
    _assert_notice(capsys.readouterr().out, flow.token_path)

    flow.token_path.unlink()
    result = CliRunner().invoke(
        cli,
        [
            "google-drive",
            "sign-in",
            str(flow.client_secrets),
            "--token-path",
            str(flow.token_path),
        ],
    )
    assert result.exit_code == 0, result.output
    _assert_notice(result.output, flow.token_path)


# --- the no-browser path -----------------------------------------------------


def test_without_a_browser_the_link_is_printed_and_the_redirect_pasted_back(
    flow: Flow,
) -> None:
    lines: list[str | None] = []

    def read_line() -> str | None:
        # Only after the link was printed (after the notice) can a person paste anything.
        while len(flow.said) < 2:
            pass
        if not lines:
            url = flow.said[1].split("\n\n  ")[1].split("\n")[0].strip()
            lines.extend(["not an address\n", flow.google.consent(url) + "\n", None])
        return lines.pop(0)

    assert flow.sign_in(browser_available=False, read_line=read_line) == EMAIL
    assert flow.opened == []  # no browser was opened
    _assert_notice(flow.said[0], flow.token_path)  # before the link
    assert "another machine" in flow.said[1]
    assert any("Paste the whole address" in s for s in flow.said[2:])
    assert flow.stored()["email"] == EMAIL


def test_the_pasted_redirect_may_be_the_whole_address_or_its_query() -> None:
    whole = "http://127.0.0.1:4242/?code=abc&state=xyz&scope=s"
    assert _oauth._parse_pasted_redirect(whole + "\n") == {
        "code": ["abc"],
        "state": ["xyz"],
        "scope": ["s"],
    }
    assert _oauth._parse_pasted_redirect("?code=abc&state=xyz") == {
        "code": ["abc"],
        "state": ["xyz"],
    }
    assert (
        _oauth._parse_pasted_redirect(
            "https://accounts.google.com/o/oauth2/v2/auth?client_id=1"
        )
        is None
    )
    assert _oauth._parse_pasted_redirect("just text") is None


def test_browser_detection_follows_the_display_and_ssh_environment() -> None:
    assert _oauth.has_browser({"BROWSER": "firefox"}, "linux")
    assert not _oauth.has_browser({}, "linux")
    assert _oauth.has_browser({"DISPLAY": ":0"}, "linux")
    assert not _oauth.has_browser({"SSH_CONNECTION": "1 2 3 4"}, "darwin")
    assert _oauth.has_browser({"SSH_CONNECTION": "1 2 3 4", "DISPLAY": ":10"}, "linux")
    assert _oauth.has_browser({}, "darwin")
    assert _oauth.has_browser({}, "win32")


# --- refresh, revocation, sign-out -------------------------------------------


def _expire(flow: Flow) -> None:
    stored = flow.stored()
    stored["expires_at"] = 0
    flow.token_path.write_text(json.dumps(stored))


def test_an_expired_access_token_is_refreshed_and_the_file_updated(flow: Flow) -> None:
    flow.sign_in()
    _expire(flow)
    creds: Any = flow.account()._credentials()  # google-auth is untyped
    assert creds.expired

    headers: dict[str, str] = {}
    creds.before_request(
        None, "GET", "https://www.googleapis.com/drive/v3/files", headers
    )
    assert headers["authorization"] == "Bearer access-2"
    assert not creds.expired
    stored = flow.stored()
    assert stored["access_token"] == "access-2"
    assert stored["refresh_token"] == "refresh-1"  # Google does not rotate it
    assert flow.google.token_requests[-1]["grant_type"] == "refresh_token"

    # A second refresh with the same stale token is one round trip, not two.
    creds.refresh(None)
    assert flow.stored()["access_token"] == "access-3"


def test_a_revoked_sign_in_asks_to_sign_in_again_without_leaking_tokens(
    flow: Flow,
) -> None:
    flow.sign_in()
    _expire(flow)
    flow.google.refresh_error = (
        400,
        {
            "error": "invalid_grant",
            "error_description": "Token has been expired or revoked.",
        },
    )
    creds = flow.account()._credentials()
    with pytest.raises(SignInRequiredError) as excinfo:
        creds.refresh(None)
    message = str(excinfo.value)
    assert "Sign in again" in message and "invalid_grant" in message
    assert "refresh-1" not in message and "access-1" not in message
    assert CLIENT_SECRET not in message
    assert repr(creds._tokens) == f"_Tokens(email={EMAIL!r})"


def test_sign_out_revokes_and_deletes(flow: Flow) -> None:
    flow.sign_in()
    account = flow.account()
    assert account.sign_out() is True
    assert flow.google.revoked == ["refresh-1"]
    assert not flow.token_path.exists()
    assert account.sign_out() is False
    assert not account.is_signed_in()


def test_sign_out_deletes_the_file_even_when_google_is_unreachable(flow: Flow) -> None:
    flow.sign_in()
    flow.google.unreachable = True
    assert flow.account().sign_out() is True
    assert not flow.token_path.exists()


# --- the token file ----------------------------------------------------------


@pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
def test_a_token_file_readable_by_others_is_refused(flow: Flow) -> None:
    flow.sign_in()
    flow.token_path.chmod(0o644)
    with pytest.raises(SignInError, match="chmod 600"):
        flow.account().email
    flow.token_path.chmod(0o600)
    assert flow.account().email == EMAIL


def test_a_token_file_of_another_version_means_signing_in_again(flow: Flow) -> None:
    flow.sign_in()
    stored = flow.stored()
    stored["version"] = 99
    flow.token_path.write_text(json.dumps(stored))
    account = flow.account()
    assert not account.is_signed_in()
    with pytest.raises(SignInRequiredError, match="No usable Google sign-in"):
        account.email


def test_the_default_token_path_follows_the_config_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    if os.name == "nt":
        monkeypatch.setenv("APPDATA", str(tmp_path))
    else:
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert (
        UserAccount().token_path == tmp_path / "cocoindex" / "google_drive_token.json"
    )
    assert UserAccount(tmp_path / "t.json").token_path == tmp_path / "t.json"


# --- what a run sees ---------------------------------------------------------


def test_a_run_without_a_sign_in_says_how_to_sign_in(flow: Flow) -> None:
    source = GoogleDriveSource(root_folder_ids=["root"], user_account=flow.account())

    async def first() -> None:
        async for _ in source.files():
            break

    with pytest.raises(SignInRequiredError) as excinfo:
        asyncio.run(first())
    message = str(excinfo.value)
    assert str(flow.token_path) in message
    assert "cocoindex google-drive sign-in" in message


def test_exactly_one_credential_kind(flow: Flow) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        GoogleDriveSourceSpec(None, ["root"])
    with pytest.raises(ValueError, match="exactly one"):
        GoogleDriveSourceSpec("sa.json", ["root"], user_account=flow.account())
    # The service-account form is unchanged, positional arguments included.
    assert GoogleDriveSourceSpec("sa.json", ["root"]).user_account is None


def test_the_memo_key_is_the_account_not_its_tokens(flow: Flow) -> None:
    flow.sign_in()
    account = flow.account()
    key = account.__coco_memo_key__()
    assert key == ("cocoindex.google_drive.user_account", EMAIL)
    for secret in (CLIENT_SECRET, "refresh-1", "access-1"):
        assert secret not in repr(key) and secret not in repr(account)
    _expire(flow)
    flow.account()._credentials().refresh(None)  # a new access token
    assert flow.account().__coco_memo_key__() == key


@pytest.mark.asyncio
async def test_a_refreshed_token_keeps_memoized_work_and_another_account_does_not(
    flow: Flow, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow.sign_in()
    calls: list[str] = []

    @coco.fn(memo=True)
    async def work(account: UserAccount) -> str:
        calls.append(account.email)
        return account.email

    @coco.fn
    async def main() -> str:
        return await work(flow.account())

    env = create_test_env(__file__, suffix="user_account_memo")
    app = coco.App(coco.AppConfig(name="user_account_memo", environment=env), main)
    assert await app.update() == EMAIL
    _expire(flow)
    flow.account()._credentials().refresh(None)
    assert await app.update() == EMAIL
    assert calls == [EMAIL]

    stored = flow.stored()
    stored["email"] = "other@example.com"
    flow.token_path.write_text(json.dumps(stored))
    assert await app.update() == "other@example.com"
    assert calls == [EMAIL, "other@example.com"]


def test_the_service_builds_on_user_credentials(
    flow: Flow, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``list_files`` with a user account builds the Drive service on the stored token."""
    flow.sign_in()
    from cocoindex.connectors.google_drive import _source

    seen: list[object] = []

    def fake_build(spec: GoogleDriveSourceSpec) -> object:
        seen.append(_source._credentials(spec))

        class Files:
            def list(self, **kwargs: object) -> object:
                return type(
                    "R", (), {"execute": lambda self, num_retries=0: {"files": []}}
                )()

        return type("Service", (), {"files": lambda self: Files()})()

    monkeypatch.setattr(_source, "_build_service", fake_build)
    spec = GoogleDriveSourceSpec(None, ["root"], user_account=flow.account())
    assert list(list_files(spec)) == []
    (creds,) = seen
    assert isinstance(creds, _oauth._UserCredentials)
    assert creds.token == "access-1"


# --- the CLI ------------------------------------------------------------------


def _script_sign_in(flow: Flow, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every interactive sign-in, the CLI's included, run against the fake.

    What the sign-in says still goes to stdout, as a real one's does.
    """
    interactive = _oauth.run_sign_in

    def scripted(client: pathlib.Path | None, token: pathlib.Path, **kw: object) -> str:
        return interactive(
            client,
            token,
            http_client=flow.http,
            open_browser=flow.browser,
            read_line=lambda: None,
            browser_available=True,
            timeout=10,
        )

    monkeypatch.setattr(_oauth, "run_sign_in", scripted)


def test_cli_sign_in_and_sign_out(flow: Flow, monkeypatch: pytest.MonkeyPatch) -> None:
    from cocoindex.cli import cli

    _script_sign_in(flow, monkeypatch)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "google-drive",
            "sign-in",
            str(flow.client_secrets),
            "--token-path",
            str(flow.token_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"Signed in as {EMAIL}." in result.output
    result = runner.invoke(
        cli, ["google-drive", "sign-out", "--token-path", str(flow.token_path)]
    )
    assert result.exit_code == 0, result.output
    assert "Signed out." in result.output
    assert not flow.token_path.exists()
    result = runner.invoke(
        cli, ["google-drive", "sign-out", "--token-path", str(flow.token_path)]
    )
    assert "No sign-in was stored." in result.output


def test_a_refused_client_is_named_at_refresh_and_at_sign_in(flow: Flow) -> None:
    """A deleted or disabled client, or a reset secret, is not the user's grant: say so."""
    flow.sign_in()
    _expire(flow)
    flow.google.client_refused = True
    with pytest.raises(SignInRequiredError, match="no longer accepts the OAuth client"):
        flow.account()._credentials().refresh(None)
    with pytest.raises(SignInError, match="Download the current JSON"):
        flow.sign_in()


# --- CocoIndex's built-in client ---------------------------------------------


def test_without_a_client_file_the_built_in_client_signs_in(
    flow: Flow, monkeypatch: pytest.MonkeyPatch
) -> None:
    from cocoindex.cli import cli

    # The release workflow writes the built-in client in the downloaded-JSON shape.
    monkeypatch.setattr(_oauth, "BUILTIN_CLIENT_PATH", flow.client_secrets)
    assert flow.sign_in_with(None) == EMAIL
    assert flow.stored()["client_id"] == CLIENT_ID

    _script_sign_in(flow, monkeypatch)
    flow.token_path.unlink()
    result = CliRunner().invoke(
        cli, ["google-drive", "sign-in", "--token-path", str(flow.token_path)]
    )
    assert result.exit_code == 0, result.output
    assert f"Signed in as {EMAIL}." in result.output
    assert flow.account().email == EMAIL


def test_a_client_file_wins_over_the_built_in_client(
    flow: Flow, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # A built-in client the fake Google doesn't know: using it would fail the consent.
    other = tmp_path / "builtin.json"
    other.write_text(
        json.dumps({"installed": {"client_id": "other", "client_secret": "other"}})
    )
    monkeypatch.setattr(_oauth, "BUILTIN_CLIENT_PATH", other)
    assert flow.sign_in() == EMAIL
    assert flow.stored()["client_id"] == CLIENT_ID


def test_a_build_without_the_built_in_client_asks_for_a_client_file(
    flow: Flow, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    from cocoindex.cli import cli

    monkeypatch.setattr(_oauth, "BUILTIN_CLIENT_PATH", tmp_path / "absent.json")
    with pytest.raises(SignInError, match="no built-in OAuth client"):
        flow.sign_in_with(None)
    assert not flow.opened and not flow.token_path.exists()

    result = CliRunner().invoke(
        cli, ["google-drive", "sign-in", "--token-path", str(flow.token_path)]
    )
    assert result.exit_code == 1
    assert "no built-in OAuth client" in result.output
    assert _oauth.OWN_CLIENT_DOCS in result.output


def test_a_refused_built_in_client_says_to_upgrade_or_bring_your_own(
    flow: Flow, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_oauth, "BUILTIN_CLIENT_PATH", flow.client_secrets)
    flow.google.client_refused = True
    with pytest.raises(SignInError, match="built-in OAuth client: invalid_client"):
        flow.sign_in_with(None)
    assert not flow.token_path.exists()
