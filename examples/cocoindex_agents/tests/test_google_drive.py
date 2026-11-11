"""A Google Drive folder as a source, with the local folder's contract (DESIGN.md §3, §7).

Against a fake Drive (``fake_drive.py``) and a token file under ``tmp_path``:
no network, no account. The orchestrator opens the folder by link; workers
read Google-native files as their export and a PDF as a document; an
unchanged rerun makes no model call and runs no tool; one edited Doc re-runs
its worker's content turns only; a reader session is skipped while the file
it read is unchanged, which Drive's ``modifiedTime`` settles without a
download; a different signed-in account, or none, re-runs what was read.
The model never sees the account: the card names the folder, and the
transcript carries no address or token.
"""

from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass, field

import pytest

import cocoindex as coco
from cocoindex.connectors import google_drive
from cocoindex.connectors.google_drive import _oauth, _source

from cocoindex_agents import (
    AGENTS_CONFIG,
    MODEL_CLIENT,
    AgentsConfig,
    RoleSpec,
    SessionResult,
    run_open_session,
)
from cocoindex_agents.hints import RunHints
from cocoindex_agents.spec import SessionSpec
from cocoindex_agents.testing import CountingRegistry, ScriptedModel
from cocoindex_agents.tools import agents, drive, sources, targets
from cocoindex_agents.transcript import (
    ModelOutput,
    ToolCall,
    Transcript,
    canonical_json,
)
from fake_drive import FakeDrive

ORCHESTRATOR = RoleSpec(
    "orchestrator", "test", ("open_source", "open_target", "for_each", "call_agent")
)
WORKER = RoleSpec("worker", "test", ("read_input", "declare"))
READER = RoleSpec("reader", "test", ("call",))

EMAIL = "someone@example.com"
LINK = "https://drive.google.com/drive/u/0/folders/{folder}?usp=sharing"
PDF = b"%PDF-1.4\n% a scanned receipt\n%%EOF\n"


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


def sign_in(token_path: pathlib.Path, email: str = EMAIL) -> None:
    """What a completed sign-in leaves behind, without the flow."""
    _oauth._save_tokens(
        token_path,
        _oauth._Tokens(
            client_id="client",
            client_secret="secret-value",
            refresh_token="refresh-value",
            access_token="access-value",
            expires_at=4_102_444_800.0,
            email=email,
        ),
    )


@dataclass
class Scenario:
    """Shape A over the folder (a summary per file), or one reader over it."""

    folder: str
    out: pathlib.Path
    reader_key: str | None = None  # a reader session reads this one file
    transcripts: dict[str, str] = field(default_factory=dict)

    def script(
        self, spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "orchestrator":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call(
                            "open_source",
                            kind="google_drive",
                            name="drive",
                            path=LINK.format(folder=self.folder),
                        ),
                        call(
                            "open_target",
                            kind="localfs_dir",
                            name="out",
                            path=str(self.out),
                        ),
                    )
                )
            if n == 1 and self.reader_key is not None:
                return ModelOutput(
                    calls=(
                        call(
                            "call_agent",
                            key="reader",
                            task="look",
                            role="reader",
                            inputs=["src:drive"],
                        ),
                    )
                )
            if n == 1:
                return ModelOutput(
                    calls=(
                        call(
                            "for_each",
                            source="src:drive",
                            key="items",
                            task="summarize",
                            inputs=["tgt:out"],
                        ),
                    )
                )
            self.transcripts["orchestrator"] = canonical_json(
                [[r.value for r in s.results] for s in prefix.steps]
            )
            return ModelOutput(answer=canonical_json(prefix.steps[1].results[0].value))
        if spec.role.name == "reader":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call(
                            "call",
                            handle="src:drive",
                            method="read",
                            args={"key": self.reader_key},
                        ),
                    )
                )
            self.transcripts["reader"] = canonical_json(
                [[r.value for r in s.results] for s in prefix.steps]
            )
            return ModelOutput(answer=canonical_json(prefix.steps[0].results[0].value))
        # worker: read, declare what it got, answer
        if n == 0:
            return ModelOutput(calls=(call("read_input"),))
        item = prefix.steps[0].results[0].value
        if n == 1:
            return ModelOutput(
                calls=(
                    call(
                        "declare",
                        target="tgt:out",
                        key=item["key"].replace("/", "__") + ".summary.md",
                        value=canonical_json(item),
                    ),
                )
            )
        self.transcripts[item["key"]] = canonical_json(
            [[r.value for r in s.results] for s in prefix.steps]
        )
        return ModelOutput(answer=item["key"])


@dataclass
class Fixture:
    drive: FakeDrive
    folder: str
    files: dict[str, str]  # name -> id
    token_path: pathlib.Path
    scenario: Scenario
    model: ScriptedModel
    tools: CountingRegistry
    app: coco.App[..., SessionResult]

    def run(self) -> SessionResult:
        self.model.reset()
        self.tools.reset()
        self.drive.downloads = 0
        result = self.app.update_blocking()
        assert isinstance(result, SessionResult)
        return result

    def executed(self) -> dict[str, int]:
        return dict(self.tools.executed)

    def output(self, name: str) -> dict[str, object]:
        return json.loads((self.scenario.out / name).read_text())  # type: ignore[no-any-return]


@pytest.fixture
def fixture(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> Fixture:
    return make_fixture(coco_env, tmp_path, monkeypatch)


def make_fixture(
    coco_env: coco.Environment,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    reader_key: str | None = None,
) -> Fixture:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    token_path = google_drive.UserAccount().token_path
    sign_in(token_path)

    fake = FakeDrive()
    folder = fake.folder("Team notes")
    sub = fake.folder("archive", folder)
    files = {
        "Meeting notes": fake.doc(folder, "Meeting notes", "# Agenda\n\nShip it.\n"),
        "Budget": fake.sheet(folder, "Budget", "item,cost\nlaptops,1200\n"),
        "receipt.pdf": fake.put(folder, "receipt.pdf", PDF, "application/pdf"),
        "archive/old.txt": fake.put(sub, "old.txt", b"an old note"),
    }

    def build_service(spec: google_drive.GoogleDriveSourceSpec) -> object:
        # The source is built on the signed-in account, never on anything the model named.
        assert isinstance(spec.user_account, google_drive.UserAccount)
        assert spec.user_account.token_path == token_path
        return fake.service()

    monkeypatch.setattr(_source, "_build_service", build_service)

    scenario = Scenario(folder, tmp_path / "out", reader_key=reader_key)
    model = ScriptedModel(scenario.script)
    tools = CountingRegistry(
        [
            sources.open_source,
            sources.call_method,
            sources.read_input,
            targets.open_target,
            targets.declare,
            agents.for_each,
            agents.call_agent,
        ]
    )
    config = AgentsConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, READER)}, tools=tools
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(AGENTS_CONFIG, config)
    app = coco.App(
        coco.AppConfig(name="drive", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "summarize the folder"),
    )
    return Fixture(fake, folder, files, token_path, scenario, model, tools, app)


def test_folder_ids_come_from_links_or_ids() -> None:
    folder = "1AbC_dEf-9"
    for text in (
        folder,
        f"https://drive.google.com/drive/folders/{folder}",
        f"https://drive.google.com/drive/u/0/folders/{folder}?usp=sharing",
        f" https://drive.google.com/drive/folders/{folder}#x ",
    ):
        assert drive.folder_id_of(text) == folder
    with pytest.raises(Exception, match="not a Google Drive folder"):
        drive.folder_id_of("https://docs.google.com/document/d/abc/edit")


def test_a_summary_per_drive_file_and_only_an_edited_docs_worker_reruns(
    fixture: Fixture,
) -> None:
    f = fixture
    first = f.run()
    assert f.model.calls_for("orchestrator") == 3
    assert f.model.calls_for("worker") == 12  # four files, three turns each
    assert '"handle":"coll:items"' in first.answer and '"failed":0' in first.answer

    # The card names the folder, not the account; nothing of the sign-in is
    # in any transcript.
    orchestrator = f.scenario.transcripts["orchestrator"]
    assert f.folder in orchestrator and "patterns" in orchestrator
    for secret in (EMAIL, "access-value", "refresh-value", "secret-value"):
        assert secret not in "".join(f.scenario.transcripts.values())

    # Google-native files are read as their export, a PDF as a document, a
    # binary text file as its text; keys are paths under the folder.
    assert f.output("Meeting notes.summary.md") == {
        "key": "Meeting notes",
        "text": "# Agenda\n\nShip it.\n",
    }
    assert f.output("Budget.summary.md")["text"] == "item,cost\nlaptops,1200\n"
    receipt = f.output("receipt.pdf.summary.md")
    assert receipt["media_type"] == "application/pdf" and receipt["bytes"] == len(PDF)
    assert f.output("archive__old.txt.summary.md")["text"] == "an old note"

    # Unchanged: skipped whole, no tool runs, nothing downloaded.
    unchanged = f.run()
    assert f.model.calls == 0 and f.executed() == {}
    assert f.drive.downloads == 0
    assert unchanged.answer == first.answer

    # One Doc edited in Drive: the fan-out body re-runs with no inference,
    # and only that worker's turns after read_input run again.
    f.drive.edit(f.files["Meeting notes"], "# Agenda\n\nShip it tomorrow.\n")
    edited = f.run()
    assert f.model.calls_for("orchestrator") == 0
    assert f.model.calls_for("worker") == 2
    assert edited.answer == first.answer
    assert (
        f.output("Meeting notes.summary.md")["text"]
        == "# Agenda\n\nShip it tomorrow.\n"
    )
    # The edited Doc was exported (for its worker) and the others were not.
    assert f.drive.downloads == 1

    # A file added and one removed: one new worker, the removed file's output
    # cleaned up, the others untouched.
    f.drive.put(f.folder, "todo.md", b"- call back")
    f.drive.delete(f.files["archive/old.txt"])
    changed = f.run()
    assert f.model.calls_for("orchestrator") == 0
    assert f.model.calls_for("worker") == 3
    assert f.output("todo.md.summary.md")["text"] == "- call back"
    assert not (f.scenario.out / "archive__old.txt.summary.md").exists()
    # The collection card is unchanged (its count is widened), so the
    # orchestrator's transcript is too: that is what keeps it at zero calls.
    assert changed.answer == first.answer


def test_a_reader_is_skipped_while_its_file_is_unchanged_without_a_download(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = make_fixture(coco_env, tmp_path, monkeypatch, reader_key="Meeting notes")
    f.run()
    assert f.model.calls_for("reader") == 2
    assert f.drive.downloads == 1  # the Doc's export, read once
    assert "Ship it." in f.scenario.transcripts["reader"]

    # Unchanged: the reader's read validates on modifiedTime alone.
    f.run()
    assert f.model.calls == 0 and f.executed() == {} and f.drive.downloads == 0

    # Another file edited, or the folder listed differently: not this reader's business.
    f.drive.edit(f.files["Budget"], "item,cost\nlaptops,1300\n")
    f.drive.put(f.folder, "new.txt", b"new")
    f.run()
    assert f.model.calls == 0 and f.executed() == {} and f.drive.downloads == 0

    # The Doc touched without a content change: re-exported to compare, then still skipped.
    f.drive.touch(f.files["Meeting notes"])
    f.run()
    assert f.model.calls == 0 and f.executed() == {} and f.drive.downloads == 1

    # The Doc edited: the reader re-runs, and only its turn after the read.
    f.drive.edit(f.files["Meeting notes"], "# Agenda\n\nShip it later.\n")
    f.run()
    assert f.model.calls_for("orchestrator") == 0
    assert f.model.calls_for("reader") == 1
    assert "Ship it later." in f.scenario.transcripts["reader"]

    # Renamed away: the path no longer resolves, which the reader is told.
    f.drive.rename(f.files["Meeting notes"], "Agenda")
    f.run()
    assert f.model.calls_for("reader") == 1
    assert "no file 'Meeting notes'" in f.scenario.transcripts["reader"]


def test_another_account_or_no_sign_in_reruns_what_was_read(fixture: Fixture) -> None:
    f = fixture
    f.run()
    f.run()
    assert f.model.calls == 0

    # A different account signed in at the same path: every record is that
    # account's, so the session re-runs (and the source's memo key differs).
    sign_in(f.token_path, "other@example.com")
    f.run()
    assert f.executed()["open_source"] == 1
    assert f.model.calls_for("orchestrator") == 0  # same transcript: turns hit
    assert f.model.calls_for("worker") == 0

    # Signed out: opening the source fails, which an open session contains
    # and reports; the model sees the error class, not a token.
    google_drive.UserAccount().token_path.unlink()
    f.run()
    assert f.executed()["open_source"] == 1
    assert "SignInRequiredError" in f.scenario.transcripts["orchestrator"]
    transcript = "".join(f.scenario.transcripts.values())
    for secret in ("access-value", "refresh-value", "secret-value"):
        assert secret not in transcript

    # Signed in again: heals.
    sign_in(f.token_path, "other@example.com")
    healed = f.run()
    assert '"handle":"coll:items"' in healed.answer and '"failed":0' in healed.answer
    f.run()
    assert f.model.calls == 0 and f.executed() == {}


def test_a_revoked_sign_in_reruns_the_session_instead_of_failing_the_run(
    fixture: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Google refusing the stored sign-in (expired or revoked) surfaces inside the
    Drive walk, in the validators as well as in the body: a validator treats it
    as an invalid record, so the session re-runs and contains it."""
    f = fixture
    first = f.run()
    revoked = True

    def build_service(spec: google_drive.GoogleDriveSourceSpec) -> object:
        if revoked:  # what the first request's token refresh raises
            raise google_drive.SignInRequiredError("Google refused the stored sign-in")
        return f.drive.service()

    monkeypatch.setattr(_source, "_build_service", build_service)
    f.run()  # does not raise: the validators did not fail the run
    assert f.executed()["open_source"] == 1
    assert "SignInRequiredError" in f.scenario.transcripts["orchestrator"]

    revoked = False
    healed = f.run()
    assert healed.answer == first.answer
    f.run()
    assert f.model.calls == 0 and f.executed() == {}
