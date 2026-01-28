import pathlib
from datetime import datetime, timezone
from typing import Iterator

import cocoindex as coco
from cocoindex.connectors import localfs


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function(memo=True)
def write_timestamp(scope: coco.Scope, target: localfs.DirTarget) -> None:
    # If memoization hits, this function won't re-run and the file won't change.
    now = datetime.now(timezone.utc).isoformat()
    target.declare_file(scope, filename="stamp.txt", content=now)


@coco.function
def app_main(scope: coco.Scope) -> None:
    target = coco.mount_run(
        localfs.declare_dir_target, scope / "setup", pathlib.Path("./out_memo")
    ).result()
    coco.mount(write_timestamp, scope / "write", target)


app = coco.App(
    app_main,
    coco.AppConfig(name="MemoApp"),
)
