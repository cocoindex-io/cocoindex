import base64
import pathlib
from typing import Iterator

from dotenv import load_dotenv

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from baml_client import b
from baml_client.types import Patient
import baml_py


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function
async def extract_patient_info(scope: coco.Scope, content: bytes) -> Patient:
    """Extract patient information from PDF content using BAML."""
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)


@coco.function(memo=True)
async def process_patient_form(
    scope: coco.Scope, file: FileLike, target: localfs.DirTarget
) -> None:
    """Process a patient intake form PDF and extract structured information."""
    content = file.read()
    patient_info = await extract_patient_info(scope, content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.relative_path.stem + ".json"
    target.declare_file(scope, filename=output_filename, content=patient_json)


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    """Main application function that processes patient intake forms."""
    target = coco.mount_run(localfs.dir_target, scope / "setup", outdir).result()

    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )

    for f in files:
        coco.mount(
            process_patient_form,
            scope / "process" / str(f.relative_path),
            f,
            target,
        )


app = coco.App(
    app_main,
    coco.AppConfig(name="PatientIntakeExtractionBaml"),
    sourcedir=pathlib.Path("./data/patient_forms"),
    outdir=pathlib.Path("./output_patients"),
)


def main() -> None:
    load_dotenv()
    app.run()


if __name__ == "__main__":
    main()
