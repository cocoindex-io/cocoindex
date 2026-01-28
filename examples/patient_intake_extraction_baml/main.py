import base64
import pathlib

from dotenv import load_dotenv

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from baml_client import b
from baml_client.types import Patient
import baml_py


@coco.function
async def extract_patient_info(content: bytes) -> Patient:
    """Extract patient information from PDF content using BAML."""
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)


@coco.function(memo=True)
async def process_patient_form(file: FileLike, target: localfs.DirTarget) -> None:
    """Process a patient intake form PDF and extract structured information."""
    content = file.read()
    patient_info = await extract_patient_info(content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.relative_path.stem + ".json"
    target.declare_file(filename=output_filename, content=patient_json)


@coco.function
def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    """Main application function that processes patient intake forms."""
    target = coco.mount_run(
        coco.component_subpath("setup"), localfs.declare_dir_target, outdir
    ).result()

    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )

    for f in files:
        coco.mount(
            coco.component_subpath("process", str(f.relative_path)),
            process_patient_form,
            f,
            target,
        )


load_dotenv()

app = coco.App(
    coco.AppConfig(name="PatientIntakeExtractionBaml"),
    app_main,
    sourcedir=pathlib.Path("./data/patient_forms"),
    outdir=pathlib.Path("./output_patients"),
)
