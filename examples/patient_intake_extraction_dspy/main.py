from __future__ import annotations

import pathlib

import dspy
from dotenv import load_dotenv
import pymupdf

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

from models import Patient


# DSPy Signature for patient information extraction from images
class PatientExtractionSignature(dspy.Signature):
    """Extract structured patient information from a medical intake form image."""

    form_images: list[dspy.Image] = dspy.InputField(
        desc="Images of the patient intake form pages"
    )
    patient: Patient = dspy.OutputField(
        desc="Extracted patient information with all available fields filled"
    )


class PatientExtractor(dspy.Module):
    """DSPy module for extracting patient information from intake form images."""

    def __init__(self) -> None:
        super().__init__()
        self.extract = dspy.ChainOfThought(PatientExtractionSignature)

    def forward(self, form_images: list[dspy.Image]) -> Patient:
        """Extract patient information from form images and return as a Pydantic model."""
        result = self.extract(form_images=form_images)
        return result.patient


@coco.function
def extract_patient(pdf_content: bytes) -> Patient:
    """Extract patient information from PDF content."""
    pdf_doc = pymupdf.open(stream=pdf_content, filetype="pdf")

    form_images = []
    for page in pdf_doc:
        pix = page.get_pixmap(matrix=pymupdf.Matrix(2, 2))
        img_bytes = pix.tobytes("png")
        form_images.append(dspy.Image(img_bytes))

    pdf_doc.close()

    extractor = PatientExtractor()
    patient = extractor(form_images=form_images)
    return patient


@coco.function(memo=True)
def process_patient_form(file: FileLike, outdir: pathlib.Path) -> None:
    """Process a patient intake form PDF and extract structured information."""
    content = file.read()
    patient_info = extract_patient(content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.file_path.path.stem + ".json"
    localfs.declare_file(
        outdir / output_filename, patient_json, create_parent_dirs=True
    )


@coco.function
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    """Main application function that processes patient intake forms."""
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await coco_aio.mount_each(process_patient_form, files.items(), outdir)


load_dotenv()
lm = dspy.LM("gemini/gemini-2.5-flash")
dspy.configure(lm=lm)

app = coco_aio.App(
    coco_aio.AppConfig(name="PatientIntakeExtractionDSPy"),
    app_main,
    sourcedir=pathlib.Path("./data/patient_forms"),
    outdir=pathlib.Path("./output_patients"),
)
