from __future__ import annotations

import datetime
import pathlib
from dotenv import load_dotenv
import dspy
from pydantic import BaseModel, Field
import pymupdf

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


# Pydantic models for DSPy structured outputs
class Contact(BaseModel):
    name: str
    phone: str
    relationship: str


class Address(BaseModel):
    street: str
    city: str
    state: str
    zip_code: str


class Pharmacy(BaseModel):
    name: str
    phone: str
    address: Address


class Insurance(BaseModel):
    provider: str
    policy_number: str
    group_number: str | None = None
    policyholder_name: str
    relationship_to_patient: str


class Condition(BaseModel):
    name: str
    diagnosed: bool


class Medication(BaseModel):
    name: str
    dosage: str


class Allergy(BaseModel):
    name: str


class Surgery(BaseModel):
    name: str
    date: str


class Patient(BaseModel):
    name: str
    dob: datetime.date
    gender: str
    address: Address
    phone: str
    email: str
    preferred_contact_method: str
    emergency_contact: Contact
    insurance: Insurance | None = None
    reason_for_visit: str
    symptoms_duration: str
    past_conditions: list[Condition] = Field(default_factory=list)
    current_medications: list[Medication] = Field(default_factory=list)
    allergies: list[Allergy] = Field(default_factory=list)
    surgeries: list[Surgery] = Field(default_factory=list)
    occupation: str | None = None
    pharmacy: Pharmacy | None = None
    consent_given: bool
    consent_date: str | None = None


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
def process_patient_form(file: FileLike, target: localfs.DirTarget) -> None:
    """Process a patient intake form PDF and extract structured information."""
    content = file.read()
    patient_info = extract_patient(content)
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
lm = dspy.LM("gemini/gemini-2.5-flash")
dspy.configure(lm=lm)

app = coco.App(
    coco.AppConfig(name="PatientIntakeExtractionDSPy"),
    app_main,
    sourcedir=pathlib.Path("./data/patient_forms"),
    outdir=pathlib.Path("./output_patients"),
)
