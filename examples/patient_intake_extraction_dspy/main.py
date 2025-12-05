import datetime

import dspy
from pydantic import BaseModel, Field
import pymupdf

import cocoindex


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
        return result.patient  # type: ignore


@cocoindex.op.function(cache=True, behavior_version=1)
def extract_patient(pdf_content: bytes) -> Patient:
    """Extract patient information from PDF content."""

    # Convert PDF pages to DSPy Image objects
    pdf_doc = pymupdf.open(stream=pdf_content, filetype="pdf")

    form_images = []
    for page in pdf_doc:
        # Render page to pixmap (image) at 2x resolution for better quality
        pix = page.get_pixmap(matrix=pymupdf.Matrix(2, 2))
        # Convert to PNG bytes
        img_bytes = pix.tobytes("png")
        # Create DSPy Image from bytes
        form_images.append(dspy.Image(img_bytes))

    pdf_doc.close()

    # Extract patient information using DSPy with vision
    extractor = PatientExtractor()
    patient = extractor(form_images=form_images)

    return patient  # type: ignore


@cocoindex.flow_def(name="PatientIntakeExtractionDSPy")
def patient_intake_extraction_dspy_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that extracts patient information from intake forms using DSPy.

    This flow:
    1. Reads patient intake PDFs as binary
    2. Uses DSPy with vision models to extract structured patient information
       (PDF to image conversion happens automatically inside the extractor)
    3. Stores the results in a Postgres database
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data/patient_forms", binary=True)
    )

    patients_index = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        # Extract patient information directly from PDF using DSPy with vision
        # (PDF->Image conversion happens inside the extractor)
        doc["patient_info"] = doc["content"].transform(extract_patient)

        # Collect the extracted patient information
        patients_index.collect(
            filename=doc["filename"],
            patient_info=doc["patient_info"],
        )

    # Export to Postgres
    patients_index.export(
        "patients",
        cocoindex.storages.Postgres(table_name="patients_info_dspy"),
        primary_key_fields=["filename"],
    )


@cocoindex.settings
def cocoindex_settings() -> cocoindex.Settings:
    # Configure the model used in DSPy
    lm = dspy.LM("gemini/gemini-2.5-flash")
    dspy.configure(lm=lm)

    return cocoindex.Settings.from_env()
