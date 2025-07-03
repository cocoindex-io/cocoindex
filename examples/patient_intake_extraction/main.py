import datetime
import tempfile
import dataclasses
import os

from markitdown import MarkItDown
from openai import OpenAI

import cocoindex


@dataclasses.dataclass
class Contact:
    name: str
    phone: str
    relationship: str


@dataclasses.dataclass
class Address:
    street: str
    city: str
    state: str
    zip_code: str


@dataclasses.dataclass
class Pharmacy:
    name: str
    phone: str
    address: Address


@dataclasses.dataclass
class Insurance:
    provider: str
    policy_number: str
    group_number: str | None
    policyholder_name: str
    relationship_to_patient: str


@dataclasses.dataclass
class Condition:
    name: str
    diagnosed: bool


@dataclasses.dataclass
class Medication:
    name: str
    dosage: str


@dataclasses.dataclass
class Allergy:
    name: str


@dataclasses.dataclass
class Surgery:
    name: str
    date: str


@dataclasses.dataclass
class Patient:
    name: str
    dob: datetime.date
    gender: str
    address: Address
    phone: str
    email: str
    preferred_contact_method: str
    emergency_contact: Contact
    insurance: Insurance | None
    reason_for_visit: str
    symptoms_duration: str
    past_conditions: list[Condition]
    current_medications: list[Medication]
    allergies: list[Allergy]
    surgeries: list[Surgery]
    occupation: str | None
    pharmacy: Pharmacy | None
    consent_given: bool
    consent_date: datetime.date | None


class ToMarkdown(cocoindex.op.FunctionSpec):
    """Convert a document to markdown."""


@cocoindex.op.executor_class(gpu=True, cache=True, behavior_version=1)
class ToMarkdownExecutor:
    """Executor for ToMarkdown."""

    spec: ToMarkdown
    _converter: MarkItDown

    def prepare(self):
        client = OpenAI()
        self._converter = MarkItDown(llm_client=client, llm_model="gpt-4o")

    def __call__(self, content: bytes, filename: str) -> str:
        suffix = os.path.splitext(filename)[1]
        with tempfile.NamedTemporaryFile(delete=True, suffix=suffix) as temp_file:
            temp_file.write(content)
            temp_file.flush()
            text = self._converter.convert(temp_file.name).text_content
            return text


@cocoindex.flow_def(name="PatientIntakeExtraction")
def patient_intake_extraction_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
):
    """
    Define a flow that extracts patient information from intake forms.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data/patient_forms", binary=True)
    )

    patients_index = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["markdown"] = doc["content"].transform(
            ToMarkdown(), filename=doc["filename"]
        )
        doc["patient_info"] = doc["markdown"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"
                ),
                output_type=Patient,
                instruction="Please extract patient information from the intake form.",
            )
        )
        patients_index.collect(
            filename=doc["filename"],
            patient_info=doc["patient_info"],
        )

    patients_index.export(
        "patients",
        cocoindex.storages.Postgres(table_name="patients_info"),
        primary_key_fields=["filename"],
    )
