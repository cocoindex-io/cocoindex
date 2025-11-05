import os
import base64
from baml_client import b
from baml_client.types import Patient
import baml_py
import cocoindex


@cocoindex.op.function(cache=True, behavior_version=1)
async def extract_patient_info(content: bytes) -> Patient:
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)


@cocoindex.flow_def(name="PatientIntakeExtractionBaml")
def patient_intake_extraction_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that extracts patient information from intake forms using BAML.

    This flow:
    1. Reads patient intake documents (PDF, DOCX, etc.) as binary
    2. Directly extracts structured patient information using BAML's native PDF/Image support
    3. Stores the results in a Postgres database
    """
    # Load documents from local file source (binary mode)
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path=os.path.join("data", "patient_forms"), binary=True
        )
    )

    # Create collector for patient data
    patients_index = data_scope.add_collector()

    # Process each document
    with data_scope["documents"].row() as doc:
        # Extract patient information using BAML directly from file bytes
        # BAML natively supports PDF and Image inputs
        doc["patient_info"] = doc["content"].transform(extract_patient_info)

        # Collect the extracted patient information
        patients_index.collect(
            filename=doc["filename"],
            patient_info=doc["patient_info"],
        )

    # Export to Postgres
    patients_index.export(
        "patients",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename"],
    )
