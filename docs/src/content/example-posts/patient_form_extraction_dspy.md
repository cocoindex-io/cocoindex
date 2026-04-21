---
title: Extracting Intake Forms with DSPy and CocoIndex
description: 'How to use DSPy together with CocoIndex to build a data pipeline that extracts structured patient information from PDF intake forms using vision models.'
slug: patient_form_extraction_dspy
image: https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/cover.png
tags: [structured-data-extraction, custom-building-blocks, vision-models]
last_reviewed: 2026-01-18
---

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction_dspy)

![Patient Form Extraction with DSPy](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/cover.png)

## Overview

This tutorial shows how to use DSPy together with CocoIndex to build a data pipeline that extracts structured patient information from PDF intake forms using vision models. DSPy provides a programming framework for LLMs with typed Signatures and Modules, while CocoIndex orchestrates file input, transformation, and incremental indexing.

:::info
The extraction quality is highly dependent on the OCR quality. You can use CocoIndex with any commercial parser or open source ones that is tailored for your domain for better results. For example, Document AI from Google Cloud and more.
:::

## Why DSPy + CocoIndex?

### DSPy: A programming framework for LLMs

Traditional LLM apps rely on prompt engineering: you write a prompt with instructions, few‑shot examples, and formatting, then call the model and parse the raw text. This approach is fragile:

- Small changes in the prompt, model, or data can break the output format or quality.
- Logic is buried in strings, making it hard to test, compose, or version.

[DSPy](https://github.com/stanfordnlp/dspy) replaces this with a programming model: you define what each LLM step should do (inputs, outputs, constraints), and the framework figures out how to prompt the model to satisfy that spec.

### CocoIndex: An ultra performant data processing engine for AI workloads

[CocoIndex](https://github.com/cocoindex-io/cocoindex) is an ultra performant compute framework for AI workloads, with incremental processing. Users write simple in-memory computations in Python and coco runs it as a resilient, scalable data pipeline (with Rust Engine) – with fresh data always ready for serving. Same flow definition you use in a notebook can be lifted easily into production.

With CocoIndex, changes in sources or transformation logic only trigger minimal recompute, cutting cold-start "backfill" latencies from hours to seconds while reducing GPU/API spend. In production, this manifests as always-fresh targets: you run in "live" mode with change data capture or polling, and CocoIndex keeps derived stores in sync with complex unstructured sources like codebases, PDFs, and multi-hop API compositions.

Because every transformation step is observable with lineage, teams get auditability and explainability out of the box, which helps for regulated scenarios like healthcare extraction or financial workflows.

### DSPy & CocoIndex Synergy

The synergy shows up most clearly in end-to-end AI data products: DSPy defines robust, typed extractors or decision modules, and CocoIndex wires them into a resilient, incremental pipeline that can meet SLOs and compliance needs. Any change in documents, code, or business rules is reflected quickly and explainably in the targets and features those agents consume.

## Flow Overview

![Flow overview](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/flow.png)

The flow itself is fairly simple.

1. Read PDF files from a directory.
2. For each file, convert PDF pages to images and use DSPy with Gemini Vision to extract structured `Patient` data.
3. Collect results and export to Postgres.

## Setup

1. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
2. Install dependencies

    ```
    pip install -U cocoindex dspy-ai pydantic pymupdf
    ```

3. Create a `.env` file:

    ```
    # Postgres database address for cocoindex
    COCOINDEX_DATABASE_URL=postgres://cocoindex:cocoindex@localhost/cocoindex

    # Gemini API key
    GEMINI_API_KEY=YOUR_GEMINI_API_KEY
    ```

## Pydantic Models: Define the structured schema

We defined Pydantic-style classes (`Contact`, `Address`, `Insurance`, etc.) to match a *FHIR-inspired patient schema*, enabling structured and validated representations of patient data. Each model corresponds to a key aspect of a patient's record, ensuring both type safety and nested relationships.

![Patient schema](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/schema.png)

### 1. Contact Model

```python
class Contact(BaseModel):
    name: str
    phone: str
    relationship: str
```

- Represents an **emergency or personal contact** for the patient.
- Fields:
    - `name`: Contact's full name.
    - `phone`: Contact phone number.
    - `relationship`: Relation to the patient (e.g., parent, spouse, friend).

### 2. Address Model

```python
class Address(BaseModel):
    street: str
    city: str
    state: str
    zip_code: str
```

- Represents a **postal address**.
- Fields:
    - `street`, `city`, `state`, `zip_code`: Standard address fields.

### 3. Pharmacy Model

```python
class Pharmacy(BaseModel):
    name: str
    phone: str
    address: Address
```

- Represents the **patient's preferred pharmacy**.
- Fields:
    - `name`: Pharmacy name.
    - `phone`: Pharmacy contact number.
    - `address`: Uses the `Address` model for structured address information.

### 4. Insurance Model

```python
class Insurance(BaseModel):
    provider: str
    policy_number: str
    group_number: str | None = None
    policyholder_name: str
    relationship_to_patient: str
```

- Represents the patient's **insurance information**.
- Fields:
    - `provider`: Insurance company name.
    - `policy_number`: Unique policy number.
    - `group_number`: Optional group number.
    - `policyholder_name`: Name of the person covered under the insurance.
    - `relationship_to_patient`: Relationship to patient (e.g., self, parent).

### 5. Condition Model

```python
class Condition(BaseModel):
    name: str
    diagnosed: bool
```

- Represents a **medical condition**.
- Fields:
    - `name`: Condition name (e.g., Diabetes).
    - `diagnosed`: Boolean indicating whether it has been officially diagnosed.

### 6. Medication Model

```python
class Medication(BaseModel):
    name: str
    dosage: str
```

- Represents a **current medication** the patient is taking.
- Fields:
    - `name`: Medication name.
    - `dosage`: Dosage information (e.g., "10mg daily").

### 7. Allergy Model

```python
class Allergy(BaseModel):
    name: str
```

- Represents a **known allergy**.
- Fields:
    - `name`: Name of the allergen (e.g., peanuts, penicillin).

### 8. Surgery Model

```python
class Surgery(BaseModel):
    name: str
    date: str
```

- Represents a **surgery or procedure** the patient has undergone.
- Fields:
    - `name`: Surgery name (e.g., Appendectomy).
    - `date`: Surgery date (as a string, ideally ISO format).

### 9. Patient Model

```python
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
```

- Represents a **complete patient record** with personal, medical, and administrative information.
- Key fields:
    - `name`, `dob`, `gender`: Basic personal info.
    - `address`, `phone`, `email`: Contact info.
    - `preferred_contact_method`: How the patient prefers to be reached.
    - `emergency_contact`: Nested `Contact` model.
    - `insurance`: Optional nested `Insurance` model.
    - `reason_for_visit`, `symptoms_duration`: Visit details.
    - `past_conditions`, `current_medications`, `allergies`, `surgeries`: Lists of nested models for comprehensive medical history.
    - `occupation`: Optional job info.
    - `pharmacy`: Optional nested `Pharmacy` model.
    - `consent_given`, `consent_date`: Legal/administrative consent info.

### Why Use Pydantic Here?

1. **Validation:** Ensures all fields are the correct type (e.g., `dob` is a `date`).
2. **Structured Nested Models:** Patient has nested objects like `Address`, `Contact`, and `Insurance`.
3. **Default Values & Optional Fields:** Handles optional fields and defaults (`Field(default_factory=list)` ensures empty lists if no data).
4. **Serialization:** Easily convert models to JSON for APIs or databases.
5. **Error Checking:** Automatically raises errors if invalid data is provided.

## DSPy Vision Extractor

### DSPy Signature

Let's define `PatientExtractionSignature`. A **Signature** describes what data your module expects and what it will produce. Think of it as a **schema for an AI task**.

`PatientExtractionSignature` is a `dspy.Signature`, which is DSPy's way of declaring *what* the model should do, not *how* it does it.

```python
# DSPy Signature for patient information extraction from images
class PatientExtractionSignature(dspy.Signature):
    """Extract structured patient information from a medical intake form image."""

    form_images: list[dspy.Image] = dspy.InputField(
        desc="Images of the patient intake form pages"
    )
    patient: Patient = dspy.OutputField(
        desc="Extracted patient information with all available fields filled"
    )
```

This signature defines **task contract** for patient information extraction.

- Inputs: `form_images` – a list of images of the intake form.
- Outputs: `patient` – a structured `Patient` object.

From DSPy's point of view, this Signature is a "spec": a mapping from an image-based context to a structured, Pydantic-backed semantic object that can later be optimized, trained, and composed with other modules.

### PatientExtractor Module

`PatientExtractor` is a `dspy.Module`, which in DSPy is a composable, potentially trainable building block that implements the Signature.

```python
class PatientExtractor(dspy.Module):
    """DSPy module for extracting patient information from intake form images."""

    def __init__(self) -> None:
        super().__init__()
        self.extract = dspy.ChainOfThought(PatientExtractionSignature)

    def forward(self, form_images: list[dspy.Image]) -> Patient:
        """Extract patient information from form images and return as a Pydantic model."""
        result = self.extract(form_images=form_images)
        return result.patient  # type: ignore
```

- In `__init__`, `ChainOfThought` is a DSPy *primitive module* that knows how to call an LLM with reasoning-style prompting to satisfy the given Signature. In other words, it is a default "strategy" for solving the "extract patient from images" task.
- The `forward` method is DSPy's standard interface for executing a module. You pass `form_images` into `self.extract()`. DSPy then handles converting this call into an LLM interaction (or a trained program) that produces a `patient` field as declared in the Signature.

Conceptually, `PatientExtractor` is an *ETL operator*: the Signature describes the input/output types, and the internal `ChainOfThought` module is the function that fills that contract.

### Single-Step Extraction

Now let's wire the DSPy Module to extract from a single PDF. From high level,

- The extractor receives PDF bytes directly
- Internally converts PDF pages to DSPy Image objects using PyMuPDF
- Processes images with vision model
- Returns Pydantic model directly

```python
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
```

This function is a **CocoIndex function** (decorated with `@cocoindex.op.function`) that takes **PDF bytes** as input and returns a fully structured `Patient` Pydantic object.

- `cache=True` allows repeated calls with the same PDF to **reuse results**.
- `behavior_version=1` ensures versioning of the function for reproducibility.

### Create DSPy Image objects

We open PDF from bytes using **PyMuPDF** (`pymupdf`), then we iterate over each page.

- Useful trick: Render the page as a **high-resolution image** (`2x`) for better OCR/vision performance.
- Convert the rendered page to **PNG bytes**.
- Wrap the PNG bytes in a **DSPy `Image` object**.

### DSPy Extraction

The list of `form_images` is passed to the DSPy module:

1. **ChainOfThought reasoning** interprets each image.
2. **Vision + NLP** extract relevant text fields.
3. **Populate Pydantic `Patient` object** with structured patient info.

## CocoIndex Flow

![CocoIndex Flow](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/flow.png)

- Loads PDFs from local directory as binary
- For each document, applies single transform: PDF bytes → Patient data
- Exports the results in a PostgreSQL table

### Declare Flow

Declare a CocoIndex flow, connect to the source, add a data collector to collect processed data.

```python
@cocoindex.flow_def(name="PatientIntakeExtractionDSPy")
def patient_intake_extraction_dspy_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data/patient_forms", binary=True)
    )

    patients_index = data_scope.add_collector()
```

- `@cocoindex.flow_def` tells CocoIndex that this function is a flow definition, not regular runtime code.
- `add_source()` registers a `LocalFile` source that traverses `data/patient_forms` directory and creates a logical table named `documents`

![Ingesting Data](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/ingest.png)

You can connect to various sources, or even custom source with CocoIndex if native connectors are not available. CocoIndex is designed to keep your indexes synchronized with your data sources. This is achieved through a feature called **live updates**, which automatically detects changes in your sources and updates your indexes accordingly. This ensures that your search results and data analysis are always based on the most current information. You can read more here [https://cocoindex.io/docs/tutorials/live_updates](https://cocoindex.io/docs/tutorials/live_updates)

### Process documents

```python
with data_scope["documents"].row() as doc:
    # Extract patient information directly from PDF using DSPy with vision
    # (PDF->Image conversion happens inside the extractor)
    doc["patient_info"] = doc["content"].transform(extract_patient)

    # Collect the extracted patient information
    patients_index.collect(
        filename=doc["filename"],
        patient_info=doc["patient_info"],
    )
```

This iterates over each document. We transform `doc["content"]` (the bytes) by our `extract_patient` function. The result is stored in a new field `patient_info`.

Then we collect a row with the `filename` and extracted `patient_info`.

![Transforming Data](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/transform.png)

![Nested Data](https://cocoindex.io/blobs/docs/img/examples/patient_form_extraction_dspy/nested.png)

### Export to Postgres

```python
patients_index.export(
    "patients",
    cocoindex.storages.Postgres(table_name="patients_info_dspy"),
    primary_key_fields=["filename"],
)
```

We export the collected index to Postgres. This will create/maintain a table `patients` keyed by filename, automatically deleting or updating rows if inputs change.

Because CocoIndex tracks data lineage, it will handle updates/deletions of source files incrementally.

### Configure CocoIndex settings

Define a **CocoIndex settings function** that configures the AI model for DSPy:

```python
@cocoindex.settings
def cocoindex_settings() -> cocoindex.Settings:
    # Configure the model used in DSPy
    lm = dspy.LM("gemini/gemini-2.5-flash")
    dspy.configure(lm=lm)

    return cocoindex.Settings.from_env()
```

It returns a `cocoindex.Settings` object initialized from environment variables, enabling the system to use the configured model and environment settings for all DSPy operations.

## Running the Pipeline

Update the index:

```bash
cocoindex update main
```

### CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with zero pipeline data retention.

```bash
cocoindex server -ci main
```

## Scalable Open ecosystem, not a closed box

CocoIndex is intentionally "composable by default": it gives you a fast, incremental data engine and clean flow, abstraction, but never locks you into a specific model, vector DB, processing module or orchestration stack.

CocoIndex treats everything — sources, ops, and storages — as pluggable interfaces rather than proprietary primitives. You can read from local files, S3, APIs, or custom sources, call any data transformation logic (beyond SQL, DSPy modules, any complex Python transformations, generated parsers etc), and export to relational databases, vector databases, search engines, or custom sinks through its storage layer.

### Why DSPy + CocoIndex fits this philosophy

DSPy is itself a compositional framework for LLMs: you define typed Signatures and Modules, and it learns how to implement them, making the LLM layer programmable, testable, and optimizable.

CocoIndex treats these modules as first-class operators in the flow, so you get a clean separation of concerns: DSPy owns "how the model thinks," while CocoIndex owns "how data moves, is cached, and is served" across changing PDFs, code, or APIs.

This pairing is powerful because neither system tries to be the entire stack: CocoIndex does not prescribe a prompt framework, and DSPy does not prescribe a data pipeline engine. Instead, they interlock: DSPy modules become composable building blocks inside CocoIndex flows, and CocoIndex gives those modules a production context with retries, batching, caching, and live updates.

## Connect to other sources

CocoIndex natively supports Google Drive, Amazon S3, Azure Blob Storage, and more.

[→ Sources](https://cocoindex.io/docs/sources)
