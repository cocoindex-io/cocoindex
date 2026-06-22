---
title: Patient Intake Forms to Typed JSON with *BAML*
description: 'Extract structured patient records from intake-form PDFs with CocoIndex V1 — define the output schema in BAML, run one type-safe Gemini vision extraction per PDF, and write a validated JSON file per patient. Plain async Python, incremental by default.'
slug: patient-intake-baml
image: https://cocoindex.io/blobs/docs-v1/img/examples/patient-intake-baml/cover.png
tags: [structured-extraction, baml]
---

![Patient Intake Forms to Typed JSON with BAML and CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/patient-intake-baml/cover.png)

We'll take a folder of patient intake forms — the messy, multi-section PDFs a clinic hands you on a clipboard — and turn each one into a clean, validated JSON record: demographics, insurance, medications, allergies, surgeries, consent. The hard part isn't reading the PDF; it's getting back data that *matches a schema* every time, so downstream code can trust it. We use [BAML](https://boundaryml.com/) to declare that schema and run a single type-safe extraction per form against a Gemini vision model.

The whole pipeline is ordinary `async` Python. You write a [BAML schema](https://docs.boundaryml.com/) for the `Patient` type and one extraction function, wrap it in a [CocoIndex function](https://cocoindex.io/docs/programming_guide/function/), and let the Rust engine underneath handle [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/) — only changed PDFs get re-extracted, and the LLM call (the one genuinely expensive step) is skipped entirely for forms you've already processed.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction_baml)

## Flow overview

![CocoIndex patient intake extraction flow: walk a folder of PDF intake forms, run one BAML Gemini-vision extraction per form into a typed Patient model, dump it to JSON, and write one JSON file per form to a local directory](https://cocoindex.io/blobs/docs-v1/img/examples/patient-intake-baml/flow-v1.png)

From a high level, these are the steps:

1. Read PDF intake forms from a local directory.
2. [Extract a typed `Patient`](https://boundaryml.com/) from each PDF with one BAML function call to a Gemini vision model, then serialize it to JSON.
3. Write one JSON file per form to an output directory (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

## Define the schema in BAML

The schema lives in `baml_src/patient.baml`, not in Python. You describe the shape of a patient record as BAML classes — and the same file holds the extraction function and the model client. BAML turns this into a strongly-typed Pydantic client, and at runtime it forces the model's output to conform to the schema (with `"N/A"` filled in for required fields that the form leaves blank).

```baml title="baml_src/patient.baml"
class Patient {
  name string
  dob string
  gender string
  address Address
  phone string
  email string
  preferred_contact_method string
  emergency_contact Contact
  insurance Insurance?
  reason_for_visit string
  symptoms_duration string
  past_conditions Condition[]
  current_medications Medication[]
  allergies Allergy[]
  surgeries Surgery[]
  occupation string?
  pharmacy Pharmacy?
  consent_given bool
  consent_date string?
}

function ExtractPatientInfo(intake_form: pdf) -> Patient {
  client Gemini
  prompt #"
    Extract all patient information from the following intake form document.
    Please be thorough and extract all available information accurately.

    {{ _.role("user") }}
    {{ intake_form }}

    Fill in with "N/A" for required fields if the information is not available.

    {{ ctx.output_format }}
  "#
}
```

The function signature is the contract: `ExtractPatientInfo` takes a `pdf` and returns a `Patient`. `{{ ctx.output_format }}` is where BAML injects the schema into the prompt, and the `Gemini` client (declared in the same file, pointing at `gemini-2.5-flash`) reads PDFs natively as vision input — no separate parse or OCR step. Nested types like `Address`, `Insurance`, and the `Condition[]` / `Medication[]` lists are defined the same way; see the full `patient.baml` in the repo.

Running `baml generate` compiles this into a `baml_client/` package you import from Python — `b.ExtractPatientInfo(...)` and the `Patient` Pydantic model.

## Wrap BAML in a CocoIndex function

`extract_patient_info` is the single transform: PDF bytes in, a typed `Patient` out. BAML's `baml_py.Pdf.from_base64` takes the raw bytes, and the generated `b.ExtractPatientInfo` does the typed extraction.

```python title="main.py"
import base64
import pathlib

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from baml_client import b
from baml_client.types import Patient
import baml_py


@coco.fn
async def extract_patient_info(content: bytes) -> Patient:
    """Extract patient information from PDF content using BAML."""
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)
```

The return type is `Patient` — the actual Pydantic class BAML generated, not a dict — so everything downstream is typed and validated. There's no prompt engineering or response parsing here; that all lives in the BAML schema, and the LLM call is one `await`.

## Process a file

![One processing component per intake form: read the PDF, extract a typed Patient with BAML, serialize to JSON, and declare a JSON file into the output directory](https://cocoindex.io/blobs/docs-v1/img/examples/patient-intake-baml/stage-file-process.png)

`process_patient_form` runs once per PDF. It reads the file, runs the BAML extraction, dumps the typed `Patient` to JSON, and declares one output file named after the source form.

```python title="main.py"
@coco.fn(memo=True)
async def process_patient_form(file: FileLike, outdir: pathlib.Path) -> None:
    """Process a patient intake form PDF and extract structured information."""
    content = await file.read()
    patient_info = await extract_patient_info(content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.file_path.path.stem + ".json"
    localfs.declare_file(
        outdir / output_filename, patient_json, create_parent_dirs=True
    )
```

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a PDF's content and this function's code are both unchanged, the whole file is skipped on the next run — so you never pay for a second Gemini call on a form you've already extracted. `patient_info.model_dump_json(indent=2)` serializes the validated model, and `localfs.declare_file` declares the JSON file as a [target state](https://cocoindex.io/docs/programming_guide/target_state/); CocoIndex handles writing, updating, or deleting it to match.

## Define the main function

`app_main` wires the source to the target. It walks the source directory for PDFs and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file with `mount_each`.

```python title="main.py"
@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    """Main application function that processes patient intake forms."""
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await coco.mount_each(process_patient_form, files.items(), outdir)


app = coco.App(
    coco.AppConfig(name="PatientIntakeExtractionBaml"),
    app_main,
    sourcedir=pathlib.Path("./data/patient_forms"),
    outdir=pathlib.Path("./output_patients"),
)
```

The [filesystem source](https://cocoindex.io/docs/connectors/localfs/) walks `data/patient_forms/` for `*.pdf`, and `mount_each` runs one component per form so the engine can track and update each independently. `coco.App` binds the main function to its arguments — the source and output directories — into a runnable unit.

## Setup

- Install CocoIndex and the dependencies this example uses (BAML ships the client generator and the Python runtime):

  ```sh
  pip install -U cocoindex baml-py pydantic python-dotenv
  ```

- Generate the BAML client from the schema. This compiles `baml_src/patient.baml` into the `baml_client/` package that `main.py` imports:

  ```sh
  baml generate
  ```

- The extraction uses a Gemini vision model. Put your key in a `.env` file in the example directory (it's auto-loaded when you run):

  ```sh
  echo "GEMINI_API_KEY=your_api_key_here" > .env
  ```

- A few intake forms to extract. The example ships a `data/patient_forms/` folder with a handful of artificial PDFs — or drop your own in.

## Run the pipeline

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to build and update the index. A catch-up run scans the source, extracts, writes, and exits:

```sh
cocoindex update main.py
```

This reads each PDF in `data/patient_forms/`, extracts a `Patient`, and writes one JSON file per form to `output_patients/`. Check the output:

```sh
ls output_patients/
# Patient_Intake_Form_David_Artificial.json
# Patient_Intake_Form_Emily_Artificial.json
# ...one .json per intake PDF
```

Each file is a fully populated, schema-validated patient record — the same shape every time, ready to load into a database, a chart, or another pipeline.

## Incremental updates

CocoIndex keeps the output in sync with your intake forms and does the **minimum work** to get there. You never compute a diff or write update logic. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a form is skipped when its bytes and the function's code are both unchanged, so Gemini never re-extracts an unchanged PDF. `localfs.declare_file` decides what to *write* — the engine compares declared output files against what's on disk and applies only the difference.

- **A form is added** — only that PDF is extracted; its JSON file is written. The rest is untouched.
- **A form is replaced** — it is re-extracted and its JSON file is rewritten; every other form is left alone.
- **A form is deleted** — its JSON file is removed from the output directory automatically.

The same machinery covers **logic** changes too: edit the BAML schema (add a field, tighten a type) or swap the model, run `baml generate` again, and the next `cocoindex update main.py` re-extracts and rewrites — applying only the difference against what's already there.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/patient_intake_extraction_baml](https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction_baml). The exact same task — intake PDFs to a typed `Patient` — has a [DSPy twin](https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction_dspy) that swaps BAML for a DSPy signature and module, so you can compare the two structured-extraction libraries side by side on one flow.

Got a stack of forms, invoices, or reports you want to turn into validated records? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
