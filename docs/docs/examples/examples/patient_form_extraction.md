---
title: Extract Nested Structured Data from Patient Form
description: Extract nested structured data from patient form
sidebar_class_name: hidden
slug: /examples/patient_form_extraction
canonicalUrl: '/examples/patient_form_extraction'
sidebar_custom_props:
  image: /img/examples/patient_form_extraction/cover.png
  tags: [structured-data-extraction, data-mapping ]
image: /img/examples/patient_form_extraction/cover.png
tags: [structured-data-extraction, data-mapping]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction" margin="0 0 24px 0" />
<YouTubeButton url="https://youtu.be/_mjlwVtnBn0?si=-TBImMyZbnKh-5FB" margin="0 0 24px 0" />

![Patient Form Extraction](/img/examples/patient_form_extraction/cover.png)

## Overview
With CocoIndex, you can easily define nested schema in Python dataclass and use LLM to extract structured data from unstructured data. This example shows how to extract structured data from patient intake forms.

:::info
The extraction quality is highly dependent on the OCR quality. You can use CocoIndex with any commercial parser or open source ones that is tailored for your domain for better results. For example, Document AI from Google Cloud and more.
:::

## Flow Overview

![Flow overview](/img/examples/patient_form_extraction/flow.png)

The flow itself is fairly simple.
1. Import a list o intake forms.
2. For each file:
    - Convert the file to Markdown.
    - Extract structured data from the Markdown.
3. Export selected fields to tables in Postgres with PGVector.

## Setup
- If you don't have Postgres installed, please refer to the [installation guide](https://cocoindex.io/docs/getting_started/installation).
-  [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai). Create a `.env` file from `.env.example`, and fill `OPENAI_API_KEY`.

Alternatively, we have native support for Gemini, Ollama, LiteLLM. You can choose your favorite LLM provider and work completely on-premises.

  <DocumentationButton url="https://cocoindex.io/docs/ai/llm" text="LLM" margin="0 0 16px 0" />


## Add source

Add source from local files.

```python
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
```

`flow_builder.add_source` will create a table with a few sub fields.

<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Sources" margin="0 0 16px 0" />


##  Parse documents with different formats to Markdown

Define a custom function to parse documents in any format to Markdown. Here we use [MarkItDown](https://github.com/microsoft/markitdown) to convert the file to Markdown. It also provides options to parse by LLM, like `gpt-4o`. At present, MarkItDown supports: PDF, Word, Excel, Images (EXIF metadata and OCR), etc.

```python
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
```

Next we plug it into the data flow.

```python
with data_scope["documents"].row() as doc:
    doc["markdown"] = doc["content"].transform(ToMarkdown(), filename=doc["filename"])
```

![Markdown](/img/examples/patient_form_extraction/tomarkdown.png)

## Define output schema

We are going to define the patient info schema for structured extraction. One of the best examples to define a patient info schema is probably following the [FHIR standard - Patient Resource](https://build.fhir.org/patient.html#resource).


In this tutorial, we'll define a simplified schema in nested dataclass for patient information extraction:

```python
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
```

A simplified illustration of the nested fields and its definition:

![Patient Fields](/img/examples/patient_form_extraction/fields.png)

## Extract structured data from Markdown
CocoIndex provides built-in functions (e.g. `ExtractByLlm`) that process data using LLMs. With CocoIndex, you can directly pass the Python dataclass `Patient` to the function, and it will automatically parse the LLM response into the dataclass.

```python
with data_scope["documents"].row() as doc:
    doc["patient_info"] = doc["markdown"].transform(
        cocoindex.functions.ExtractByLlm(
            llm_spec=cocoindex.LlmSpec(
                api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
            output_type=Patient,
            instruction="Please extract patient information from the intake form."))
    patients_index.collect(
        filename=doc["filename"],
        patient_info=doc["patient_info"],
    )
```

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#extractbyllm" text="ExtractByLlm" margin="0 0 16px 0" />

![Extracted](/img/examples/patient_form_extraction/extraction.png)

After the extraction, we collect all the fields for simplicity. You can also select any fields and also perform data mapping and field level transformation on the fields before the collection. If you have any questions, feel free to ask us in [Discord](https://discord.com/invite/zpA9S2DR7s).


##  Export the extracted data to a table

```python
patients_index.export(
    "patients",
    cocoindex.storages.Postgres(table_name="patients_info"),
    primary_key_fields=["filename"],
)
```

## Run and Query
### Install dependencies
    ```bash
    pip install -e .
    ```

### Setup and update the index
    ```sh
    cocoindex update --setup main.py
    ```
    You'll see the index updates state in the terminal

### Query the output table
After the index is built, you have a table with the name `patients_info`. You can query it at any time, e.g., start a Postgres shell:

```bash
psql postgres://cocoindex:cocoindex@localhost/cocoindex
```

The run:

```sql
select * from patients_info;
```

You could see the patients_info table.

## Evaluate
For mission-critical use cases, it is important to evaluate the quality of the extraction. CocoIndex supports a simple way to evaluate the extraction. More updates are coming soon.

1.  Dump the extracted data to YAML files.

    ```bash
    python3 main.py cocoindex evaluate
    ```

    It dumps what should be indexed to files under a directory. Using my example data sources, it looks like [the golden files](https://github.com/cocoindex-io/patient-intake-extraction/tree/main/data/eval_PatientIntakeExtraction_golden) with a timestamp on the directory name.


2.  Compare the extracted data with golden files.
    We created a directory with golden files for each patient intake form. You can find them [here](https://github.com/cocoindex-io/patient-intake-extraction/tree/main/data/eval_PatientIntakeExtraction_golden).

    You can run the following command to see the diff:
    ```bash
    diff -r data/eval_PatientIntakeExtraction_golden data/eval_PatientIntakeExtraction_output
    ```

    I used a tool called [DirEqual](https://apps.apple.com/us/app/direqual/id1435575700) for mac. We also recommend [Meld](https://meldmerge.org/) for Linux and Windows.

    A diff from DirEqual looks like this:


    And double click on any row to see file level diff. In my case, there's missing `condition` for `Patient_Intake_Form_Joe.pdf` file.


## Troubleshooting
If extraction is not ideal, this is how I troubleshoot. My original golden file for this record is [this one](https://github.com/cocoindex-io/patient-intake-extraction/blob/main/data/example_forms/Patient_Intake_Form_Joe_Artificial.pdf).

We could troubleshoot in two steps:
1. Convert to Markdown
2. Extract structured data from Markdown

I also use CocoInsight to help me troubleshoot.

```bash
cocoindex server -ci main
```

Go to `https://cocoindex.io/cocoinsight`. You could see an interactive UI to explore the data.


Click on the `markdown` column for `Patient_Intake_Form_Joe.pdf`, you could see the Markdown content. We could try a few different models with the Markdown converter/LLM to iterate and see if we can get better results, or needs manual correction.


## Connect to other sources
CocoIndex natively supports Google Drive, Amazon S3, Azure Blob Storage, and more.

<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Sources" margin="0 0 16px 0" />
