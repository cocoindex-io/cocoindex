<p align="center">
  <a href="https://cocoindex.io/docs/examples/patient-intake-baml/" title="Extract typed patient records from intake-form PDFs with CocoIndex and BAML — one type-safe Gemini vision extraction per form, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/patient-intake-baml/cover.svg" alt="Turn messy patient intake PDFs into validated JSON with CocoIndex and BAML — declare the Patient schema in BAML, run one type-safe Gemini vision extraction per form, and write a schema-validated JSON record per patient" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Patient intake forms to <em>typed</em> JSON, with BAML.</h1>

<p align="center">
  <b>Declare the <em>Patient</em> schema in BAML, run one type-safe Gemini vision extraction per intake PDF, and get back a validated JSON record — the same shape every time — in plain async Python.</b><br/>
  The hard part isn't reading the PDF; it's getting data that matches a schema so downstream code can trust it.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/patient-intake-baml/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Intake forms are messy, multi-section PDFs — demographics, insurance, medications, allergies, consent — and the real challenge is getting back data that *matches a schema* every time. This pipeline uses [BAML](https://boundaryml.com/) to declare that schema and run a single type-safe extraction per form against a Gemini vision model. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed PDFs get re-extracted and the LLM call is skipped entirely for forms you've already processed.

## How it works

The schema lives in `baml_src/patient.baml`, not in Python. You describe the `Patient` record as BAML classes; the same file holds the extraction function and the `Gemini` client (pointing at `gemini-2.5-flash`), which reads PDFs natively as vision input — no separate parse or OCR step. Running `baml generate` compiles this into a `baml_client/` package you import from Python: `b.ExtractPatientInfo(...)` and the `Patient` Pydantic model.

The CocoIndex side is two short functions — wrap BAML in a `@coco.fn`, then declare one JSON file per form. Read it in [`main.py`](main.py):

```python
@coco.fn
async def extract_patient_info(content: bytes) -> Patient:
    """Extract patient information from PDF content using BAML."""
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)


@coco.fn(memo=True)
async def process_patient_form(file: FileLike, outdir: pathlib.Path) -> None:
    content = await file.read()
    patient_info = await extract_patient_info(content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.file_path.path.stem + ".json"
    localfs.declare_file(outdir / output_filename, patient_json, create_parent_dirs=True)
```

The return type is `Patient` — the actual Pydantic class BAML generated, not a dict — so everything downstream is typed and validated. There's no prompt engineering or response parsing here; that all lives in the BAML schema, and the LLM call is one `await`. `app_main` walks `data/patient_forms/` for `*.pdf` and runs one `process_patient_form` component per file with `mount_each`.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/patient-intake-baml/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the BAML schema, the type-safe extraction function, the per-form component, and what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **The schema is the contract.** `ExtractPatientInfo(intake_form: pdf) -> Patient` is the whole spec — BAML forces the model's output to conform, so every record has the same shape, ready to load into a database or chart.
- **Native PDF vision, no OCR.** The `Gemini` client reads the PDF directly as vision input — checkboxes, hand-filled fields, tables — with no separate parse or Markdown step.
- **Typed all the way down.** `b.ExtractPatientInfo` returns a generated Pydantic `Patient`, not a string to parse — `model_dump_json` serializes the validated model straight to disk.
- **Incremental by default.** `@coco.fn(memo=True)` skips a form entirely when its bytes and the function's code are unchanged, so you never pay for a second Gemini call on a PDF you've already extracted.
- **Compare libraries on one flow.** A [DSPy twin](https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction_dspy) runs the exact same task with a DSPy signature instead of BAML — same input, same output, swap the extraction layer.

## Run it

**1. Install:**

```sh
pip install -e .
```

**2. Generate the BAML client** — compiles `baml_src/patient.baml` into the `baml_client/` package that `main.py` imports (required):

```sh
baml generate
```

**3. Configure** — the extraction uses a Gemini vision model:

```sh
cp .env.example .env     # set GEMINI_API_KEY
```

**4. Run the pipeline** — a catch-up run scans the forms, extracts, writes, and exits:

```sh
cocoindex update main.py
```

This reads each PDF in `data/patient_forms/`, extracts a `Patient`, and writes one JSON file per form to `output_patients/`:

```sh
ls output_patients/
# Patient_Intake_Form_David_Artificial.json
# Patient_Intake_Form_Emily_Artificial.json
# ...one .json per intake PDF
```

Each file is a fully populated, schema-validated patient record — the same shape every time. Edit the BAML schema or swap the model, run `baml generate` again, and the next `cocoindex update main.py` re-extracts only what changed.

---

<p align="center">
  If this turned a stack of forms into clean records, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/patient-intake-baml/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/patient_intake_extraction_baml" alt="" width="1" height="1" />
