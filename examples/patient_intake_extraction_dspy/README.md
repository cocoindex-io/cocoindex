<p align="center">
  <a href="https://cocoindex.io/docs/examples/patient-intake-dspy/" title="Extract typed patient records from intake-form PDFs with CocoIndex and DSPy — render each page to an image, a ChainOfThought vision module on Gemini extracts a typed Patient, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/patient-intake-dspy/cover.svg" alt="Turn patient intake PDFs into validated JSON with CocoIndex and DSPy — render each page to an image, a typed ChainOfThought vision module on Gemini reads the form like a person and returns a validated Patient, one JSON file per form" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Patient intake forms to <em>typed</em> JSON, with DSPy.</h1>

<p align="center">
  <b>Render each PDF page to an image and let a typed <em>ChainOfThought</em> vision module on Gemini read the form the way a person would — returning a validated <em>Patient</em>, no prompt strings to hand-tune — in plain async Python.</b><br/>
  Intake forms are visual: checkboxes, hand-filled fields, tables of medications — so we extract from the rendered page, not the text.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/patient-intake-dspy/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Intake forms are *visual* — checkboxes, hand-filled fields, tables of medications — so instead of extracting text, this pipeline renders each PDF page to an image and lets a vision model read the form the way a person would. [DSPy](https://github.com/stanfordnlp/dspy) handles the prompting: you declare a typed `Signature` and it produces a Pydantic `Patient`, no prompt strings to hand-tune. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed forms get re-extracted and each one becomes exactly one JSON file on disk.

## How it works

The output is just Python types: `Patient` (in `models.py`) is a Pydantic model with nested models for address, insurance, medications, and the rest. That model *is* the contract — DSPy fills it in, Pydantic validates it, it serializes straight to JSON. Rather than write a prompt, you declare a typed `Signature` and wrap it in `ChainOfThought` so the model reasons before it answers; `extract_patient` rasterizes every page to a PNG and runs the extractor. Read it in [`main.py`](main.py):

```python
class PatientExtractionSignature(dspy.Signature):
    """Extract structured patient information from a medical intake form image."""
    form_images: list[dspy.Image] = dspy.InputField(desc="Images of the intake form pages")
    patient: Patient = dspy.OutputField(desc="Extracted patient information")


class PatientExtractor(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        self.extract = dspy.ChainOfThought(PatientExtractionSignature)

    def forward(self, form_images: list[dspy.Image]) -> Patient:
        return self.extract(form_images=form_images).patient


@coco.fn
def extract_patient(pdf_content: bytes) -> Patient:
    pdf_doc = pymupdf.open(stream=pdf_content, filetype="pdf")
    form_images = []
    for page in pdf_doc:
        pix = page.get_pixmap(matrix=pymupdf.Matrix(2, 2))   # 2x scale: small print stays legible
        form_images.append(dspy.Image(pix.tobytes("png")))
    pdf_doc.close()
    return PatientExtractor()(form_images=form_images)
```

Because the `OutputField` is typed as `Patient`, DSPy asks the model for that exact shape and hands back a validated object, not a string to parse. The LM is configured once at module load — `dspy.configure(lm=dspy.LM("gemini/gemini-2.5-flash"))`. `process_patient_form` (decorated `@coco.fn(memo=True)`) reads each PDF, extracts the `Patient`, and declares one JSON file named after the source form; `app_main` runs one component per file with `mount_each`.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/patient-intake-dspy/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the Pydantic schema, the DSPy signature and module, the PDF rasterization, and what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Read the form, not the text.** Each page is rendered at `Matrix(2, 2)` so small hand-entered text stays legible — the difference between reading a zip code and guessing one. No OCR, no Markdown step.
- **Declared, not prompted.** A typed `Signature` plus `ChainOfThought` is the whole spec; DSPy compiles the typed in/out into the actual prompt and reasons before answering on dense, checkbox-heavy forms.
- **Typed and validated.** The `OutputField` is `Patient`, so DSPy returns a validated Pydantic object; optional fields and `default_factory=list` mean a form that omits medications yields an empty list, not a failure.
- **Incremental by default.** `@coco.fn(memo=True)` skips a form entirely when its bytes and the function's code are unchanged, so you never re-run the slow, paid vision extraction on a PDF you've already processed.
- **Compare libraries on one flow.** A [BAML twin](https://github.com/cocoindex-io/cocoindex/tree/main/examples/patient_intake_extraction_baml) runs the exact same task with a BAML schema instead of a DSPy signature — same input, same output, swap the extraction layer.

## Run it

**1. Install** — this pulls in DSPy, PyMuPDF (to rasterize PDFs), and Pillow (for the image bytes DSPy passes along):

```sh
pip install -e .
```

**2. Configure** — the extraction runs on a Gemini vision model:

```sh
cp .env.example .env     # set GEMINI_API_KEY
```

**3. Run the pipeline** — a catch-up run scans the forms, extracts, writes, and exits:

```sh
cocoindex update main.py
```

Each PDF in `data/patient_forms/` becomes a JSON file in `output_patients/`, named after the source form:

```sh
ls output_patients/
# Patient_Intake_Form_David_Artificial.json
# Patient_Intake_Form_Emily_Artificial.json
# ...one .json per intake PDF
```

Open one and you'll see the full `Patient` record — name, date of birth, address, insurance, the medication and allergy lists, consent — extracted straight from the rendered form and validated against the schema. Add a field to the `Patient` model or switch the LM, and the next run re-extracts only the affected forms.

---

<p align="center">
  If this turned a stack of forms into clean records, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/patient-intake-dspy/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/patient_intake_extraction_dspy" alt="" width="1" height="1" />
