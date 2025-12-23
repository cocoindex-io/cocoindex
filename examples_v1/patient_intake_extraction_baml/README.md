# Extract structured data from patient intake forms with BAML (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

This example shows how to use [BAML](https://boundaryml.com/) to extract structured data from patient intake PDFs using CocoIndex v1. BAML provides type-safe structured data extraction with native PDF support.

- **BAML Schema** (`baml_src/patient.baml`) - Defines the data structure and extraction function
- **CocoIndex v1 App** (`main.py`) - Wraps BAML in a custom function, processes files incrementally, and writes results to JSON files

## Run

### 1. Install dependencies

Install from the project's pyproject.toml:

```sh
pip install -e .
```

### 2. Generate BAML client code

This is a required step that generates the Python client code from your BAML schema:

```sh
baml generate
```

This will create a `baml_client/` directory with the generated Python code.

### 3. Set up environment variables

Create a `.env` file in the example directory:

```sh
echo "GEMINI_API_KEY=your_api_key_here" > .env
```

Replace `your_api_key_here` with your actual Gemini API key.

### 4. Run the application

```sh
python main.py
```

This will:
1. Read all PDF files from `data/patient_forms/`
2. Extract patient information using BAML
3. Write the extracted data as JSON files to `output_patients/`

### 5. Verify the output

After running, check the `output_patients/` directory:

```sh
ls -la output_patients/
```

You should see JSON files such as:
- `Patient_Intake_Form_David_Artificial.json`
- `Patient_Intake_Form_Emily_Artificial.json`
- `Patient_Intake_Form_Joe_Artificial.json`
- `Patient_Intake_From_Jane_Artificial.json`
