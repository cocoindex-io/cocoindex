# Extract structured data from patient intake forms with DSPy (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

This example shows how to use [DSPy](https://github.com/stanfordnlp/dspy) with Gemini 2.5 Flash (vision model) to extract structured data from patient intake PDFs using CocoIndex v1.

- **Pydantic Models** (`main.py`) - Defines the data structure using Pydantic for type safety
- **DSPy Module** (`main.py`) - Defines the extraction signature and module using DSPy's ChainOfThought with vision support
- **CocoIndex v1 App** (`main.py`) - Wraps DSPy in a custom function, processes files incrementally, and writes results to JSON files

## Key Features

- **Native PDF Support**: Converts PDFs to images and processes directly with vision models
- **DSPy Vision Integration**: Uses DSPy's `Image` type with `ChainOfThought` for visual document understanding
- **Structured Outputs**: Pydantic models ensure type-safe, validated extraction
- **No Text Extraction Required**: Directly processes PDF images without intermediate markdown conversion
- **Incremental Processing**: CocoIndex handles batching and caching automatically
- **Portable outputs**: JSON results stored locally under `output_patients/`

## Run

### 1. Install dependencies

Install from the project's pyproject.toml:

```sh
pip install -e .
```

This installs Pillow, which DSPy uses to process image bytes.

### 2. Set up environment variables

Create a `.env` file in the example directory:

```sh
echo "GEMINI_API_KEY=your_api_key_here" > .env
```

Replace `your_api_key_here` with your actual Gemini API key.

### 3. Run the application

```sh
python main.py
```

This will:
1. Read all PDF files from `data/patient_forms/`
2. Extract patient information using DSPy
3. Write the extracted data as JSON files to `output_patients/`

### 4. Verify the output

After running, check the `output_patients/` directory:

```sh
ls -la output_patients/
```

You should see JSON files such as:
- `Patient_Intake_Form_David_Artificial.json`
- `Patient_Intake_Form_Emily_Artificial.json`
- `Patient_Intake_Form_Joe_Artificial.json`
- `Patient_Intake_Form_Jane_Artificial.json`
