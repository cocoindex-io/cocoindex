# Extract structured data from patient intake forms with LLM
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.


This repo shows how to use LLM to extract structured data from patient intake forms with different formats - like PDF, Docx, etc.
CocoIndex supports multiple [sources](https://cocoindex.io/docs/ops/sources) and [LLM models](https://cocoindex.io/docs/ai/llm) natively.

![Structured Data From Patient Intake Forms](https://github.com/user-attachments/assets/1f6afb69-d26d-4a08-8774-13982d6aec1e)


## Tutorials
- Step by step tutorial - Check out the [blog](https://cocoindex.io/blogs/patient-intake-form-extraction-with-llm).
- Video tutorial - [Youtube](https://youtu.be/_mjlwVtnBn0?si=cpH-4kkOAYm2HhK6).

## Prerequisite
1. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2. Install CocoIndex
   ```bash
   pip install -U cocoindex
   ```

3. Install MarkItDown
   ```bash
   pip install 'markitdown[all]'
   ```
4. Create a `.env` file from `.env.example`, and fill `OPENAI_API_KEY`.

## Run

Setup index:

```bash
cocoindex setup main.py
```

Update index:

```bash
cocoindex update main.py
```

Run query:

```bash
python main.py
```

Run with CocoInsight:
```bash
cocoindex server -ci main
```
<img width="1405" alt="Screenshot 2025-07-02 at 11 59 24 AM" src="https://github.com/user-attachments/assets/6f5154cd-8a53-4baa-b914-cd60ffecf3d4" />



View results at https://cocoindex.io/cocoinsight
