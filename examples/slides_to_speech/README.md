# Slides to Speech Example

This example demonstrates how to use CocoIndex to convert presentation slides from Google Drive into speech audio.

## License

This example is licensed under GPL-3.0-or-later because it uses piper-tts, which is GPL-licensed. See [LICENSE](LICENSE) for details.

## Overview

The pipeline performs the following steps:

1. **Read slides from Google Drive** - Monitors Google Drive folders for PDF presentation files
2. **Convert PDF pages to images** - Extracts each slide as an image
3. **Extract transcripts** - Uses BAML with Gemini Vision to analyze slide images and generate structured transcripts with speaker notes
4. **Generate speech** - Converts speaker notes to audio using piper-tts (high-quality neural TTS)
5. **Store in LanceDB** - Saves filename, page number, image, transcript, and audio data in MP3 format

## Prerequisites

1. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2. Prepare for Google Drive:

    - Setup a service account in Google Cloud, and download the credential file.
    - Share folders containing files you want to import with the service account's email address.

    See [Setup for Google Drive](https://cocoindex.io/docs/sources/googledrive#setup-for-google-drive) for more details.

3. Create `.env` file with your credential file and folder IDs.
    Starting from copying the `.env.example`, and then edit it to fill in your credential file path and folder IDs.

    ```sh
    cp .env.exmaple .env
    $EDITOR .env
    ```

4. Install dependencies:

    ```sh
    cd examples/slides_to_speech
    pip install -e .
    ```

5. Generate BAML client code:

    ```sh
    baml-cli generate --from baml_src
    ```

6. Download Piper TTS voice model:

    ```sh
    python -m piper.download_voices en_US-lessac-medium
    ```

## Run

Update index:

```sh
cocoindex update main
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
