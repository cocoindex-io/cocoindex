<p align="center">
    <img src="https://cocoindex.io/images/github.svg" alt="CocoIndex">
</p>

<h1 align="center">Slides to Speech</h1>

<div align="center">

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
[![Documentation](https://img.shields.io/badge/Documentation-394e79?logo=readthedocs&logoColor=00B9FF)](https://cocoindex.io/docs/getting_started/quickstart)
[![PyPI version](https://img.shields.io/pypi/v/cocoindex?color=5B5BD6)](https://pypi.org/project/cocoindex/)
<!--[![PyPI - Downloads](https://img.shields.io/pypi/dm/cocoindex)](https://pypistats.org/packages/cocoindex) -->
[![PyPI Downloads](https://static.pepy.tech/badge/cocoindex/month)](https://pepy.tech/projects/cocoindex)
[![CI](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml)
[![release](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml)
[![Link Check](https://github.com/cocoindex-io/cocoindex/actions/workflows/links.yml/badge.svg)](https://github.com/cocoindex-io/cocoindex/actions/workflows/links.yml)
[![Discord](https://img.shields.io/discord/1314801574169673738?logo=discord&color=5B5BD6&logoColor=white)](https://discord.com/invite/zpA9S2DR7s)

</div>


This example demonstrates how to use CocoIndex to convert presentation slides from Google Drive into speech audio.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="2732" height="1540" alt="cover" src="https://github.com/user-attachments/assets/dbced683-abc1-439c-9ead-678c028b0912" />

## License
- CocoIndex is licensed under Apache 2.0.
- This particular example is licensed under GPL-3.0-or-later because it uses piper-tts, which is GPL-licensed. See [LICENSE](LICENSE) for details.

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

<img width="2716" height="1410" alt="cocoinsight-transcript-extraction" src="https://github.com/user-attachments/assets/8c11418d-3bd7-40cd-9189-18929c206ecb" />
