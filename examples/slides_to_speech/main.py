import os
import io
import base64
import datetime
import functools
from pathlib import Path
import cocoindex
import cocoindex.targets.lancedb as coco_lancedb
from dataclasses import dataclass
import pymupdf
from baml_client import b
from baml_client.types import SlideTranscript
import baml_py
from piper import PiperVoice
from pydub import AudioSegment


@dataclass
class SlidePage:
    page_number: int
    image_data: bytes


@functools.cache
def get_piper_voice() -> PiperVoice:
    """
    Load and cache the Piper voice model.
    This ensures the model is only loaded once across all invocations.

    Uses PIPER_MODEL_NAME environment variable (defaults to 'en_US-lessac-medium').
    """
    model_name = os.environ.get("PIPER_MODEL_NAME", "en_US-lessac-medium")
    model_path = f"{model_name}.onnx"

    if not Path(model_path).exists():
        raise FileNotFoundError(
            f"Piper model not found at {model_path}. "
            f"Please download it using: python3 -m piper.download_voices {model_name}"
        )

    return PiperVoice.load(model_path)


@cocoindex.op.function()
def pdf_to_images(content: bytes, mime_type: str) -> list[SlidePage]:
    """
    Convert each page of a PDF to an image using pymupdf.
    """
    result: list[SlidePage] = []
    if mime_type != "application/pdf":
        return result

    # Open PDF from bytes
    pdf_doc = pymupdf.open(stream=content, filetype="pdf")

    for page_num, page in enumerate(pdf_doc):
        # Render page to pixmap (image) at 2x resolution for better quality
        pix = page.get_pixmap(matrix=pymupdf.Matrix(2, 2))
        # Convert to PNG bytes
        img_bytes = pix.tobytes("png")

        result.append(SlidePage(page_number=page_num + 1, image_data=img_bytes))

    pdf_doc.close()
    return result


@cocoindex.op.function(cache=True, behavior_version=1)
async def extract_slide_transcript(image_data: bytes) -> SlideTranscript:
    """
    Extract transcript from a slide image using BAML.
    """
    image = baml_py.Image.from_base64(
        "image/png", base64.b64encode(image_data).decode("utf-8")
    )
    return await b.ExtractSlideTranscript(image)


@cocoindex.op.function(cache=True, behavior_version=1)
def text_to_speech(text: str) -> bytes:
    """
    Convert text to speech audio using piper-tts.
    Returns audio data as bytes (MP3 format).
    """
    # Get the cached Piper voice model
    voice = get_piper_voice()

    # Synthesize speech - collect audio chunks from iterator
    chunks = list(voice.synthesize(text))

    # Combine all audio chunks
    pcm_bytes = b"".join(chunk.audio_int16_bytes for chunk in chunks)

    # Convert PCM to MP3 using pydub
    # Get audio parameters from first chunk
    first_chunk = chunks[0]
    audio = AudioSegment(
        data=pcm_bytes,
        sample_width=first_chunk.sample_width,
        frame_rate=first_chunk.sample_rate,
        channels=first_chunk.sample_channels,
    )
    mp3_data = io.BytesIO()
    audio.export(mp3_data, format="mp3", bitrate="64k")

    return mp3_data.getvalue()


@cocoindex.flow_def(name="SlidesToSpeech")
def slides_to_speech_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that converts slides from Google Drive to speech.

    This flow:
    1. Reads PDF slides from Google Drive
    2. Converts each page to an image
    3. Extracts transcript from the image using BAML
    4. Converts transcript to speech audio
    5. Stores results in LanceDB
    """
    # Set up Google Drive source
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.GoogleDrive(
            service_account_credential_path=credential_path,
            root_folder_ids=root_folder_ids,
            binary=True,
        ),
        refresh_interval=datetime.timedelta(minutes=1),
    )

    # Create collector for slide data
    slides_output = data_scope.add_collector()

    # Process each document
    with data_scope["documents"].row() as doc:
        # Convert PDF to images (one per page)
        doc["pages"] = flow_builder.transform(
            pdf_to_images, doc["content"], doc["mime_type"]
        )

        with doc["pages"].row() as page:
            # Extract transcript from slide image
            page["transcript"] = page["image_data"].transform(extract_slide_transcript)

            # Convert speaker notes to speech audio
            page["voice"] = page["transcript"]["speaker_notes"].transform(
                text_to_speech
            )

            # Collect the results
            slides_output.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=doc["filename"],
                page=page["page_number"],
                image=page["image_data"],
                transcript=page["transcript"],
                voice=page["voice"],
            )

    # Export to LanceDB
    slides_output.export(
        "slides",
        coco_lancedb.LanceDB(
            db_uri=os.environ.get("LANCEDB_URI", "./lancedb_data"),
            table_name="slides_to_speech",
        ),
        primary_key_fields=["id"],
    )
