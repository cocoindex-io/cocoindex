# Video hybrid search (CocoIndex + zvec)

Index a folder of videos into an embedded [zvec](https://zvec.org) collection, one document per scene,
then search it by keyframe vector, spoken words, and time window in a single query. CocoIndex keeps the
collection in sync as the folder changes: add a clip and only its scenes process, delete a clip and its
scenes drop, change the CLIP model and the embeddings recompute while transcripts stay cached.

## The idea

A video library grows every time you record or download something, and later you want to find a moment
by what is on screen, what someone said, and roughly when. Re-running the whole pipeline on every clip
is wasteful once transcription and embedding are in the mix. A plain folder watcher does not help
either: it will not remove a deleted clip's scenes, and it will not notice that swapping the embedder
should recompute the vectors.

CocoIndex handles that. You declare the set of scene rows that should exist, and it reconciles the
collection to match, fingerprinting both the source content and the transform code. zvec holds the
keyframe vector, the transcript as a full-text field, and the scalar fields in one collection, so a
query can use all three at once.

People have been building local video search tools lately (the
[Framedex](https://news.ycombinator.com/item?id=48222733) and
[edit-mind](https://news.ycombinator.com/item?id=48528029) discussions on Hacker News). The part they
tend to hand-roll is keeping the index in sync as the library changes. That is the part CocoIndex owns
here.

## How it works

One document per scene:

```python
@dataclass
class Scene:
    id: str                                        # deterministic per (video_path, start)
    video_path: str                                # scalar filter
    start: float                                   # scalar seconds
    end: float                                      # scalar seconds
    transcript: Annotated[str, zvec.ZvecFtsType()]  # full-text search over spoken words
    embedding: NDArray[np.float32]                  # dense CLIP keyframe vector
```

The pipeline in [`main.py`](main.py) walks `./videos`, segments each clip into fixed `SCENE_SECONDS`
windows with `ffprobe` and `ffmpeg`, and for each scene pulls one keyframe and the audio slice, embeds
the keyframe with CLIP, transcribes the audio with faster-whisper, and declares a `Scene` row.

Each heavy stage is its own memoized function, composed with `use_mount`:

```python
@coco.fn(memo=True, deps=CLIP_MODEL_NAME)
async def embed_keyframe(frame_bytes: bytes) -> list[float]: ...

@coco.fn(memo=True, deps=WHISPER_MODEL_NAME)
async def transcribe_audio(audio_bytes: bytes) -> str: ...

embedding = await coco.use_mount(coco.component_subpath("embed", scene_id), embed_keyframe, frame_bytes)
transcript = await coco.use_mount(coco.component_subpath("transcribe", scene_id), transcribe_audio, audio_bytes)
```

Because embedding and transcription are separate memoized steps, changing the embedder recomputes only
`embed_keyframe` and leaves the transcripts cached.

## Run it

You need `ffmpeg` and `ffprobe` on your PATH (`brew install ffmpeg`, or `apt install ffmpeg`); the
download script uses them too. `main.py` checks for ffmpeg and errors early if it is missing.

**1. Install, then use the example's venv.**

```sh
cd examples/video_hybrid_search
uv sync
source .venv/bin/activate
```

Run everything through this venv. If you skip `source`, prefix the commands with `.venv/bin/` instead.
Plain `python` or `python3` uses your system interpreter, which does not have the project installed and
fails with `ImportError: cannot import name 'zvec' from 'cocoindex.connectors'`.

The state db (`cocoindex.db`) and the zvec collection (`zvec_data`) are written next to `main.py`, so
run the commands from this folder.

**2. Optional: a [Hugging Face token](https://huggingface.co/settings/tokens)** for faster,
warning-free model downloads. Copy the template and set `HF_TOKEN` in it:

```sh
cp .env.example .env
```

**3. Get some clips into `./videos`.** Download a small, openly licensed sample set (two short clips with
real scenes and English dialogue, a few MB total):

```sh
./download_sample_videos.sh
```

That gives you short segments from two Blender open movies, Tears of Steel and Sintel (both CC BY,
visually very different). ffmpeg pulls only the trimmed part over HTTP, so it is quick even though the
source films are large. Credits and licenses are in [ATTRIBUTION.md](ATTRIBUTION.md).

No network, or want a deterministic offline set? `./make_sample_videos.sh` (macOS `say`) generates
solid-color clips with a spoken sentence. They drive the whole pipeline for a quick check, but the
visuals are blank, so CLIP has nothing to match and results come only from the transcript.

**4. Index.** CocoIndex indexes the folder once and exits:

```sh
cocoindex update main.py
```

The first run downloads the CLIP and whisper models. The default models are small; set
`CLIP_MODEL=openai/clip-vit-large-patch14` for higher-quality embeddings.

> [!NOTE]
> Add `-L` to watch the folder live: `cocoindex update -L main.py`. It catches up, then reprocesses on
> every change, so adding or deleting a clip syncs its scenes within seconds. Live mode holds the
> terminal, so run queries and edit `./videos` from a second terminal in the same folder.

**5. Query.** `query.py` reads the zvec collection directly, outside CocoIndex. It fuses a dense
sub-query and a full-text sub-query with reciprocal rank fusion, and filters on the scalar fields:

```sh
python query.py "two people talking on a bridge" --fts "robotics"      # tears_of_steel: visual + spoken
python query.py "a red-haired girl" --mode dense                       # sintel: visual only
python query.py "" --mode fts --fts "robot hand"                       # tears_of_steel: transcript only
python query.py "two people talking on a bridge" --filter "start < 15" # tears_of_steel: visual + time window
```

On the downloaded clips: the bridge query returns the Tears of Steel canal scene, the red-haired-girl
query returns Sintel, the robot-hand query returns the Tears of Steel line about a robot hand, and the
filter keeps only scenes before the 15-second mark. The first dense query loads the CLIP model, so it
pauses for a bit and prints `Loading the CLIP model...` while it works. `--mode fts` skips CLIP and
returns quickly.

## Incremental behavior

Re-run `cocoindex update main.py` after each change and watch what it does:

- Add a clip to `./videos`, and only the new clip's scenes process. The rest are cache hits.
- Delete a clip, and its scenes disappear from the collection.
- Set `CLIP_MODEL` to a different model, and the embeddings recompute while the transcripts stay cached.
  If the new model has a different vector size, zvec rebuilds the collection schema and rewrites the
  vectors. To see only the embeddings change, switch between two same-size models, for example
  `clip-vit-base-patch32` and `clip-vit-base-patch16`, both 512-dim.

The state db and the zvec collection are a matched pair. To start over, delete both together:

```sh
rm -rf cocoindex.db zvec_data
```

## Troubleshooting

- **`ffmpeg not found` / `ffprobe not found`.** Install ffmpeg and make sure it is on your PATH.
- **`ImportError: cannot import name 'zvec' from 'cocoindex.connectors'`.** You ran with system Python.
  Activate the venv (`source .venv/bin/activate`) or prefix commands with `.venv/bin/`.
- **A dense query seems to hang.** The first one loads the CLIP model, downloading it on the very first
  run. Give it a minute. `--mode fts` skips CLIP.
- **`collection path .../zvec_data/scenes not exist`.** The state db and the collection are out of sync,
  usually because one got deleted on its own. Reset both and re-index: `rm -rf cocoindex.db zvec_data`,
  then `cocoindex update main.py`.
- **Empty or odd query results after changing models.** Query with the same `CLIP_MODEL` you indexed
  under, so the query vector matches the stored size. Otherwise re-index.
- **`Loaded environment variables from: ~/.env`.** The `cocoindex` CLI looks for a `.env` up the
  directory tree and fell back to your home one. Harmless here, since the state db path is pinned in
  `main.py`. Add a local `.env` (`cp .env.example .env`) to stop it climbing.
- **Not on a Mac.** `make_sample_videos.sh` (the offline fallback) uses macOS `say`. Use
  `./download_sample_videos.sh` instead, or add your own clips to `./videos`.

## Notes

- Scene IDs are the video path plus start time, sanitized to what zvec allows (`[A-Za-z0-9._-]`), with
  the real path kept in the `video_path` field. Move or rename files and you will want a content hash
  or a stable video id instead.
- Out of scope here: captioning with a vision model, face and object detection, diarization,
  scene-detection segmentation, and any UI past this CLI.
- This stores one dense keyframe vector per scene. zvec can hold several named vector fields and fuse
  them at query time, so a natural next step is two or three keyframes per scene. It has no
  late-interaction (MaxSim / ColPali) multivector field, so that style of retrieval is not an option.
- `download_sample_videos.sh` fetches the corpus at runtime and `make_sample_videos.sh` generates one,
  so no video binaries live in the repo. Both write into `./videos`, which is gitignored.
