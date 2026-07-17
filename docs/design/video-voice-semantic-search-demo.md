# Video Voice Semantic Search Demo

## Scope

This demo validates the fastest useful video semantic-search path for Drive9:
voice in supported media files becomes `files.content_text` through the existing
durable `audio_extract_text` worker, and search reuses the current grep, FTS, and
auto-embedding pipeline. It is a video voice-search demo, not visual scene
retrieval.

The integration deliberately avoids a sidecar media stack for the first PR:

- video/audio bytes remain ordinary Drive9 files
- `.mp4`, `.m4a`, `.mp3`, and `.wav` use the current audio extraction closed set
- transcripts are written into `files.content_text`
- search stays on existing Drive9 APIs
- no code is copied from external repositories

## GitHub Survey

| Repo | License | Fit | Notes |
| --- | --- | --- | --- |
| [backblaze-b2-samples/video-semantic-search](https://github.com/backblaze-b2-samples/video-semantic-search) | MIT | Best architecture reference | Full-stack TypeScript/Python sample: upload video, extract audio with ffmpeg, transcribe with Whisper, chunk transcript, embed chunks, search timestamped results. Good shape, but it is B2/Next/FastAPI-specific and heavier than a Drive9 demo needs. |
| [WxExLGHTZ/Multimodal-Video-Retrieval](https://github.com/WxExLGHTZ/Multimodal-Video-Retrieval) | MIT | Useful V2 reference | Combines OpenCLIP visual embeddings, Whisper transcription, and metadata with late fusion. Better for visual-scene retrieval, but requires Python model stack/GPU-style dependencies and is too heavy for a quick Drive9 PR. |
| [aihpi/workshop-video-search](https://github.com/aihpi/workshop-video-search) | No license found | Idea only | Demonstrates transcribe-and-search workflow, but no license means no code reuse. |
| [ashrielbrian/video_search](https://github.com/ashrielbrian/video_search) | No license found | Idea only | YouTube download, Whisper transcript segments, embeddings, and Supabase. No license and not aligned with Drive9 storage/search internals. |

The practical choice is to adopt the transcript-first architecture pattern, not
the implementation. Drive9 already has the key backend primitive on `main`:
`audio_extract_text` writes searchable transcript text into the same
`content_text` field used by existing retrieval.

## Demo Flow

1. Start `drive9-server-local` with TiDB auto-embedding mode and audio extraction
   enabled.
2. Upload a supported media object under `/video-voice-demo/`.
3. Wait for the durable `audio_extract_text` task to succeed for that file
   revision.
4. Assert `files.content_text` is populated.
5. Run grep under `/video-voice-demo/` for a spoken phrase and verify the uploaded
   path is returned.

Local deterministic smoke:

```bash
source ./scripts/drive9-server-local-env.sh
export DRIVE9_LOCAL_EMBEDDING_MODE=auto
export DRIVE9_AUDIO_EXTRACT_ENABLED=true
export DRIVE9_AUDIO_EXTRACT_MODE=stub
make run-server-local
python3 scripts/verify_local_video_voice_search_demo.py --mode stub
```

Provider smoke with a real video file:

```bash
source ./scripts/drive9-server-local-env.sh
export DRIVE9_LOCAL_EMBEDDING_MODE=auto
export DRIVE9_AUDIO_EXTRACT_ENABLED=true
export DRIVE9_AUDIO_EXTRACT_MODE=openai
export DRIVE9_AUDIO_EXTRACT_API_BASE=...
export DRIVE9_AUDIO_EXTRACT_API_KEY=...
export DRIVE9_AUDIO_EXTRACT_MODEL=whisper-1
make run-server-local
python3 scripts/verify_local_video_voice_search_demo.py --mode provider --video-file ./demo.mp4 --query "spoken phrase"
```

`qwen-asr` can be used the same way by setting
`DRIVE9_AUDIO_EXTRACT_MODE=qwen-asr` and the provider-specific API settings.

## Current Boundaries

- This is transcript search over media voice tracks. It does not index frames,
  objects, OCR, faces, or visual scenes.
- Results are file-level because Drive9 currently stores searchable text at
  `files.content_text`; there are no timestamped transcript segments yet.
- The current extraction closed set is `.mp4`, `.m4a`, `.mp3`, and `.wav`.
  WebM, OGG, FLAC, AAC, MOV, and MKV need explicit follow-up support.
- The extraction task is bounded by `DRIVE9_AUDIO_EXTRACT_MAX_BYTES`,
  `DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS`, and
  `DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES`; long videos can be truncated or skipped.
- Provider mode depends on the configured ASR accepting the uploaded container.
  If a provider rejects direct MP4 input, the next step is an ffmpeg/remux stage
  before calling ASR.

## Follow-Up PRs

- Add segment-level transcript storage with timestamps so search can return exact
  moments inside a video.
- Add optional ffmpeg extraction/remuxing for provider compatibility.
- Add visual retrieval as a separate representation using frame sampling plus
  CLIP-style embeddings; do not overload `content_text` with visual vectors.
- Surface this in UI/SDK flows once the backend demo is validated.
