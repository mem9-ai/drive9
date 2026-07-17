#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))

import verify_local_video_voice_search_demo as demo


class VideoVoiceDemoHelpersTest(unittest.TestCase):
    def test_normalize_remote_prefix(self) -> None:
        self.assertEqual(demo.normalize_remote_prefix("video-demo"), "/video-demo/")
        self.assertEqual(demo.normalize_remote_prefix("/video-demo"), "/video-demo/")
        self.assertEqual(demo.normalize_remote_prefix("/video-demo/"), "/video-demo/")

    def test_supported_media_extension_accepts_current_closed_set(self) -> None:
        self.assertEqual(demo.supported_media_extension("clip.MP4"), ".mp4")
        self.assertEqual(demo.supported_media_extension("voice.m4a"), ".m4a")
        self.assertEqual(demo.supported_media_extension("speech.mp3"), ".mp3")
        self.assertEqual(demo.supported_media_extension("sample.wav"), ".wav")

    def test_supported_media_extension_rejects_webm(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported media extension"):
            demo.supported_media_extension("clip.webm")

    def test_make_demo_path_uses_prefix_and_extension(self) -> None:
        path = demo.make_demo_path("/video-voice-demo", ".mp4")
        self.assertTrue(path.startswith("/video-voice-demo/voice-"))
        self.assertTrue(path.endswith(".mp4"))

    def test_parse_args_requires_provider_media(self) -> None:
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                demo.parse_args(["--mode", "provider"])


if __name__ == "__main__":
    unittest.main()
