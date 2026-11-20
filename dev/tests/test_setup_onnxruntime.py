"""Exercise CI runtime setup with real local archives and a fake downloader.

Run without the Python package or Rust toolchain:
    python -m unittest discover -s dev/tests -p test_setup_onnxruntime.py
"""

import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "setup_onnxruntime.sh"


class SetupOnnxRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.download = self.root / "download"
        self.github_env = self.root / "github-env"
        self.github_path = self.root / "github-path"
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "RUNNER_TEMP": str(self.root),
            "GITHUB_ENV": str(self.github_env),
            "GITHUB_PATH": str(self.github_path),
            "FAKE_DOWNLOAD": str(self.download),
            "CURL_LOG": str(self.root / "curl-log"),
        }
        curl = self.bin / "curl"
        curl.write_text(
            "#!/usr/bin/env bash\n"
            'printf \'%s\\n\' "$@" > "$CURL_LOG"\n'
            "[[ ${FAIL_DOWNLOAD:-0} == 1 ]] && exit 22\n"
            "while [[ $# -gt 0 ]]; do\n"
            '  if [[ $1 == -o ]]; then cp "$FAKE_DOWNLOAD" "$2"; exit; fi\n'
            "  shift\n"
            "done\n"
            "exit 2\n"
        )
        curl.chmod(0o755)

    def archive(self, platform: str, library: str, *, missing: bool = False) -> None:
        name = f"onnxruntime-{platform}-1.20.0"
        files = {} if missing else {f"{name}/lib/{library}": b"test runtime"}
        if platform.startswith("win-") and not missing:
            files[f"{name}/lib/onnxruntime.lib"] = b"test import library"
        if platform.startswith("win-"):
            with zipfile.ZipFile(self.download, "w") as archive:
                for path, content in files.items():
                    archive.writestr(path, content)
        else:
            with tarfile.open(self.download, "w:gz") as archive:
                for path, content in files.items():
                    info = tarfile.TarInfo(path)
                    info.size = len(content)
                    archive.addfile(info, io.BytesIO(content))

    def run_setup(self, runner_os: str, arch: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT)],
            env={**self.env, "RUNNER_OS": runner_os, "RUNNER_ARCH": arch},
            capture_output=True,
            text=True,
            check=False,
        )

    def test_supported_platforms_use_official_matching_archives(self) -> None:
        for runner_os, arch, platform, library in [
            ("Linux", "X64", "linux-x64", "libonnxruntime.so"),
            ("Linux", "ARM64", "linux-aarch64", "libonnxruntime.so"),
            ("macOS", "ARM64", "osx-arm64", "libonnxruntime.dylib"),
            ("macOS", "X64", "osx-x86_64", "libonnxruntime.dylib"),
            ("Windows", "X64", "win-x64", "onnxruntime.dll"),
        ]:
            with self.subTest(platform=platform):
                self.github_env.unlink(missing_ok=True)
                self.github_path.unlink(missing_ok=True)
                self.archive(platform, library)
                result = self.run_setup(runner_os, arch)
                self.assertEqual(result.returncode, 0, result.stderr)
                values = dict(
                    line.split("=", 1)
                    for line in self.github_env.read_text().splitlines()
                )
                install_dir = Path(values["ORT_LIB_LOCATION"])
                self.assertTrue((install_dir / "lib" / library).is_file())
                self.assertEqual(values["ORT_PREFER_DYNAMIC_LINK"], "1")
                self.assertEqual(
                    self.github_path.read_text().strip(), str(install_dir / "lib")
                )
                extension = "zip" if runner_os == "Windows" else "tgz"
                self.assertIn(
                    "https://github.com/microsoft/onnxruntime/releases/download/"
                    f"v1.20.0/onnxruntime-{platform}-1.20.0.{extension}",
                    (self.root / "curl-log").read_text().splitlines(),
                )

    def test_preserves_existing_loader_paths(self) -> None:
        self.archive("linux-x64", "libonnxruntime.so")
        self.env["LD_LIBRARY_PATH"] = "/existing/lib"
        result = self.run_setup("Linux", "X64")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(":/existing/lib\n", self.github_env.read_text())

    def test_path_with_spaces(self) -> None:
        temp_with_spaces = self.root / "runner temp"
        temp_with_spaces.mkdir()
        self.env["RUNNER_TEMP"] = str(temp_with_spaces)
        self.archive("linux-x64", "libonnxruntime.so")
        result = self.run_setup("Linux", "X64")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(str(temp_with_spaces), self.github_env.read_text())

    def test_unsupported_platform_writes_no_environment(self) -> None:
        result = self.run_setup("Linux", "RISCV64")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.github_env.exists())
        self.assertFalse((self.root / "curl-log").exists())

    def test_download_failure_writes_no_environment(self) -> None:
        self.env["FAIL_DOWNLOAD"] = "1"
        result = self.run_setup("Linux", "X64")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.github_env.exists())
        self.assertFalse(self.github_path.exists())

    def test_missing_library_writes_no_environment(self) -> None:
        self.archive("linux-x64", "libonnxruntime.so", missing=True)
        result = self.run_setup("Linux", "X64")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.github_env.exists())

    def test_missing_windows_import_library_writes_no_environment(self) -> None:
        with zipfile.ZipFile(self.download, "w") as archive:
            archive.writestr(
                "onnxruntime-win-x64-1.20.0/lib/onnxruntime.dll", b"test runtime"
            )
        result = self.run_setup("Windows", "X64")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.github_env.exists())

    def test_invalid_archive_writes_no_environment(self) -> None:
        self.download.write_text("not an archive")
        result = self.run_setup("Linux", "X64")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.github_env.exists())


if __name__ == "__main__":
    unittest.main()
