#!/usr/bin/env bash
# Provision the runtime expected by ort-sys 2.0.0-rc.9 (fastembed 4).
# CI-only: do not change the SDK's default feature or linking behavior.
set -euo pipefail

version=1.20.0
case "$RUNNER_OS-$RUNNER_ARCH" in
  Linux-X64) platform=linux-x64; library=libonnxruntime.so ;;
  Linux-ARM64) platform=linux-aarch64; library=libonnxruntime.so ;;
  macOS-ARM64) platform=osx-arm64; library=libonnxruntime.dylib ;;
  macOS-X64) platform=osx-x86_64; library=libonnxruntime.dylib ;;
  Windows-X64) platform=win-x64; library=onnxruntime.dll ;;
  *) echo "::error::No ONNX Runtime build for $RUNNER_OS-$RUNNER_ARCH"; exit 1 ;;
esac

# Git Bash accepts forward-slash paths, as do the GITHUB_ENV/PATH files.
dest=$(mktemp -d "${RUNNER_TEMP//\\//}/onnxruntime.XXXXXX")
name="onnxruntime-$platform-$version"
extension=tgz
[[ "$RUNNER_OS" == Windows ]] && extension=zip
archive="$dest/runtime.$extension"
curl -fsSL --retry 5 --connect-timeout 30 --max-time 600 -o "$archive" \
  "https://github.com/microsoft/onnxruntime/releases/download/v$version/$name.$extension"
if [[ "$extension" == zip ]]; then
  unzip -q "$archive" -d "$dest"
else
  tar -xzf "$archive" -C "$dest"
fi
install_dir="$dest/$name"
test -f "$install_dir/lib/$library"
if [[ "$RUNNER_OS" == Windows ]]; then
  test -f "$install_dir/lib/onnxruntime.lib"
fi

# Use the archive root, not lib/: ort-sys otherwise mistakes the Windows
# import library for a static library. Explicitly select dynamic linking.
{
  echo "ORT_LIB_LOCATION=$install_dir"
  echo "ORT_PREFER_DYNAMIC_LINK=1"
  case "$RUNNER_OS" in
    Linux) echo "LD_LIBRARY_PATH=$install_dir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
    macOS) echo "DYLD_LIBRARY_PATH=$install_dir/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
  esac
} >> "$GITHUB_ENV"
echo "$install_dir/lib" >> "$GITHUB_PATH"
echo "Installed ONNX Runtime $version for $platform"
