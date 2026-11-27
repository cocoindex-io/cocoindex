#!/bin/sh
# The linker of this repository's Linux arm64 builds, named in
# `.cargo/config.toml`: the C compiler driver, run as rustc runs it, linking
# with lld where lld is installed.
#
# rustc links through `cc`, and on Linux arm64 `cc` links with GNU ld, which
# is slow and memory-hungry on this workspace: it takes 143 s and 4.3 GB to
# link the Rust SDK's integration test binary, where lld takes 4 s and 3.0 GB.
# Linux x86_64 needs no such wrapper, since rustc links with lld there by
# itself, and macOS and Windows have their own linkers.
#
# Without lld this links as it did before, so lld is not needed to build.
if command -v ld.lld > /dev/null 2>&1; then
    exec cc -fuse-ld=lld "$@"
fi
exec cc "$@"
