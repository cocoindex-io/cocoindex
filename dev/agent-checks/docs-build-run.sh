#!/usr/bin/env bash
# Portable docs build check for coding agents and local automation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}/docs"

# The docs need Node >= 22.12 (`engines` in docs/package.json), and a machine's
# system Node is often older: Ubuntu 24.04 ships 18, so on a dev box every
# build failed and the Stop hook reported an error for nothing the change did.
# Use the `node` on PATH when it is new enough; else one already installed (nvm,
# Homebrew, /opt); else the pinned release, downloaded once into a cache and
# checked against nodejs.org's SHASUMS256.txt.
NODE_PINNED=22.23.4

node_new_enough() {
  "$1" -e 'const [major, minor] = process.versions.node.split(".").map(Number);
process.exit(major > 22 || (major === 22 && minor >= 12) ? 0 : 1)' 2>/dev/null
}

node_cache="${XDG_CACHE_HOME:-${HOME}/.cache}/cocoindex-docs"

download_node() {
  local os arch name base tmp
  case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=darwin ;;
    *) return 1 ;;
  esac
  case "$(uname -m)" in
    x86_64 | amd64) arch=x64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) return 1 ;;
  esac
  name="node-v${NODE_PINNED}-${os}-${arch}"
  base="https://nodejs.org/dist/v${NODE_PINNED}"
  tmp="$(mktemp -d)"
  if (
    cd "${tmp}" &&
      curl -fsSL "${base}/${name}.tar.xz" -o "${name}.tar.xz" &&
      curl -fsSL "${base}/SHASUMS256.txt" -o SHASUMS256.txt &&
      grep " ${name}.tar.xz\$" SHASUMS256.txt >sum.txt &&
      if command -v sha256sum >/dev/null; then
        sha256sum -c --status sum.txt
      else
        shasum -a 256 -c --status sum.txt
      fi &&
      tar -xJf "${name}.tar.xz"
  ); then
    mkdir -p "${node_cache}"
    # Another build may have finished the same download first; either copy works.
    mv "${tmp}/${name}" "${node_cache}/${name}" 2>/dev/null || true
  fi
  rm -rf "${tmp}"
  [ -x "${node_cache}/${name}/bin/node" ] && echo "${node_cache}/${name}/bin/node"
}

find_node() {
  local candidate
  for candidate in \
    "$(command -v node || true)" \
    "${NVM_DIR:-${HOME}/.nvm}"/versions/node/v*/bin/node \
    /opt/homebrew/opt/node@22/bin/node \
    /usr/local/opt/node@22/bin/node \
    /opt/node*/bin/node \
    "${node_cache}"/node-v*/bin/node; do
    if [ -x "${candidate}" ] && node_new_enough "${candidate}"; then
      echo "${candidate}"
      return 0
    fi
  done
  download_node
}

if ! NODE_BIN="$(find_node)" || [ -z "${NODE_BIN}" ]; then
  echo "docs build: needs Node >= 22.12; found $(node --version 2>/dev/null || echo none)," \
    "and could not download Node ${NODE_PINNED}" >&2
  exit 1
fi
if [ "${NODE_BIN}" != "$(command -v node || true)" ]; then
  echo "docs build: using ${NODE_BIN} ($("${NODE_BIN}" --version)); the node on PATH is" \
    "$(node --version 2>/dev/null || echo missing)" >&2
  PATH="$(dirname "${NODE_BIN}"):${PATH}"
  export PATH
fi

# `npm ci`, not `npm i`: `npm i` re-serializes package-lock.json, which rewrites
# it whenever the local npm differs from the one that generated it. `npm ci`
# never writes the lockfile and fails if it disagrees with package.json. It also
# wipes node_modules, so skip it while npm's hidden lockfile (written by every
# install) is newer than both package.json and package-lock.json.
HIDDEN_LOCKFILE=node_modules/.package-lock.json
if [ ! -f "${HIDDEN_LOCKFILE}" ] ||
  [ package.json -nt "${HIDDEN_LOCKFILE}" ] ||
  [ package-lock.json -nt "${HIDDEN_LOCKFILE}" ]; then
  # No audit: its report says to run `npm audit fix`, which rewrites the lockfile.
  npm ci --no-audit --no-fund 2>&1
fi
npm run build 2>&1
