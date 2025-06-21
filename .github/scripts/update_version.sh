#!/bin/bash -e

# Extract version from tag (e.g. refs/tags/v1.2.3 -> 1.2.3)
if [[ ! "$GITHUB_REF" =~ ^refs/tags/v ]]; then
    echo "No version tag found"
    exit 0
fi
VERSION="${GITHUB_REF#refs/tags/v}"

echo "Building release version: $VERSION"

# Detect OS and set sed inline flag accordingly.
if [[ "$(uname)" == "Darwin" ]]; then
  # macOS/BSD sed requires an empty string as backup extension
  SED_INLINE=(-i '')
else
  # GNU sed on Linux can use -i without a backup extension
  SED_INLINE=(-i)
fi

# Update Cargo.toml
sed "${SED_INLINE[@]}" "s/^version = .*/version = \"$VERSION\"/" Cargo.toml
