#!/bin/bash
set -euo pipefail

# Publishes the napi-build.yaml CI workflow's `npm-publish-ready` artifact zip
# to npm: platform packages first, then the root package (root's
# optionalDependencies pin exact versions of the platform packages, so they
# must exist on the registry first).
#
# Usage:
#   scripts/publish-from-artifact.sh [path/to/npm-publish-ready.zip] [--dry-run] [--only <platform-dir-name>]
#
# --only restricts publishing to a single platform dir under npm/ (e.g.
# "linux-x64-musl") and skips every other platform package and the root
# package. Use this when only some platform packages are new/changed and the
# rest (including root) are already published at this version.

zip_path="npm-publish-ready.zip"
dry_run=false
only_platform=""

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) dry_run=true ;;
        --only) shift; only_platform="$1" ;;
        *) zip_path="$1" ;;
    esac
    shift
done

if [ ! -f "$zip_path" ]; then
    echo "Artifact zip not found: $zip_path" >&2
    exit 1
fi
zip_path="$(realpath "$zip_path")"

if ! npm whoami >/dev/null 2>&1; then
    echo "Not authenticated with npm (npm whoami failed)." >&2
    echo "Run 'npm login', or fix the token in ~/.npmrc, then retry." >&2
    exit 1
fi

repo_root="$(git rev-parse --show-toplevel)"
# napi prepublish's prepublishOnly hook shells out to `git log`, so the
# extraction dir must live inside the repo (not /tmp) to find our .git.
work_dir="$(mktemp -d "$repo_root/.napi-publish-XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

echo "Extracting $zip_path"
unzip -q "$zip_path" -d "$work_dir"

version="$(node -p "require('$work_dir/package.json').version")"
name="$(node -p "require('$work_dir/package.json').name")"
echo "Publishing $name@$version"

publish_flags=(--access public)
if [ "$dry_run" = true ]; then
    publish_flags+=(--dry-run)
    echo "(dry run — nothing will actually be published)"
fi

if [ -n "$only_platform" ]; then
    platform_dir="$work_dir/npm/$only_platform"
    if [ ! -d "$platform_dir" ]; then
        echo "No such platform dir in artifact: npm/$only_platform" >&2
        echo "Available: $(cd "$work_dir/npm" && ls)" >&2
        exit 1
    fi
    platform_name="$(node -p "require('$platform_dir/package.json').name")"
    echo "--- Publishing $platform_name only ---"
    (cd "$platform_dir" && npm publish "${publish_flags[@]}")
    echo "Done. (root package and other platforms skipped due to --only)"
    exit 0
fi

for platform_dir in "$work_dir"/npm/*/; do
    platform_name="$(node -p "require('${platform_dir}package.json').name")"
    echo "--- Publishing $platform_name ---"
    (cd "$platform_dir" && npm publish "${publish_flags[@]}")
done

echo "--- Publishing $name (root package) ---"
(cd "$work_dir" && npm publish "${publish_flags[@]}")

echo "Done."
