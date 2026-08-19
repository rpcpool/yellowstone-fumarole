#!/bin/bash
set -euo pipefail

# Publishes the napi-build.yaml CI workflow's `npm-publish-ready` artifact zip
# to npm: platform packages first, then the root package (root's
# optionalDependencies pin exact versions of the platform packages, so they
# must exist on the registry first).
#
# Usage:
#   scripts/publish-from-artifact.sh [path/to/npm-publish-ready.zip] [--dry-run]

zip_path="npm-publish-ready.zip"
dry_run=false

for arg in "$@"; do
    case "$arg" in
        --dry-run) dry_run=true ;;
        *) zip_path="$arg" ;;
    esac
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

for platform_dir in "$work_dir"/npm/*/; do
    platform_name="$(node -p "require('${platform_dir}package.json').name")"
    echo "--- Publishing $platform_name ---"
    (cd "$platform_dir" && npm publish "${publish_flags[@]}")
done

echo "--- Publishing $name (root package) ---"
(cd "$work_dir" && npm publish "${publish_flags[@]}")

echo "Done."
