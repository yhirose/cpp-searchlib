#!/usr/bin/env bash
#
# Release a new version of cpp-searchlib.
#
# Usage: ./release.sh [--run] [--minor | --major]
#
# By default, runs in dry-run mode (no changes made).
# Pass --run to actually update files, commit, tag, and push.
# Pass --minor or --major to force a bigger bump than the automatic choice.
#
# cpp-httplib/cpp-peglib pick the bump from abidiff; the break that matters
# here is the index file format instead. A saved index records its
# format_type and schema_version, and one saved under another schema is
# refused, so a change of any kFormatType* / kSchemaVersion* constant since
# the last release means stored indexes have to be rebuilt -- that bumps
# minor instead of patch.
#
# This script:
#   1. Reads the current version from include/searchlib.h
#   2. Checks that the working directory is clean
#   3. Verifies CI status of the latest commit
#   4. Determines the version to release:
#        - if the current version has no vX.Y.Z tag yet, it is released
#          as-is (the bootstrap case: the version was bumped by hand or
#          set by a previous aborted release)
#        - otherwise: the index format constants changed since the last
#          release tag → minor bump (e.g., 0.1.0 → 0.2.0); unchanged → patch
#          bump (e.g., 0.1.0 → 0.1.1); --minor / --major override either way
#   5. Updates include/searchlib.h
#   6. Commits, tags (vX.Y.Z), and pushes

set -euo pipefail

cd "$(dirname "$0")/.."

VERSION_HEADER="include/searchlib.h"

DRY_RUN=1
FORCE_MINOR=0
FORCE_MAJOR=0
while [ $# -gt 0 ]; do
  case "$1" in
    --run)
      DRY_RUN=0
      shift
      ;;
    --minor)
      FORCE_MINOR=1
      shift
      ;;
    --major)
      FORCE_MAJOR=1
      shift
      ;;
    *)
      echo "Usage: $0 [--run] [--minor | --major]"
      exit 1
      ;;
  esac
done

# --- Step 1: Read current version from searchlib.h ---
CURRENT_VERSION=$(sed -n 's/^#define CPPSEARCHLIB_VERSION "\([^"]*\)"/\1/p' "$VERSION_HEADER")
IFS='.' read -r V_MAJOR V_MINOR V_PATCH <<< "$CURRENT_VERSION"

echo "==> Current version: $CURRENT_VERSION"

# --- Step 2: Check working directory is clean ---
if [ -n "$(git status --porcelain)" ]; then
  echo "Error: working directory is not clean"
  exit 1
fi

# --- Step 3: Check CI status of the latest commit ---
echo ""
echo "==> Checking CI status of the latest commit..."

HEAD_SHA=$(git rev-parse HEAD)
HEAD_SHORT=$(git rev-parse --short HEAD)
echo "    Latest commit: $HEAD_SHORT"

# Fetch all workflow runs for the HEAD commit. A commit can accumulate
# multiple runs of the same workflow (reruns), so judge only the most
# recent run of each workflow.
RUNS=$(gh run list --commit "$HEAD_SHA" \
         --json name,status,conclusion,headSha,createdAt |
       jq '[group_by(.name)[] | max_by(.createdAt)]')

NUM_RUNS=$(echo "$RUNS" | jq 'length')

if [ "$NUM_RUNS" -eq 0 ]; then
  echo "Error: No CI runs found for commit $HEAD_SHORT."
  echo "       Wait for CI to complete before releasing."
  exit 1
fi

echo "    Found $NUM_RUNS workflow run(s):"

FAILED=0
RUNNING=0
while IFS=$'\t' read -r name status conclusion; do
  # A run that hasn't completed yet has an empty conclusion; don't treat it
  # as a failure — the release should wait until CI finishes.
  if [ "$status" != "completed" ]; then
    echo "      [ .. ] $name (still running)"
    RUNNING=1
    continue
  fi

  if [ "$conclusion" = "success" ]; then
    echo "      [ OK ] $name"
  else
    echo "      [FAIL] $name ($conclusion)"
    FAILED=1
  fi
done < <(echo "$RUNS" | jq -r '.[] | [.name, .status, .conclusion] | @tsv')

if [ "$RUNNING" -eq 1 ]; then
  echo ""
  echo "Error: Some CI checks are still running. Wait for them to complete before releasing."
  exit 1
fi

if [ "$FAILED" -eq 1 ]; then
  echo ""
  echo "Error: Some CI checks failed. Fix them before releasing."
  exit 1
fi

echo "    All CI checks passed."

# --- Step 4: Determine the version to release ---
# The index format constants at a revision, one "name = value" per line.
index_format() {
  git show "$1:$VERSION_HEADER" 2>/dev/null |
    sed -n 's/^inline constexpr uint32_t \(k\(FormatType\|SchemaVersion\)[A-Za-z]*\) = \([0-9]*\);.*/\1 = \3/p'
}

if ! git rev-parse -q --verify "refs/tags/v$CURRENT_VERSION" >/dev/null; then
  # The version in the header has never been tagged: release it as-is.
  NEW_VERSION="$CURRENT_VERSION"
  echo ""
  echo "==> v$CURRENT_VERSION is not tagged yet → releasing the current version as-is"
elif [ "$FORCE_MAJOR" -eq 1 ]; then
  NEW_MAJOR=$((V_MAJOR + 1))
  NEW_VERSION="$NEW_MAJOR.0.0"
  echo ""
  echo "==> --major specified → major bump"
elif [ "$FORCE_MINOR" -eq 1 ]; then
  NEW_MINOR=$((V_MINOR + 1))
  NEW_VERSION="$V_MAJOR.$NEW_MINOR.0"
  echo ""
  echo "==> --minor specified → minor bump"
else
  PREV_FORMAT=$(index_format "v$CURRENT_VERSION")
  HEAD_FORMAT=$(index_format HEAD)
  if [ -z "$HEAD_FORMAT" ]; then
    echo "Error: could not read the index format constants from $VERSION_HEADER"
    exit 1
  fi

  if [ "$PREV_FORMAT" != "$HEAD_FORMAT" ]; then
    NEW_MINOR=$((V_MINOR + 1))
    NEW_VERSION="$V_MAJOR.$NEW_MINOR.0"
    echo ""
    echo "==> index format changed since v$CURRENT_VERSION → minor bump:"
    diff <(echo "$PREV_FORMAT") <(echo "$HEAD_FORMAT") | sed -n 's/^[<>]/    &/p' || true
  else
    NEW_PATCH=$((V_PATCH + 1))
    NEW_VERSION="$V_MAJOR.$V_MINOR.$NEW_PATCH"
    echo ""
    echo "==> index format unchanged since v$CURRENT_VERSION → patch bump"
  fi
fi

IFS='.' read -r N_MAJOR N_MINOR N_PATCH <<< "$NEW_VERSION"
VERSION_HEX=$(printf "0x%02x%02x%02x" "$N_MAJOR" "$N_MINOR" "$N_PATCH")

if [ "$DRY_RUN" -eq 1 ]; then
  echo "==> [DRY RUN] Version to release: $NEW_VERSION ($VERSION_HEX)"
else
  echo "==> Version to release: $NEW_VERSION ($VERSION_HEX)"
fi

# --- Step 5: Update files (no-op when releasing the current version) ---
echo ""
if [ "$DRY_RUN" -eq 1 ]; then
  if [ "$NEW_VERSION" != "$CURRENT_VERSION" ]; then
    echo "==> [DRY RUN] Would update $VERSION_HEADER:"
    echo "    CPPSEARCHLIB_VERSION     = \"$NEW_VERSION\""
    echo "    CPPSEARCHLIB_VERSION_NUM = \"$VERSION_HEX\""
    echo ""
    echo "==> [DRY RUN] Would commit, tag v$NEW_VERSION, and push."
  else
    echo "==> [DRY RUN] $VERSION_HEADER already at $NEW_VERSION; would tag v$NEW_VERSION and push."
  fi
  echo ""
  echo "==> Dry run complete. No changes were made."
else
  if [ "$NEW_VERSION" != "$CURRENT_VERSION" ]; then
    echo "==> Updating $VERSION_HEADER..."
    # `-i.bak` is the in-place form GNU and BSD sed both accept (`-i ''` is
    # BSD-only: GNU sed reads the '' as the script).
    sed -i.bak "s/#define CPPSEARCHLIB_VERSION \"[^\"]*\"/#define CPPSEARCHLIB_VERSION \"$NEW_VERSION\"/" "$VERSION_HEADER"
    sed -i.bak "s/#define CPPSEARCHLIB_VERSION_NUM \"0x[0-9a-fA-F]*\"/#define CPPSEARCHLIB_VERSION_NUM \"$VERSION_HEX\"/" "$VERSION_HEADER"
    rm -f "$VERSION_HEADER.bak"
    echo "    CPPSEARCHLIB_VERSION     = \"$NEW_VERSION\""
    echo "    CPPSEARCHLIB_VERSION_NUM = \"$VERSION_HEX\""

    # --- Step 6: Commit, tag, and push ---
    echo ""
    echo "==> Committing and tagging..."
    git add "$VERSION_HEADER"
    git commit -m "Release v$NEW_VERSION"
  else
    echo "==> $VERSION_HEADER already at $NEW_VERSION; tagging the current commit."
  fi
  git tag "v$NEW_VERSION"

  echo ""
  echo "==> Pushing..."
  git push && git push --tags

  echo ""
  echo "==> Released v$NEW_VERSION"
fi
