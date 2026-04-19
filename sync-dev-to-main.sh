#!/usr/bin/env bash
# Merge local dev (this repo) into main in a sibling clone, then push main.
# Dev clone lives under /root/github/Duck (see DEV_REPO default).
# Usage: ./sync-dev-to-main.sh [--no-pull] [--no-push]
set -euo pipefail

DEV_REPO="${DEV_REPO:-/root/github/Duck/Fishin-Tiffin-dev}"
MAIN_REPO="${MAIN_REPO:-/root/github/Fishin-Tiffin}"

DO_PULL=1
DO_PUSH=1
for arg in "$@"; do
  case "$arg" in
    --no-pull) DO_PULL=0 ;;
    --no-push) DO_PUSH=0 ;;
    -h|--help)
      echo "Usage: $0 [--no-pull] [--no-push]"
      echo "  DEV_REPO defaults to /root/github/Duck/Fishin-Tiffin-dev"
      echo "  MAIN_REPO defaults to /root/github/Fishin-Tiffin"
      echo "  Override with DEV_REPO=/path MAIN_REPO=/path"
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

if [[ ! -d "$MAIN_REPO/.git" ]]; then
  echo "Main repo not found: $MAIN_REPO" >&2
  echo "Set MAIN_REPO to your Fishin-Tiffin clone path." >&2
  exit 1
fi

require_clean() {
  local repo="$1"
  if [[ -n "$(git -C "$repo" status --porcelain)" ]]; then
    echo "Working tree not clean: $repo" >&2
    git -C "$repo" status -sb >&2
    exit 1
  fi
}

require_clean "$DEV_REPO"
require_clean "$MAIN_REPO"

if (( DO_PULL )); then
  git -C "$DEV_REPO" fetch origin --prune
  git -C "$DEV_REPO" pull --ff-only origin dev
  git -C "$MAIN_REPO" fetch origin --prune
  git -C "$MAIN_REPO" pull --ff-only origin main
fi

git -C "$DEV_REPO" rev-parse --verify dev >/dev/null

git -C "$MAIN_REPO" checkout main
git -C "$MAIN_REPO" pull "$DEV_REPO" dev

if (( DO_PUSH )); then
  git -C "$MAIN_REPO" push origin main
  echo "Done: merged dev into main and pushed origin/main."
else
  echo "Done: merged dev into main (push skipped; run with push or push manually)."
fi
