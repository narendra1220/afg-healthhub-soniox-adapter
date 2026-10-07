#!/usr/bin/env bash

set -euo pipefail

adapter_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_name=${1:-${GITHUB_REPO_NAME:-afg-healthhub-soniox-adapter}}
remote_url=${GITHUB_REMOTE_URL:-}

readonly files_to_publish=(
  adapter.mjs
  offline-test.mjs
  probe-stt.mjs
  probe-tts.mjs
  package.json
  pnpm-lock.yaml
  Dockerfile
  render.yaml
  README.md
  .env.example
  .gitignore
  artemis-provider-registration.example.json
  artemis-pipeline-voice-speech.example.json
)

fail() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

command -v git >/dev/null 2>&1 || fail 'git is required.'

cd -- "$adapter_dir"

for file in "${files_to_publish[@]}"; do
  test -f "$file" || fail "expected adapter file is missing: $file"
done

# Refuse to publish obvious secret material or local audio fixtures. Do not
# print matching contents because a failed safety check must not leak a key.
if test -f .env; then
  fail '.env exists; remove it before publishing.'
fi
readonly secret_scan_files=(
  adapter.mjs
  offline-test.mjs
  probe-stt.mjs
  probe-tts.mjs
  package.json
  pnpm-lock.yaml
  Dockerfile
  render.yaml
  artemis-provider-registration.example.json
  artemis-pipeline-voice-speech.example.json
)
if rg -l \
  'snx_proj_[A-Za-z0-9._-]{20,}|sk-[A-Za-z0-9_-]{20,}|BEGIN (RSA|OPENSSH|EC) PRIVATE KEY' \
  "${secret_scan_files[@]}" >/dev/null 2>&1; then
  fail 'possible credential material found in the adapter directory.'
fi
if find . -maxdepth 1 -type f \( -name '*.pcm' -o -name '*.wav' -o -name '*.mp3' \) -print -quit | grep -q .; then
  fail 'audio fixtures found in the adapter directory; remove them before publishing.'
fi

has_origin=0
if test -d .git && git remote get-url origin >/dev/null 2>&1; then
  has_origin=1
fi
if test "$has_origin" -eq 0 && test -z "$remote_url"; then
  if command -v gh >/dev/null 2>&1; then
    gh auth status >/dev/null 2>&1 || fail 'gh is installed but not authenticated; run gh auth login.'
  else
    fail 'gh is not installed. Create an empty private GitHub repository, then rerun with GITHUB_REMOTE_URL=https://github.com/<account>/<repo>.git'
  fi
fi

if test ! -d .git; then
  git init
fi
git branch -M main

# Stage only the reviewed adapter allowlist; never use git add . here.
git add -- "${files_to_publish[@]}"
if ! git diff --cached --quiet; then
  git commit -m 'Add Soniox BYO STT adapter'
fi

if git remote get-url origin >/dev/null 2>&1; then
  current_origin=$(git remote get-url origin)
  if test -n "$remote_url" && test "$current_origin" != "$remote_url"; then
    fail "origin already points to $current_origin; refusing to replace it."
  fi
  remote_url=$current_origin
else
  if test -n "$remote_url"; then
    git remote add origin "$remote_url"
  elif command -v gh >/dev/null 2>&1; then
    printf 'Creating private GitHub repository %s ...\n' "$repo_name"
    gh repo create "$repo_name" \
      --private \
      --source=. \
      --remote=origin \
      --description 'English-only Soniox BYO STT adapter for AFG HealthHub Pipeline Voice'
  else
    fail 'gh is not installed. Create an empty private GitHub repository, then rerun with GITHUB_REMOTE_URL=https://github.com/<account>/<repo>.git'
  fi
fi

test -n "$(git remote get-url origin)" || fail 'origin remote was not configured.'
git push --set-upstream origin main

printf '\nPublished successfully.\n'
printf 'Repository: %s\n' "$(git remote get-url origin)"
printf 'Next: open Render → New → Blueprint and select this private repository.\n'
