#!/bin/sh
# Builds and pushes dist/ to the playground repo's gh-pages branch.
set -e

REPO=https://github.com/Mach-Five-Group/chat-syncopation-playground.git
HERE=$(cd "$(dirname "$0")" && pwd)

cd "$HERE"
npm run build

WORK=$(mktemp -d)
git clone --branch gh-pages --depth 1 "$REPO" "$WORK" 2>/dev/null || {
  git clone --depth 1 "$REPO" "$WORK"
  cd "$WORK" && git checkout --orphan gh-pages && git rm -rqf . 2>/dev/null || true
}

cd "$WORK"
# Keep .git, replace everything else: a stale asset served from gh-pages is
# worse than a missing one, because it looks current.
find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
cp -R "$HERE/dist/." .
touch .nojekyll        # or GitHub Pages hides files beginning with _

git add -A
if git diff --cached --quiet; then
  echo "playground already up to date"
else
  git commit -q -m "build: playground $(date -u '+%Y-%m-%d %H:%M')"
  git push -q origin gh-pages
  echo "deployed to https://mach-five-group.github.io/chat-syncopation-playground/"
fi
rm -rf "$WORK"
