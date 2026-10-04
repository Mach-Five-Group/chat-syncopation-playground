# Chat Syncopation playground

A Vite app that exercises every component in a real browser, plus `verify.mjs`,
which measures WCAG contrast across all six OS-preference × `theme` combinations.

```bash
npm install
npm run dev                       # iterate
npm run build && npm run preview  # then, in another shell:
npm run verify                    # the contrast + layout audit
```

## Why this exists

The package's jsdom suite has no layout engine and no computed style, so it is
structurally blind to contrast, overflow, stacking and scroll position. Every
visual defect in this family of packages was found here, not there — while the unit
suite stayed green.

Run `npm run verify` before any release that touches component CSS or markup.

## The dependency points at the working tree

`file:../..` while the package is unpublished. Once it is on npm, switch to the
registry version so the playground verifies what consumers actually get:

```bash
npm pkg set dependencies.@machfivetechchicago/machvive-chat-syncopation-ai=^0.1.0
rm -rf node_modules package-lock.json && npm install
```

**Delete the lock file and `node_modules` when switching in either direction.** npm
keeps the old symlink otherwise, and verification silently tests your working tree
while appearing to test the registry copy.

## Playwright

Launch with `chromium.launch({ channel: 'chrome' })` — the cached Playwright
Chromium is a version behind on this machine and fails to launch.

## Deploying

`./deploy.sh` builds and pushes `dist/` to the `chat-syncopation-playground` repo's
`gh-pages` branch.
