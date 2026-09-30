# FreqInOut documentation site

This directory is the curated source for the public FreqInOut documentation
prototype. VitePress builds only this directory and explicitly selected image
assets. Do not point GitHub Pages at the repository root or copy internal
project documentation into this tree.

## Local preview

Use Node.js 20 or newer:

```bash
cd website
npm ci
npm run dev
```

The configured GitHub Pages base path is `/FreqInOut/`.

## Release verification

```bash
npm ci
npm run verify
```

`npm run verify` audits the locked dependency tree and performs a production
build. The generated site is written to `website/.vitepress/dist/` and is not
tracked by Git.

## Content boundary

- Publish task-focused operator documentation only.
- Do not publish `docs/internal`, private testing material, databases, logs, or
  configuration samples containing station secrets.
- Review screenshots for callsigns, coordinates, messages, access codes, and
  local paths.
- Keep installation claims aligned with the current public release.
- Prefer one authoritative page over duplicated instructions.
