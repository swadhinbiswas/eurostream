# EuroStream Cookbook — Docs Site

The [EuroStream](https://github.com/swadhin/eurostream) engineering cookbook:
how the platform was built, why every decision was made, and how to defend it
in an interview.

Built with **[Astro](https://astro.build) + [Starlight](https://starlight.astro.build)**,
deployed as a static site on **Cloudflare Pages**.

## Commands

| Command | Action |
|---|---|
| `npm install` | Install dependencies |
| `npm run dev` | Dev server at `localhost:4321` |
| `npm run build` | Production build to `dist/` (sitemap, search index, JSON-LD) |
| `npm run preview` | Preview the production build locally |
| `./deploy.sh` | Build + deploy to Cloudflare Pages (`--branch <name>` for previews) |

## Content map

```
src/content/docs/
├── overview, quickstart, faq          # Start here
├── cookbook/                          # 7 recipes: problem → decision → code → gotchas
├── deep-dives/                        # Why the architecture wins, build log, prod playbook
├── interview/                         # Pitch, Q&A, trade-off cheatsheet
└── reference/                         # Config, API, deployment guides
```

## SEO machinery

- Per-page keyword-led `<title>` + meta `description` (frontmatter)
- Canonical URLs via Astro `site` config (`SITE_URL` env override)
- Open Graph + Twitter cards injected into every page at build time
- JSON-LD: `WebSite`/`Organization` on home; `FAQPage` on FAQ and Q&A pages
- `@astrojs/sitemap` → `sitemap-index.xml`; `public/robots.txt` references it
- Static HTML output — fully crawlable, no client-side rendering

Set a custom domain with `SITE_URL=https://your-domain npm run build`.
