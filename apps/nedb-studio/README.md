# NEDB Studio

**Prompt-to-database scaffolding for agent-native applications.**

A [Portal](https://github.com/interchained/Portal)-powered workspace over the [NEDB engine](https://github.com/Eth-Interchained/nedb) — the **phpMyAdmin for NEDB**. Describe an app in plain language and get a validated schema — collections, fields, types, relations, indexes, search fields, seed data, NQL, plus Python and Node snippets and a README — rendered as a live schema graph and exportable in one click.

Then **query your data in plain English**: the query console compiles natural language → **NQL** (the verifiable, editable intermediate) and runs it live against the seed data, in-browser — results in a table.

Generation runs through **AiAssist** (`api.aiassist.net`) as the AI gateway. With no credentials, the studio runs fully in **mock mode** on deterministic templates (and a heuristic NL→NQL compiler).

---

## Built on Portal

This is a Portal app, not a generic SPA. It uses Portal conventions throughout:

- **Contract** — `app.contract.ts` via `defineApp(...)` (schema v1.1): identity, audience, conversion goals, integrations, and quality gates.
- **File-based routes** — `routes/*.page.tsx`, each exporting an `intent` object for the agents.
- **Entry** — `src/main.tsx` wraps the app in `<PortalProvider routes={routes} contract={contract} />` (routes from the `@portal/routes` virtual module).
- **Components/hooks** — `Link`, `Head`, `useIsActive`, `useSearchParams` from `@interchained/portal-react`.
- **Vite** — `portalPlugin()` from `@interchained/portal-core/vite`.
- **CLI** — `portal dev | build | preview | serve | audit | improve | guard`.

---

## Quickstart

```bash
cd apps/nedb-studio
npm install
cp .env.example .env        # optional — leave blank to run in mock mode

npm run dev                 # Portal app on :3000 + AiAssist API server on :3001
```

- `npm run dev` runs the Portal dev server **and** the Express API server (which holds the AiAssist key) together. Vite proxies `/api/*` → `:3001`.
- Production: `npm run build` then `npm start` (one Express process serves `dist/` + `/api`).

---

## AiAssist (live mode)

NEDB Studio uses AiAssist Secure / AiAS as the **only** AI gateway — it never calls OpenAI/Anthropic/Groq/Gemini directly. Set these server-side:

```env
AIASSIST_BASE_URL=https://api.aiassist.net
AIASSIST_API_KEY=your_key_here
AIASSIST_DEFAULT_PROVIDER=anthropic
AIASSIST_DEFAULT_MODEL=claude-sonnet-4-6
```

- The key is read **only** by the server (`server.ts` / `src/server`). It is never sent to the browser.
- Provider/model lists come from `GET /v1/providers` (bearer auth, server-side) and populate the selectors + the marquee.
- `X-AiAssist-Provider` is sent on **every** request; the selected model is sent in the body.
- Pipeline: prompt → **runner** (fast generation) → **sentinel** (validate/repair) → **Zod** (`validateScaffold`) → render. Model output is extracted with KeyStone-Lite **sentinel blocks** (`<<<SCAFFOLD>>>` / `<<<NQL>>>`, content taken verbatim) so code/quotes/newlines never break parsing.
- **Mock templates are the no-credentials demo only.** In live mode there is no silent fallback — if generation fails, the studio surfaces the real error instead of quietly serving a mock.

## Mock mode

No credentials? Everything still works. The studio serves deterministic templates — **Contractor CRM, Salon booking, AI agent memory store, Marketplace,** and a generic fallback — matched to your prompt by keyword. Schema graph, artifacts, and export are all functional offline.

---

## Routes

| Route | Purpose |
| --- | --- |
| `/` | Landing — hero, example prompts, feature grid |
| `/studio` | Three-panel workspace: prompt + provider/model controls · schema graph **+ NL→NQL query console** · artifact tabs |
| `/about` | The NEDB engine: replay-protected log, MVCC time-travel, relations, Cascade compression, Merkle roots |
| `/docs` | Install (npm/PyPI), Python/Node usage, AiAssist setup, mock mode, export |

## Project structure

```
apps/nedb-studio/
  app.contract.ts          Portal contract (v1.1)
  vite.config.ts           portalPlugin + /api dev proxy
  server.ts                Express: /api routes + serves dist in prod
  routes/                  index | studio | about | docs  (*.page.tsx)
  src/
    main.tsx               PortalProvider entry
    lib/                   types (Zod), scaffold builders, mock templates, api client
    server/                aiassist client, prompts, generate router
    components/            Nav, Marquee, PromptPanel, SchemaGraph, ArtifactTabs
```

## Artifacts produced

Schema JSON · Relations · Indexes · Seed Data · NQL Queries · Python · Node · README — each copyable and downloadable, plus a one-click full export (`*-nedb-scaffold.json`).

## Quality gates

`app.contract.ts` enables `forbidPlaceholderCopy`, `forbidUnreplacedTokens`, `requireMetaTitle/Description`, `requireH1`, `requirePrimaryCTA`, and zero broken links. Run `portal audit` to check, `portal improve` to fix, `portal guard` before applying patches. The AiAssist key is never exposed client-side; all generated scaffolds validate before render.

## License

Apache-2.0 · part of the NEDB / Interchained ecosystem.
