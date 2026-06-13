import { Router } from "express";

import { finalizeScaffold } from "../lib/scaffold";
import { matchTemplate, MOCK_PROVIDERS } from "../lib/mock";
import { heuristicNlToNql } from "../lib/nql";
import { validateScaffold } from "../lib/types";
import { extractBlock } from "./blocks";
import { chat, defaults, hasCredentials, listProviders } from "./aiassist";
import {
  extractJson,
  extractNql,
  nqlMessages,
  nqlSystem,
  runnerMessages,
  runnerSystem,
  sentinelMessages,
  sentinelSystem,
} from "./prompts";

/**
 * /api router. The AiAssist key never leaves this process. Every path degrades
 * gracefully to deterministic mock output, so the studio is always usable.
 */
export const api = Router();

/**
 * Pull the scaffold JSON out of a raw completion: KeyStone-Lite sentinel block
 * (<<<SCAFFOLD>>>…<<<END>>>) first, then a brace-slice fallback (extractJson).
 * Throws if neither yields parseable JSON (caller falls back to a mock template).
 */
function scaffoldJson(raw: string): unknown {
  const block = extractBlock(raw, "SCAFFOLD") ?? raw;
  try {
    return JSON.parse(block);
  } catch {
    return extractJson(block);
  }
}

api.get("/status", (_req, res) => {
  const d = defaults();
  res.json({ mode: hasCredentials() ? "live" : "mock", defaultProvider: d.provider, defaultModel: d.model });
});

// Providers + models for the UI selectors and marquee (bearer auth, server-side).
api.get("/providers", async (_req, res) => {
  if (!hasCredentials()) {
    res.json({ ...MOCK_PROVIDERS, mode: "mock" });
    return;
  }
  try {
    const result = await listProviders();
    res.json({ ...result, mode: "live" });
  } catch (err) {
    // Credentials exist, so stay LIVE — degrade to the configured default
    // provider/model so the selectors still work. Never relabel as "mock".
    const d = defaults();
    res.json({
      defaultProvider: d.provider,
      providers: [{ id: d.provider, label: d.provider, isDefault: true, models: [{ id: d.model, name: d.model }] }],
      mode: "live",
      error: String(err),
    });
  }
});

api.post("/generate", async (req, res) => {
  const prompt = String(req.body?.prompt ?? "").trim();
  const provider = req.body?.provider ? String(req.body.provider) : undefined;
  const model = req.body?.model ? String(req.body.model) : undefined;

  if (!prompt) {
    res.status(400).json({ error: "prompt is required" });
    return;
  }

  // Demo mode — ONLY when there are no AiAssist credentials. With credentials,
  // generation is always live and failures are surfaced (never a silent mock).
  if (!hasCredentials()) {
    res.json({
      scaffold: matchTemplate(prompt),
      mode: "mock",
      notes: ["Demo mode (no AiAssist credentials) — deterministic template. Add a key for live AI generation."],
    });
    return;
  }

  const notes: string[] = [];
  try {
    // ── Runner: fast first-pass generation ──────────────────────────────────
    const raw = await chat({
      messages: [{ role: "system", content: runnerSystem() }, ...runnerMessages(prompt)],
      model,
      provider,
      temperature: 0.2,
      maxTokens: 4000,
    });
    let candidate = scaffoldJson(raw);
    let result = validateScaffold(candidate);

    // ── Sentinel: validate / repair if the runner output is invalid ─────────
    if (!result.ok) {
      notes.push("Runner output failed validation; sentinel repaired it.");
      const repaired = await chat({
        messages: [
          { role: "system", content: sentinelSystem() },
          ...sentinelMessages(prompt, JSON.stringify(candidate), result.errors ?? []),
        ],
        model,
        provider,
        temperature: 0,
        maxTokens: 4000,
      });
      candidate = scaffoldJson(repaired);
      result = validateScaffold(candidate);
    }

    // Live mode never silently falls back to a mock. If it still won't validate
    // after the sentinel repair pass, surface the real error.
    if (!result.ok || !result.scaffold) {
      res.status(422).json({
        error: "Generation didn't produce a valid schema",
        details: [...notes, ...(result.errors ?? []).slice(0, 8)],
      });
      return;
    }

    // Fill any server-owned artifacts (snippets/README) the model left empty.
    const scaffold = finalizeScaffold(result.scaffold);
    res.json({ scaffold, mode: "live", provider, model, notes });
  } catch (err) {
    res.status(502).json({ error: "AiAssist generation error", details: [String(err)] });
  }
});

// Natural language → NQL (the query console). Compilation is server-side via
// AiAssist; execution happens in the browser against the scaffold's seed data.
api.post("/nql", async (req, res) => {
  const prompt = String(req.body?.prompt ?? "").trim();
  const schema = req.body?.schema;
  if (!prompt || !schema?.collections?.length) {
    res.status(400).json({ error: "prompt and schema are required" });
    return;
  }
  // Demo mode only when no credentials: heuristic NL→NQL compiler.
  if (!hasCredentials()) {
    res.json({ nql: heuristicNlToNql(prompt, schema), mode: "mock" });
    return;
  }
  try {
    const raw = await chat({
      messages: [{ role: "system", content: nqlSystem(schema) }, ...nqlMessages(prompt)],
      temperature: 0,
      maxTokens: 160,
    });
    const nql = extractNql(raw);
    if (!/^from\s/i.test(nql)) {
      res.status(422).json({ error: "Model did not return a valid NQL query", details: [nql.slice(0, 200)] });
      return;
    }
    res.json({ nql, mode: "live" });
  } catch (err) {
    res.status(502).json({ error: "AiAssist NQL compile error", details: [String(err)] });
  }
});
