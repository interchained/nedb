import type { GenerateResponse, ProvidersPayload, StudioStatus } from "./types";

/**
 * Browser → server API. The browser only ever talks to our own /api routes
 * (proxied to the Express server in dev). It never sees the AiAssist key.
 */

async function errorMessage(res: Response, fallback: string): Promise<string> {
  try {
    const data = await res.json();
    if (data?.error) return data.details?.length ? `${data.error}: ${data.details.join("; ")}` : data.error;
  } catch {
    /* response wasn't JSON */
  }
  return `${fallback} (${res.status})`;
}

export async function getStatus(): Promise<StudioStatus> {
  const res = await fetch("/api/status");
  if (!res.ok) throw new Error(`/api/status -> ${res.status}`);
  return (await res.json()) as StudioStatus;
}

export async function getProviders(): Promise<ProvidersPayload> {
  const res = await fetch("/api/providers");
  if (!res.ok) throw new Error(`/api/providers -> ${res.status}`);
  return (await res.json()) as ProvidersPayload;
}

export async function generate(
  prompt: string,
  provider?: string,
  model?: string,
): Promise<GenerateResponse> {
  const res = await fetch("/api/generate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt, provider, model }),
  });
  if (!res.ok) {
    throw new Error(await errorMessage(res, "Generation failed"));
  }
  return (await res.json()) as GenerateResponse;
}

export interface CompileNqlResult {
  nql: string;
  mode: "mock" | "live";
  error?: string;
}

/** Natural language → NQL. Schema is {collections, relations, indexes}. */
export async function compileNql(prompt: string, schema: unknown): Promise<CompileNqlResult> {
  const res = await fetch("/api/nql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt, schema }),
  });
  if (!res.ok) {
    throw new Error(await errorMessage(res, "Query compile failed"));
  }
  return (await res.json()) as CompileNqlResult;
}
