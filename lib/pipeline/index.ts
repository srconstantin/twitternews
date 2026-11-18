import { THEMES } from "@/config/themes";
import { LlmClient } from "@/lib/claude";
import { TweetFetcher } from "@/lib/fetcher/types";
import { Storage, digestKey, indexKey, stageKey } from "@/lib/storage";
import {
  Digest,
  DigestIndexEntry,
  DigestItem,
  Stage,
  STAGES,
  ThemeConfig,
} from "@/lib/types";
import { fetchStage } from "@/lib/pipeline/fetch";
import { filterStage } from "@/lib/pipeline/filter";
import { clusterStage } from "@/lib/pipeline/cluster";
import { summarizeStage } from "@/lib/pipeline/summarize";

export interface PipelineCtx {
  date: string; // YYYY-MM-DD — the digest date; the tweet window is the 24h ending at 11:00 UTC on this date
  storage: Storage;
  fetcher: TweetFetcher;
  llm: LlmClient;
  mock: boolean;
  forceFrom?: Stage;
  log: (msg: string) => void;
}

const STAGE_FILES: Record<Stage, string> = {
  fetch: "01-raw",
  filter: "02-filtered",
  cluster: "03-clusters",
  summarize: "04-digest-items",
};

function shouldForce(ctx: PipelineCtx, stage: Stage): boolean {
  if (!ctx.forceFrom) return false;
  return STAGES.indexOf(stage) >= STAGES.indexOf(ctx.forceFrom);
}

// Run one stage with checkpointing: skip (and return the stored result) when a
// checkpoint exists, unless --force-from covers this stage.
async function runStage<T>(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  stage: Stage,
  run: () => Promise<T>
): Promise<T> {
  const key = stageKey(ctx.date, theme.id, STAGE_FILES[stage]);
  if (!shouldForce(ctx, stage)) {
    const cached = await ctx.storage.getJson<T>(key);
    if (cached !== null) {
      ctx.log(`[${theme.id}] ${stage}: checkpoint exists, skipping`);
      return cached;
    }
  }
  const started = Date.now();
  const result = await run();
  await ctx.storage.putJson(key, result);
  ctx.log(
    `[${theme.id}] ${stage}: done in ${((Date.now() - started) / 1000).toFixed(1)}s`
  );
  return result;
}

// Stages tolerate individual failures (one bad query, one malformed LLM reply)
// so a partial digest still ships. But when EVERY attempt in a stage fails the
// cause is systemic — exhausted credits, rejected credentials, an outage — and
// carrying on would publish an empty digest and save an empty checkpoint that
// later runs skip over. A tally lets a stage tell those two cases apart.
export class StageFailedError extends Error {}

export class Tally {
  private ok = 0;
  private failed = 0;
  private lastError: unknown;

  success(): void {
    this.ok++;
  }
  failure(err: unknown): void {
    this.failed++;
    this.lastError = err;
  }
  // Throws when there were attempts and none succeeded. Zero attempts (nothing
  // to do) is not a failure.
  assertSomeSucceeded(themeId: string, stage: Stage, what: string): void {
    if (this.failed > 0 && this.ok === 0) {
      throw new StageFailedError(
        `[${themeId}] ${stage}: all ${this.failed} ${what} failed; last error: ${String(this.lastError)}`
      );
    }
  }
}

export async function runThemePipeline(
  ctx: PipelineCtx,
  theme: ThemeConfig
): Promise<DigestItem[]> {
  const raw = await runStage(ctx, theme, "fetch", () => fetchStage(ctx, theme));
  const filtered = await runStage(ctx, theme, "filter", () =>
    filterStage(ctx, theme, raw)
  );
  const clusters = await runStage(ctx, theme, "cluster", () =>
    clusterStage(ctx, theme, filtered)
  );
  return runStage(ctx, theme, "summarize", () =>
    summarizeStage(ctx, theme, clusters, filtered)
  );
}

export async function runPipeline(
  ctx: PipelineCtx,
  themeIds?: string[]
): Promise<Digest> {
  const themes = THEMES.filter((t) => !themeIds || themeIds.includes(t.id));
  if (themes.length === 0) {
    throw new Error(`No themes matched ${JSON.stringify(themeIds)}`);
  }

  const digest: Digest = {
    date: ctx.date,
    generatedAt: new Date().toISOString(),
    themes: {},
  };
  // A theme whose stage failed outright is left out of the digest (its previous
  // content for this date, if any, is kept) and reported at the end, so one
  // broken theme doesn't stop the others from publishing.
  const failures: string[] = [];
  for (const theme of themes) {
    ctx.log(`=== theme: ${theme.id} ===`);
    try {
      digest.themes[theme.id] = await runThemePipeline(ctx, theme);
    } catch (err) {
      if (!(err instanceof StageFailedError)) throw err;
      ctx.log(`PIPELINE FAILURE ${err.message}`);
      failures.push(err.message);
    }
  }
  if (failures.length === themes.length) {
    throw new StageFailedError(
      `digest ${ctx.date} not written, every theme failed:\n${failures.join("\n")}`
    );
  }

  // Merge with any themes already present for this date (per-theme runs).
  const existing = await ctx.storage.getJson<Digest>(digestKey(ctx.date));
  if (existing) {
    digest.themes = { ...existing.themes, ...digest.themes };
  }
  await ctx.storage.putJson(digestKey(ctx.date), digest);

  const index =
    (await ctx.storage.getJson<DigestIndexEntry[]>(indexKey())) ?? [];
  const entry: DigestIndexEntry = {
    date: ctx.date,
    themes: Object.fromEntries(
      Object.entries(digest.themes).map(([id, items]) => [id, items.length])
    ),
  };
  const next = [entry, ...index.filter((e) => e.date !== ctx.date)].sort(
    (a, b) => b.date.localeCompare(a.date)
  );
  await ctx.storage.putJson(indexKey(), next);

  ctx.log(
    `digest ${ctx.date} written: ` +
      Object.entries(digest.themes)
        .map(([id, items]) => `${id}=${items.length}`)
        .join(", ")
  );
  if (failures.length > 0) {
    throw new StageFailedError(
      `digest ${ctx.date} written without ${failures.length} failed theme(s):\n${failures.join("\n")}`
    );
  }
  return digest;
}

// The tweet window for a digest date: 24h ending at the cron hour (11:00 UTC).
export function windowFor(date: string): { sinceIso: string; untilIso: string } {
  const end = new Date(`${date}T11:00:00Z`).getTime();
  return {
    sinceIso: new Date(end - 24 * 60 * 60 * 1000).toISOString(),
    untilIso: new Date(end).toISOString(),
  };
}

// Small concurrency-limited map used by several stages.
export async function pMap<T, R>(
  items: T[],
  fn: (item: T, index: number) => Promise<R>,
  concurrency: number
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let cursor = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      while (cursor < items.length) {
        const i = cursor++;
        results[i] = await fn(items[i], i);
      }
    }
  );
  await Promise.all(workers);
  return results;
}
