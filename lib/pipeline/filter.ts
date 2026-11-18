import { MODEL_FAST } from "@/lib/claude";
import { safeTruncate } from "@/lib/text";
import { extractCanonicalIds, normalizeUrl } from "@/lib/links";
import { FilteredTweet, ThemeConfig, Tweet } from "@/lib/types";
import { PipelineCtx, Tally, pMap } from "@/lib/pipeline/index";

const BATCH_SIZE = 30;
const CONCURRENCY = 5;

export async function filterStage(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  tweets: Tweet[]
): Promise<FilteredTweet[]> {
  // Deterministic link work first — no LLM involved.
  const annotated: FilteredTweet[] = tweets.map((t) => ({
    ...t,
    normalizedUrls: t.urls
      .map(normalizeUrl)
      .filter((u): u is string => u !== null),
    canonicalIds: extractCanonicalIds(t.urls, theme.canonicalPatterns),
  }));

  const autoKeep: FilteredTweet[] = [];
  const needsClassification: FilteredTweet[] = [];
  for (const t of annotated) {
    if (theme.clusterStrategy === "canonical-link") {
      if (t.canonicalIds.length > 0) {
        // Having a paper link IS the inclusion criterion — skip the classifier.
        autoKeep.push(t);
      } else if (t.normalizedUrls.length > 0) {
        needsClassification.push(t);
      }
      // No URLs at all: nothing to anchor a cluster on — drop silently.
    } else {
      needsClassification.push(t);
    }
  }
  ctx.log(
    `[${theme.id}] filter: ${autoKeep.length} auto-kept by canonical link, ${needsClassification.length} to classify`
  );

  const batches: FilteredTweet[][] = [];
  for (let i = 0; i < needsClassification.length; i += BATCH_SIZE) {
    batches.push(needsClassification.slice(i, i + BATCH_SIZE));
  }

  let kept = autoKeep.length;
  const tally = new Tally();
  const classified = await pMap(
    batches,
    async (batch, bi) => {
      const verdicts = await classifyBatch(ctx, theme, batch, tally);
      const keptTweets = batch.filter((_, i) => verdicts[i]);
      kept += keptTweets.length;
      ctx.log(
        `[${theme.id}] filter: batch ${bi + 1}/${batches.length} kept ${keptTweets.length}/${batch.length} (total ${kept})`
      );
      return keptTweets;
    },
    CONCURRENCY
  );
  tally.assertSomeSucceeded(theme.id, "filter", "classification batches");

  return [...autoKeep, ...classified.flat()];
}

async function classifyBatch(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  batch: FilteredTweet[],
  tally: Tally
): Promise<boolean[]> {
  const listing = batch
    .map(
      (t, i) =>
        `${i + 1}. @${t.authorHandle}: ${safeTruncate(t.text, 400).replace(/\n/g, " ")}` +
        (t.normalizedUrls.length ? ` [links: ${t.normalizedUrls.slice(0, 3).join(" ")}]` : "")
    )
    .join("\n");

  const system = `You classify tweets for a themed digest called "${theme.label}".
INCLUDE: ${theme.inclusionCriteria}
EXCLUDE: ${theme.exclusionCriteria}
Respond ONLY with a JSON array like [{"n": 1, "keep": true}, ...] covering every tweet number exactly once.`;

  try {
    const result = await ctx.llm.json<{ n: number; keep: boolean }[]>({
      model: MODEL_FAST,
      system,
      prompt: `Tweets:\n${listing}`,
      maxTokens: 2000,
      mock: () => batch.map((_, i) => ({ n: i + 1, keep: true })),
    });
    const keepByN = new Map(result.map((r) => [r.n, r.keep]));
    tally.success();
    // Fail-open per tweet: missing verdicts count as keep.
    return batch.map((_, i) => keepByN.get(i + 1) ?? true);
  } catch (err) {
    // Fail-open per batch: ranking will bury junk; silent drops are worse.
    ctx.log(`[${theme.id}] filter: batch classification FAILED (keeping all): ${String(err)}`);
    tally.failure(err);
    return batch.map(() => true);
  }
}
