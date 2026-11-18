import { Tweet, ThemeConfig } from "@/lib/types";
import { PipelineCtx, Tally, pMap, windowFor } from "@/lib/pipeline/index";

const MAX_PER_ACCOUNT = 40;
const MAX_PER_QUERY = 200;
const FETCH_CONCURRENCY = 4;

export async function fetchStage(
  ctx: PipelineCtx,
  theme: ThemeConfig
): Promise<Tweet[]> {
  const { sinceIso, untilIso } = windowFor(ctx.date);
  ctx.log(`[${theme.id}] fetch: window ${sinceIso} .. ${untilIso}`);

  const tally = new Tally();
  const accountBatches = await pMap(
    theme.accounts,
    async (handle) => {
      try {
        const tweets = await ctx.fetcher.userRecentTweets(handle, sinceIso);
        ctx.log(`[${theme.id}] fetch: @${handle} -> ${tweets.length}`);
        tally.success();
        return tweets.slice(0, MAX_PER_ACCOUNT);
      } catch (err) {
        ctx.log(`[${theme.id}] fetch: @${handle} FAILED: ${String(err)}`);
        tally.failure(err);
        return [];
      }
    },
    FETCH_CONCURRENCY
  );

  const queryBatches = await pMap(
    theme.searchQueries,
    async (query) => {
      try {
        const tweets = await ctx.fetcher.search(query, sinceIso, MAX_PER_QUERY);
        ctx.log(`[${theme.id}] fetch: "${query}" -> ${tweets.length}`);
        tally.success();
        return tweets;
      } catch (err) {
        ctx.log(`[${theme.id}] fetch: "${query}" FAILED: ${String(err)}`);
        tally.failure(err);
        return [];
      }
    },
    FETCH_CONCURRENCY
  );

  tally.assertSomeSucceeded(theme.id, "fetch", "account and search requests");

  // Window-filter, dedupe by id (summing folded-retweet amplification counts).
  const since = new Date(sinceIso).getTime();
  const until = new Date(untilIso).getTime();
  const byId = new Map<string, Tweet>();
  let outOfWindow = 0;
  for (const t of [...accountBatches.flat(), ...queryBatches.flat()]) {
    const ts = new Date(t.createdAt).getTime();
    if (ts < since || ts >= until) {
      outOfWindow++;
      continue;
    }
    const prior = byId.get(t.id);
    if (prior) {
      prior.amplification =
        (prior.amplification ?? 0) + (t.amplification ?? 0);
    } else {
      byId.set(t.id, { ...t });
    }
  }

  const deduped = [...byId.values()].sort(
    (a, b) => engagement(b) - engagement(a)
  );
  const capped = deduped.slice(0, theme.maxTweets);
  ctx.log(
    `[${theme.id}] fetch: ${byId.size} unique in window (${outOfWindow} outside), kept ${capped.length} (cap ${theme.maxTweets})`
  );
  return capped;
}

export function engagement(t: Tweet): number {
  return t.likeCount + 2 * t.retweetCount + 2 * t.quoteCount;
}
