import { MODEL_SMART } from "@/lib/claude";
import { safeTruncate } from "@/lib/text";
import { extractCanonicalIds } from "@/lib/links";
import { resolveAbstract } from "@/lib/abstracts";
import {
  Cluster,
  DigestItem,
  FilteredTweet,
  RankedCluster,
  ThemeConfig,
} from "@/lib/types";
import { PipelineCtx, Tally, pMap } from "@/lib/pipeline/index";
import { engagement } from "@/lib/pipeline/fetch";
import { rankClusters } from "@/lib/pipeline/rank";

const CONCURRENCY = 3;
const MAX_TWEETS_IN_PROMPT = 25;
const MAX_SOURCE_TWEET_LINKS = 10;

export async function summarizeStage(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  clusters: Cluster[],
  tweets: FilteredTweet[]
): Promise<DigestItem[]> {
  const ranked = rankClusters(theme, clusters, tweets);
  ctx.log(
    `[${theme.id}] summarize: top ${ranked.length} clusters (of ${clusters.length})`
  );
  const byId = new Map(tweets.map((t) => [t.id, t]));

  const tally = new Tally();
  const items = await pMap(
    ranked,
    async (cluster, i) => {
      try {
        const item = await summarizeCluster(ctx, theme, cluster, byId);
        ctx.log(
          `[${theme.id}] summarize: ${i + 1}/${ranked.length} "${item.headline.slice(0, 60)}"`
        );
        tally.success();
        return item;
      } catch (err) {
        ctx.log(
          `[${theme.id}] summarize: cluster "${cluster.key}" FAILED: ${String(err)}`
        );
        tally.failure(err);
        return null;
      }
    },
    CONCURRENCY
  );
  tally.assertSomeSucceeded(theme.id, "summarize", "cluster summaries");
  return items.filter((i): i is DigestItem => i !== null);
}

async function summarizeCluster(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  cluster: RankedCluster,
  byId: Map<string, FilteredTweet>
): Promise<DigestItem> {
  const members = cluster.tweetIds
    .map((id) => byId.get(id))
    .filter((t): t is FilteredTweet => t !== undefined)
    .sort((a, b) => engagement(b) - engagement(a));

  const paper =
    theme.fetchAbstracts && !ctx.mock
      ? await resolveAbstract(cluster.canonicalIds, cluster.urls)
      : null;

  const tweetListing = members
    .slice(0, MAX_TWEETS_IN_PROMPT)
    .map(
      (t) =>
        `- @${t.authorHandle} (${engagement(t)} engagement): ${safeTruncate(t.text, 500).replace(/\n/g, " ")}`
    )
    .join("\n");

  const paperSection = paper
    ? `\n\nPaper title: ${paper.title ?? "(unknown)"}\nAbstract (from ${paper.source}):\n${paper.abstract ?? "(unavailable)"}`
    : theme.fetchAbstracts
      ? "\n\nAbstract unavailable — base the summary only on the tweet discussion and hedge accordingly."
      : "";

  const linkHint = theme.primaryLinkHosts?.length
    ? `\nPrefer primaryLinks whose host matches one of: ${theme.primaryLinkHosts.join(", ")} — but only URLs that appear in the list above.`
    : "";

  const categoryIds = theme.categories?.map((c) => c.id);
  const categoryField = categoryIds ? ', "category": "..."' : "";
  const categoryHint = categoryIds
    ? `\ncategory must be exactly one of: ${categoryIds.join(", ")}.`
    : "";

  const system = `You write one item for a daily "${theme.label}" digest built from Twitter discussion.
Style: ${theme.summaryStyle}
Respond ONLY with JSON:
{"headline": "...", "summary": "...", "primaryLinks": [{"url": "...", "title": "..."}]${categoryField}}
primaryLinks must be chosen from the URLs provided — never invent URLs. headline is a specific, factual title (max 100 chars).${categoryHint}${linkHint}`;

  const prompt = `Topic: ${cluster.label}

Tweets discussing it:
${tweetListing}

URLs appearing in the discussion:
${cluster.urls.slice(0, 20).join("\n") || "(none)"}${paperSection}`;

  const result = await ctx.llm.json<{
    headline: string;
    summary: string;
    primaryLinks: { url: string; title: string }[];
    category?: string;
  }>({
    model: MODEL_SMART,
    system,
    prompt,
    maxTokens: 2000,
    mock: () => ({
      headline: `[mock] ${cluster.label.slice(0, 80)}`,
      summary: members[0]?.text.slice(0, 300) ?? "(no tweets)",
      primaryLinks: cluster.urls.slice(0, 2).map((u) => ({ url: u, title: u })),
      category: categoryIds?.[categoryIds.length - 1],
    }),
  });

  // Guard against invented links: keep only URLs actually in the cluster.
  const known = new Set(cluster.urls);
  const primaryLinks = (result.primaryLinks ?? []).filter((l) => known.has(l.url));

  // The model paraphrases (and sometimes garbles) link titles. When the
  // abstract resolver gave us the paper's exact title, use it verbatim for any
  // link that points at that paper.
  if (paper?.title) {
    for (const link of primaryLinks) {
      const ids = extractCanonicalIds([link.url], theme.canonicalPatterns);
      if (ids.some((id) => cluster.canonicalIds.includes(id))) {
        link.title = paper.title;
      }
    }
  }

  // Unknown category ids fall back to "other" (or drop if no such category).
  const category = categoryIds?.includes(result.category ?? "")
    ? result.category
    : categoryIds?.includes("other")
      ? "other"
      : undefined;

  return {
    headline: result.headline,
    summary: result.summary,
    ...(categoryIds ? { category } : {}),
    primaryLinks,
    sourceTweets: members.slice(0, MAX_SOURCE_TWEET_LINKS).map((t) => ({
      url: `https://x.com/${t.authorHandle}/status/${t.id}`,
      authorHandle: t.authorHandle,
    })),
    stats: {
      tweetCount: cluster.tweetCount,
      distinctAuthors: cluster.distinctAuthors,
      engagement: cluster.engagement,
    },
  };
}
