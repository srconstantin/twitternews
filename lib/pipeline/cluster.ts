import { MODEL_FAST, MODEL_SMART } from "@/lib/claude";
import { safeTruncate } from "@/lib/text";
import { Cluster, FilteredTweet, ThemeConfig } from "@/lib/types";
import { PipelineCtx, Tally } from "@/lib/pipeline/index";
import { engagement } from "@/lib/pipeline/fetch";

const TOPIC_BATCH_SIZE = 40;

export async function clusterStage(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  tweets: FilteredTweet[]
): Promise<Cluster[]> {
  const clusters =
    theme.clusterStrategy === "canonical-link"
      ? clusterByCanonicalLink(tweets)
      : await clusterByTopic(ctx, theme, tweets);
  ctx.log(`[${theme.id}] cluster: ${clusters.length} clusters from ${tweets.length} tweets`);
  return clusters;
}

// Deterministic grouping: one cluster per canonical id; tweets with no
// extractable id group by normalized URL instead. A tweet referencing two
// papers legitimately appears in both clusters.
function clusterByCanonicalLink(tweets: FilteredTweet[]): Cluster[] {
  const byKey = new Map<string, Cluster>();
  const add = (key: string, label: string, t: FilteredTweet) => {
    let c = byKey.get(key);
    if (!c) {
      c = { key, label, tweetIds: [], canonicalIds: [], urls: [] };
      byKey.set(key, c);
    }
    c.tweetIds.push(t.id);
    for (const cid of t.canonicalIds) {
      if (!c.canonicalIds.includes(cid)) c.canonicalIds.push(cid);
    }
    for (const u of t.normalizedUrls) {
      if (!c.urls.includes(u)) c.urls.push(u);
    }
  };

  for (const t of tweets) {
    if (t.canonicalIds.length > 0) {
      for (const cid of t.canonicalIds) add(cid, cid, t);
    } else {
      for (const url of t.normalizedUrls) add(`url:${url}`, url, t);
    }
  }
  return [...byKey.values()];
}

interface TopicLabel {
  slug: string;
  description: string;
}

// Incremental LLM event labeling: engagement-first batches; each call sees the
// growing label list and assigns existing-or-new labels; a final merge pass
// unifies labels that describe the same event.
async function clusterByTopic(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  tweets: FilteredTweet[]
): Promise<Cluster[]> {
  const sorted = [...tweets].sort((a, b) => engagement(b) - engagement(a));
  const labels: TopicLabel[] = [];
  const assignment = new Map<string, string>(); // tweetId -> slug
  const tally = new Tally();

  for (let i = 0; i < sorted.length; i += TOPIC_BATCH_SIZE) {
    const batch = sorted.slice(i, i + TOPIC_BATCH_SIZE);
    const batchNo = i / TOPIC_BATCH_SIZE + 1;
    try {
      const result = await assignLabels(ctx, theme, labels, batch);
      for (const nl of result.newLabels ?? []) {
        if (nl.slug && !labels.some((l) => l.slug === nl.slug)) labels.push(nl);
      }
      for (const a of result.assignments ?? []) {
        const t = batch[a.n - 1];
        if (t && a.slug) assignment.set(t.id, a.slug);
      }
      ctx.log(
        `[${theme.id}] cluster: batch ${batchNo}, ${labels.length} labels so far`
      );
      tally.success();
    } catch (err) {
      ctx.log(`[${theme.id}] cluster: batch ${batchNo} FAILED (tweets unassigned): ${String(err)}`);
      tally.failure(err);
    }
  }
  tally.assertSomeSucceeded(theme.id, "cluster", "labeling batches");

  // Merge labels that describe the same story. Up to two rounds: the second
  // catches duplicates that survive the first. Groups union transitively
  // (["a","b"] + ["b","c"] -> one story).
  const canonicalSlug = new Map<string, string>();
  const resolve = (slug: string): string => {
    let s = slug;
    while (canonicalSlug.has(s) && canonicalSlug.get(s) !== s) {
      s = canonicalSlug.get(s)!;
    }
    return s;
  };
  let activeLabels = labels;
  for (let round = 0; round < 2; round++) {
    const merged = await mergeLabels(ctx, activeLabels);
    if (merged.length === 0) break;
    for (const group of merged) {
      const roots = [...new Set(group.map(resolve))];
      for (const r of roots) canonicalSlug.set(r, roots[0]);
    }
    const survivors = new Set(activeLabels.map((l) => resolve(l.slug)));
    activeLabels = activeLabels.filter((l) => survivors.has(l.slug) && resolve(l.slug) === l.slug);
  }

  const byId = new Map(tweets.map((t) => [t.id, t]));
  const byKey = new Map<string, Cluster>();
  for (const [tweetId, rawSlug] of assignment) {
    const slug = resolve(rawSlug);
    const t = byId.get(tweetId);
    if (!t) continue;
    let c = byKey.get(slug);
    if (!c) {
      const label = labels.find((l) => l.slug === slug);
      c = {
        key: slug,
        label: label?.description ?? slug,
        tweetIds: [],
        canonicalIds: [],
        urls: [],
      };
      byKey.set(slug, c);
    }
    c.tweetIds.push(tweetId);
    for (const u of t.normalizedUrls) {
      if (!c.urls.includes(u)) c.urls.push(u);
    }
  }
  return [...byKey.values()];
}

async function assignLabels(
  ctx: PipelineCtx,
  theme: ThemeConfig,
  labels: TopicLabel[],
  batch: FilteredTweet[]
): Promise<{
  assignments: { n: number; slug: string }[];
  newLabels: TopicLabel[];
}> {
  const labelList =
    labels.length > 0
      ? labels.map((l) => `- ${l.slug}: ${l.description}`).join("\n")
      : "(none yet)";
  const listing = batch
    .map((t, i) => `${i + 1}. ${safeTruncate(t.text, 300).replace(/\n/g, " ")}`)
    .join("\n");

  const system = `You group tweets about ${theme.label.toLowerCase()} events by the specific real-world event they discuss.
Existing event labels:
${labelList}

For each tweet, assign the slug of an existing label when the tweet is about that event; otherwise invent a new label (short-kebab-case slug + one-line description) and use it. Tweets about the same event must share one label even when they take different angles on it.
Respond ONLY with JSON: {"assignments": [{"n": 1, "slug": "..."}], "newLabels": [{"slug": "...", "description": "..."}]}`;

  return ctx.llm.json({
    model: MODEL_FAST,
    system,
    prompt: `Tweets:\n${listing}`,
    maxTokens: 3000,
    mock: () => mockAssign(batch),
  });
}

// Mock topic assignment: group by the first URL's host (deterministic, no LLM).
function mockAssign(batch: FilteredTweet[]): {
  assignments: { n: number; slug: string }[];
  newLabels: TopicLabel[];
} {
  const assignments: { n: number; slug: string }[] = [];
  const newLabels = new Map<string, TopicLabel>();
  batch.forEach((t, i) => {
    let slug = "general-discussion";
    try {
      if (t.normalizedUrls[0]) {
        slug = new URL(t.normalizedUrls[0]).hostname.replace(/\W+/g, "-");
      }
    } catch {
      // keep default
    }
    assignments.push({ n: i + 1, slug });
    newLabels.set(slug, { slug, description: `Mock topic: ${slug}` });
  });
  return { assignments, newLabels: [...newLabels.values()] };
}

async function mergeLabels(
  ctx: PipelineCtx,
  labels: TopicLabel[]
): Promise<string[][]> {
  if (labels.length < 2) return [];
  try {
    const result = await ctx.llm.json<{ merges: string[][] }>({
      model: MODEL_SMART,
      system:
        'You consolidate event labels for a daily digest. Given labels with one-line descriptions, return groups of slugs that cover the SAME underlying story — including different facets, reactions, follow-ups, or sub-events of one story (e.g. "colombia-earthquake-response", "colombia-earthquake-donations", and "colombia-earthquake-backlash" are one story; "fed-rate-decision" and "fomc-meeting" are one story). Merge whenever two labels would read as duplicate coverage of the same story in the same day\'s digest, and WHEN IN DOUBT, MERGE — a digest that folds two borderline-related labels into one item beats one that repeats the same story twice. Keep labels separate only when they are clearly unrelated stories. Respond ONLY with JSON: {"merges": [["slug-a", "slug-b", ...], ...]}. Return {"merges": []} if none.',
      prompt: labels.map((l) => `- ${l.slug}: ${l.description}`).join("\n"),
      // Generous cap: the model reasons over 200+ labels before emitting JSON;
      // a low cap gets consumed by reasoning and yields an empty response.
      maxTokens: 16000,
      mock: () => ({ merges: [] }),
    });
    if (result.merges.length > 0) {
      ctx.log(`cluster: merged ${result.merges.length} label groups`);
    }
    return result.merges;
  } catch (err) {
    ctx.log(`cluster: label merge FAILED (skipping merges): ${String(err)}`);
    return [];
  }
}
