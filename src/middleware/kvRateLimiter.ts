// /cloudflare/src/middleware/kvRateLimiter.ts
/**
 * KV-backed approx sliding-window rate limiter for Cloudflare Workers
 *
 * - Uses a KV binding (RATE_LIMIT_KV) to store per-second buckets:
 *    key: `rl:{scope}:{epochSec}`
 *    value: numeric count (string)
 * - On each request:
 *    1) read current second bucket -> increment -> put with TTL (windowSec*2)
 *    2) read last `windowSec` buckets and sum them
 *    3) allow if sum < limit
 *
 * Caveats:
 * - Cloudflare KV is eventually consistent and not atomic. Concurrent increments can lose / race.
 * - This is an **approximate** limiter suitable for many production use-cases.
 * - For strict exact counters use a Durable Object (recommended when accuracy matters).
 *
 * ENV / binding:
 * - Expects a KV binding named RATE_LIMIT_KV in worker env.
 *
 * Usage:
 * import { kvRateLimit } from "./middleware/kvRateLimiter";
 * const allowed = await kvRateLimit(env.RATE_LIMIT_KV, "ip:1.2.3.4", { windowSec: 60, limit: 60 });
 */

type KV = KVNamespace;

export interface KVRateLimitOpts {
  windowSec?: number;   // sliding window size
  limit?: number;       // max allowed in window
  bucketKeyPrefix?: string; // prefix for keys; default 'rl'
  // per-bucket TTL to keep the entries in KV; used as put options
  bucketTTL?: number;   // seconds; default windowSec * 2
}

/**
 * Helper: build key for bucket
 */
function bucketKey(prefix: string, scope: string, epochSec: number) {
  // keep keys compact
  return `${prefix}:${scope}:${epochSec}`;
}

/**
 * Main function: returns true if request allowed and (optionally) current count
 */
export async function kvRateLimit(
  kv: KV,
  scope: string,
  opts: KVRateLimitOpts = {}
): Promise<{ allowed: boolean; total: number; limit: number }> {
  const windowSec = opts.windowSec ?? 60;
  const limit = opts.limit ?? 60;
  const prefix = opts.bucketKeyPrefix ?? "rl";
  const bucketTTL = opts.bucketTTL ?? windowSec * 2; // seconds

  const nowMs = Date.now();
  const nowSec = Math.floor(nowMs / 1000);

  const currentKey = bucketKey(prefix, scope, nowSec);

  // 1) Read current bucket count
  // Note: read -> write is not atomic. This is an eventually-consistent approximate approach.
  let currentVal = 0;
  try {
    const currentRaw = await kv.get(currentKey);
    if (currentRaw) currentVal = parseInt(currentRaw as string) || 0;
  } catch (e) {
    // KV read failed -> conservative fallback: allow (or deny) — we choose allow here but log
    console.warn("kvRateLimit: kv.get error", String(e));
  }

  // increment locally
  const newCurrentVal = currentVal + 1;

  // 2) Write back incremented bucket with TTL
  try {
    // set the new count as a string; expiration in seconds
    await kv.put(currentKey, String(newCurrentVal), { expirationTtl: bucketTTL });
  } catch (e) {
    console.warn("kvRateLimit: kv.put error", String(e));
    // If we cannot write to KV, we fall through to perform the best-effort check
  }

  // 3) Sum last windowSec buckets
  const keysToRead: string[] = [];
  for (let i = 0; i < windowSec; i++) {
    const sec = nowSec - i;
    keysToRead.push(bucketKey(prefix, scope, sec));
  }

  // Read multiple keys (note: KV supports list/get many by issuing individual gets — no batch API)
  let total = 0;
  try {
    // parallelize gets but limit concurrency if you expect big windowSec
    const readPromises = keysToRead.map((k) => kv.get(k));
    const results = await Promise.all(readPromises);
    for (const r of results) {
      if (r != null) total += parseInt(r as string) || 0;
    }
  } catch (e) {
    console.warn("kvRateLimit: bucket reads failed", String(e));
    // If reads fail, fall back to using current bucket only (conservative)
    total = newCurrentVal;
  }

  const allowed = total <= limit;
  return { allowed, total, limit };
}
