// A source that fails while a broker is down keeps showing its last answer instead of vanishing.
// Groups are cached per topic because their commit offsets belong to one topic.
export function mergeLastGood(next, cache, topic) {
  const out = { ...next };
  for (const k of ["topics", "brokers", "groups"]) {
    const slot = k === "groups" ? `groups:${topic}` : k;
    if (next.errors && next.errors[k]) out[k] = cache[slot] || [];
    else cache[slot] = out[k] = next[k] || [];
  }
  return out;
}
