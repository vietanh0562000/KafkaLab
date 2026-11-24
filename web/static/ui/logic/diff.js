const done = (s) => !s.pending && !s.err;
const readKey = (r) => `${r.partition}:${r.offset}:${Boolean(r.redelivered)}`;

function newSends(prev, next, topic) {
  const seen = new Set((prev.producers || []).flatMap((p) => (p.sends || []).filter(done).map((s) => `${p.name}:${s.id}`)));
  return (next.producers || []).flatMap((p) => (p.sends || [])
    .filter((s) => done(s) && s.topic === topic && !seen.has(`${p.name}:${s.id}`))
    .map((s) => ({ kind: "send", from: p.name, to: s.partition, partition: s.partition, key: s.key })));
}

function newReads(prev, next, topic) {
  const before = new Map((prev.consumers || []).map((c) => [c.name, new Set((c.received || []).map(readKey))]));
  return (next.consumers || []).filter((c) => c.topic === topic).flatMap((c) => (c.received || [])
    .filter((r) => !(before.get(c.name) || new Set()).has(readKey(r)))
    .map((r) => ({ kind: "read", from: r.partition, to: c.name, partition: r.partition, key: r.key })));
}

// diffState turns two reads of the lab into what visibly moved between them.
export function diffState(prev, next, maxTokens = 12) {
  const out = { tokens: [], overflow: 0, changedPartitions: [], topicReset: false };
  if (!prev) return out;
  const was = prev.topic, now = next.topic;
  if (was && !(next.topics || []).some((t) => t.name === was.name)) out.topicReset = true;
  if (!was || !now || was.name !== now.name) return out;

  const ends = new Map(was.partitions.map((p) => [p.partition, p.end]));
  let grown = 0;
  for (const p of now.partitions) {
    const before = ends.get(p.partition);
    if (before === undefined || before < 0 || p.end < 0 || before === p.end) continue;
    out.changedPartitions.push(p.partition);
    if (p.end < before) out.topicReset = true;
    else grown += p.end - before;
  }
  if (out.topicReset) return out;

  const all = [...newSends(prev, next, now.name), ...newReads(prev, next, now.name)];
  out.tokens = all.slice(0, maxTokens);
  // The send buffer is short, so a large burst is counted from the log rather than from the buffer.
  out.overflow = Math.max(all.length, grown) - out.tokens.length;
  if (out.overflow < 0) out.overflow = 0;
  return out;
}
