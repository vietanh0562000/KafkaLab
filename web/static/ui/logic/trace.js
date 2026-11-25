const same = (a, b) => (a.key || "") === (b.key || "") && (a.value || "") === (b.value || "");

function traceSend({ topic, record, partition, producers, staleBefore }) {
  const at = (s) => new Date(s.at).getTime();
  const found = (producers || []).flatMap((p) => p.sends || []).filter((s) =>
    s.topic === topic && s.partition === partition && s.offset === record.offset &&
    !s.err && !s.pending && at(s) >= (staleBefore || 0));
  // acks=0 gets no offset from the broker, so its reported offset proves nothing.
  const acked = found.filter((s) => String(s.acks) !== "0");
  const match = acked.find((s) => same(s, record));
  if (match) return { status: "ok", producer: match.producer, acks: match.acks, tookMs: match.tookMs, at: match.at };
  if (acked.length) return { status: "unverified", reason: "mismatch", producer: acked[0].producer };
  if (found.length) return { status: "unverified", reason: "acks0", producer: found[0].producer };
  return { status: "evicted" };
}

function traceRead(c, { record, partition, ignoredReads }) {
  const row = { group: c.group, consumer: c.name, status: "pending", redelivered: false };
  const ignored = ignoredReads && ignoredReads.has(`${c.name}:${partition}:${record.offset}`);
  const seen = ignored ? [] : (c.received || []).filter((r) => r.partition === partition && r.offset === record.offset);
  if (seen.length) {
    row.status = seen.some((r) => same(r, record)) ? "ok" : "unverified";
    row.redelivered = seen.some((r) => r.redelivered);
    return row;
  }
  const pos = c.positions && c.positions[partition];
  if (!ignored && pos !== undefined && pos > record.offset) row.status = "evicted";
  return row;
}

// buildTrace reports only what the lab has evidence for: anything else is pending, unverified or evicted.
export function buildTrace({ topic, record, partition, producers, consumers, groups, staleBefore, ignoredReads }) {
  const mine = (consumers || []).filter((c) => c.topic === topic);
  const names = [...new Set([...mine.map((c) => c.group), ...(groups || []).map((g) => g.name)])];
  const commits = names.map((group) => {
    const g = (groups || []).find((x) => x.name === group);
    const committed = g && g.committed ? g.committed[partition] : undefined;
    return { group, committed: committed === undefined ? null : committed,
      status: committed !== undefined && committed > record.offset ? "ok" : "pending" };
  });
  return {
    send: traceSend({ topic, record, partition, producers, staleBefore }),
    log: { status: "ok", partition, offset: record.offset },
    reads: mine.map((c) => traceRead(c, { record, partition, ignoredReads })),
    commits,
  };
}
