import { diffState } from "./diff.js";
import { lagOf } from "./health.js";

export function topicTotals(topic, groups) {
  const parts = (topic && topic.partitions) || [];
  const known = parts.length > 0 && parts.every((p) => p.end >= 0);
  const end = known ? parts.reduce((a, p) => a + p.end, 0) : null;
  let lag = null, partial = false;
  for (const g of groups || []) {
    for (const p of parts) {
      const l = lagOf(p.end, g.committed && g.committed[p.partition]);
      if (l === null) partial = true;
      else lag = (lag || 0) + l;
    }
  }
  if (lag === null) partial = false;
  return { end, lag, partial };
}

// createTracker keeps what a single read cannot show: when things last moved and which evidence is stale.
export function createTracker() {
  let prev = null;
  const changeAt = {}, lagSince = {}, lastLag = {}, lastCommit = {}, staleBefore = {}, ignoredReads = {};

  return {
    update(next, now) {
      const diff = diffState(prev, next);
      const gone = prev && prev.topic && (diff.topicReset ? prev.topic.name : null);
      if (gone) {
        staleBefore[gone] = now;
        const held = ignoredReads[gone] || (ignoredReads[gone] = new Set());
        for (const c of prev.consumers || []) {
          if (c.topic !== gone) continue;
          for (const r of c.received || []) held.add(`${c.name}:${r.partition}:${r.offset}`);
        }
      }
      const topic = next.topic;
      if (topic) {
        for (const p of diff.changedPartitions) changeAt[`${topic.name}:${p}`] = now;
        for (const g of next.groups || []) {
          for (const p of topic.partitions) {
            const key = `${topic.name}:${g.name}:${p.partition}`;
            const committed = g.committed && g.committed[p.partition];
            if (committed !== undefined && lastCommit[key] !== undefined && committed !== lastCommit[key]) {
              changeAt[`${topic.name}:${p.partition}`] = now;
            }
            if (committed !== undefined) lastCommit[key] = committed;
            const lag = lagOf(p.end, committed);
            if (lag === null || lag === 0) delete lagSince[key];
            else if (lagSince[key] === undefined || lag < lastLag[key]) lagSince[key] = now;
            lastLag[key] = lag;
          }
        }
      }
      prev = next;
      return { tokens: diff.tokens, overflow: diff.overflow, changeAt, lagSince, staleBefore, ignoredReads };
    },
  };
}
