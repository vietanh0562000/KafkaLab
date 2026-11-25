const GAP_MS = 5000;

export function createHistory(windowMs = 900000) {
  const byTopic = new Map();
  return {
    sample(topic, at, { end, lag }) {
      const list = byTopic.get(topic) || [];
      list.push({ at, end, lag });
      while (list.length && list[0].at < at - windowMs) list.shift();
      byTopic.set(topic, list);
    },
    // A null entry breaks the line: nothing was sampled for this topic during that time.
    series(topic) {
      const out = [];
      (byTopic.get(topic) || []).forEach((p, i, list) => {
        const prev = list[i - 1];
        const gap = prev && p.at - prev.at > GAP_MS;
        if (gap) out.push(null);
        let rate = null;
        if (prev && !gap && p.end !== null && prev.end !== null && p.end >= prev.end) {
          rate = ((p.end - prev.end) * 1000) / (p.at - prev.at);
        }
        out.push({ at: p.at, rate, lag: p.lag });
      });
      return out;
    },
  };
}
