const isTracking = (name) => name.endsWith("-tracking");

// layoutFlow decides which nodes and connections exist; where they land on screen is left to CSS.
export function layoutFlow({ producers, partitions, groups, service, dlq }) {
  const parts = partitions || [];
  const nodes = [], edges = [];
  const link = (from, to, partition) => edges.push({ id: `${from}>${to}`, from, to, partition });

  if (!service) {
    for (const p of producers || []) {
      const id = `producer:${p.name}`;
      nodes.push({ id, kind: "producer", name: p.name });
      const sent = (p.partitions || []).filter((n) => parts.some((x) => x.partition === n));
      if (sent.length) sent.forEach((n) => link(id, `partition:${n}`, n));
      else if (parts.length) link(id, "cluster");
    }
  }
  if (parts.length) nodes.push({ id: "cluster", kind: "cluster" });
  for (const p of parts) nodes.push({ id: `partition:${p.partition}`, kind: "partition", partition: p.partition });
  for (const g of groups || []) {
    const id = `group:${g.name}`;
    nodes.push({ id, kind: "group", name: g.name });
    for (const n of g.partitions || []) {
      if (parts.some((x) => x.partition === n)) link(`partition:${n}`, id, n);
    }
    if (service && !isTracking(g.name)) link(id, "service");
  }
  if (service) {
    nodes.push({ id: "service", kind: "service" }, { id: "db", kind: "db" }, { id: "dlq", kind: "dlq", count: dlq });
    link("service", "db");
    link("service", "dlq");
  }
  return { nodes, edges };
}

const committedOf = (g) => Object.fromEntries((g.lags || [])
  .filter((l) => l.committed !== undefined && l.committed >= 0).map((l) => [l.partition, l.committed]));

// serviceView reshapes the service snapshot into the same form the map reads for a playground topic.
export function serviceView(s) {
  const containers = (s && s.containers) || {};
  const partitions = ((s && s.partitions) || []).map((p) => ({ ...p, start: -1, records: [] }));
  const groups = [s && s.group, s && s.tracking].filter((g) => g && g.name).map((g) => ({
    name: g.name, state: g.state, members: g.members || 0, committed: committedOf(g),
    partitions: (g.lags || []).map((l) => l.partition),
  }));
  const errors = (s && s.errors) || {};
  return {
    topic: { name: "lab.short-term", rf: partitions[0] ? partitions[0].replicas.length : 0, minIsr: 2, partitions },
    brokers: [1, 2, 3].map((id) => ({ id, container: `lab-kafka-${id}`, state: containers[`lab-kafka-${id}`] || "", registered: true })),
    groups,
    instances: (s && s.instances) || [],
    dlq: s && s.dlqEnd !== undefined ? s.dlqEnd : null,
    dbRows: errors.db ? null : (s && s.dbRows !== undefined ? s.dbRows : null),
    lateRows: errors.db ? null : (s && s.lateRows !== undefined ? s.lateRows : null),
    errors,
  };
}
