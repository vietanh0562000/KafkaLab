import test from "node:test";
import assert from "node:assert/strict";
import { layoutFlow, serviceView } from "../static/ui/logic/layout.js";

const partitions = [{ partition: 0 }, { partition: 1 }];
const ids = (l) => l.nodes.map((n) => n.id);
const edge = (l, from, to) => l.edges.some((e) => e.from === from && e.to === to);

test("playground mode has no service or DB nodes", () => {
  const l = layoutFlow({ producers: [{ name: "p1", partitions: [0] }], partitions, groups: [{ name: "g1", partitions: [0] }] });
  assert.ok(!ids(l).includes("service"));
  assert.ok(!ids(l).includes("db"));
  assert.ok(edge(l, "producer:p1", "partition:0"));
  assert.ok(edge(l, "partition:0", "group:g1"));
  assert.ok(!edge(l, "partition:1", "group:g1"));
});

test("a producer that has not sent yet connects to the cluster", () => {
  const l = layoutFlow({ producers: [{ name: "p1", partitions: [] }], partitions, groups: [] });
  assert.ok(edge(l, "producer:p1", "cluster"));
});

test("service mode has no producer column and has a DLQ edge", () => {
  const l = layoutFlow({ producers: [{ name: "p1", partitions: [0] }], partitions, groups: [{ name: "gap-lab", partitions: [0, 1] }], service: {}, dlq: 3 });
  assert.ok(!ids(l).some((id) => id.startsWith("producer:")));
  assert.ok(edge(l, "group:gap-lab", "service"));
  assert.ok(edge(l, "service", "db"));
  assert.ok(edge(l, "service", "dlq"));
});

test("a tracking group has no edge to the service", () => {
  const l = layoutFlow({ partitions, groups: [{ name: "gap-lab-tracking", partitions: [0] }], service: {} });
  assert.ok(ids(l).includes("group:gap-lab-tracking"));
  assert.ok(!edge(l, "group:gap-lab-tracking", "service"));
});

test("null inputs give an empty valid layout", () => {
  const l = layoutFlow({ producers: null, partitions: null, groups: null });
  assert.deepEqual(l.edges, []);
  assert.ok(Array.isArray(l.nodes));
});

test("serviceView copes with a snapshot that has no service", () => {
  const v = serviceView({ containers: {}, partitions: null, group: {}, tracking: {}, instances: null, dlqEnd: 0, errors: {} });
  assert.deepEqual(v.topic.partitions, []);
  assert.deepEqual(v.groups, []);
  assert.deepEqual(v.instances, []);
});

test("serviceView maps partitions, committed offsets and broker states", () => {
  const v = serviceView({
    containers: { "lab-kafka-1": "running", "lab-kafka-2": "exited" },
    partitions: [{ partition: 0, leader: 1, replicas: [1, 2], isr: [1], end: 9 }],
    group: { name: "gap-lab", state: "Stable", members: 1, lags: [{ partition: 0, committed: 4, lag: 5 }] },
    tracking: { name: "gap-lab-tracking", state: "Empty", members: 0, lags: [{ partition: 0, committed: -1, lag: 9 }] },
    instances: [], dlqEnd: 2, dbRows: 7, errors: {},
  });
  assert.equal(v.topic.partitions[0].end, 9);
  assert.deepEqual(v.groups[0].committed, { 0: 4 });
  assert.deepEqual(v.groups[1].committed, {});
  assert.equal(v.brokers.find((b) => b.id === 2).state, "exited");
  assert.equal(v.dlq, 2);
});

test("serviceView keeps DB rows unknown while the DB is unreachable", () => {
  const v = serviceView({ containers: {}, group: {}, tracking: {}, dbRows: 0, errors: { db: "down" } });
  assert.equal(v.dbRows, null);
});
