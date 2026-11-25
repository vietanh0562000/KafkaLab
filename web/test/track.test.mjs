import test from "node:test";
import assert from "node:assert/strict";
import { createTracker, topicTotals } from "../static/ui/logic/track.js";

const st = (end, committed, extra = {}) => ({
  topics: [{ name: "orders" }],
  topic: { name: "orders", partitions: [{ partition: 0, end }] },
  groups: [{ name: "g1", members: 1, committed: committed === null ? {} : { 0: committed } }],
  producers: [],
  consumers: [],
  ...extra,
});

test("lagSince starts when lag appears and holds while lag does not fall", () => {
  const t = createTracker();
  t.update(st(0, 0), 1000);
  assert.equal(t.update(st(5, 0), 2000).lagSince["orders:g1:0"], 2000);
  assert.equal(t.update(st(8, 0), 3000).lagSince["orders:g1:0"], 2000);
});

test("lagSince restarts when lag falls", () => {
  const t = createTracker();
  t.update(st(5, 0), 1000);
  assert.equal(t.update(st(5, 2), 4000).lagSince["orders:g1:0"], 4000);
});

test("lagSince clears when lag is zero or unknown", () => {
  const t = createTracker();
  t.update(st(5, 0), 1000);
  assert.equal(t.update(st(5, 5), 2000).lagSince["orders:g1:0"], undefined);
  t.update(st(9, 5), 3000);
  assert.equal(t.update(st(-1, 5), 4000).lagSince["orders:g1:0"], undefined);
});

test("an end offset change records when the partition last moved", () => {
  const t = createTracker();
  t.update(st(0, 0), 1000);
  assert.equal(t.update(st(3, 0), 2000).changeAt["orders:0"], 2000);
  assert.equal(t.update(st(3, 0), 9000).changeAt["orders:0"], 2000);
});

test("a commit also counts as movement", () => {
  const t = createTracker();
  t.update(st(3, 0), 1000);
  assert.equal(t.update(st(3, 3), 2000).changeAt["orders:0"], 2000);
});

test("a topic reset marks older evidence stale and ignores held reads", () => {
  const consumers = [{ name: "c1", group: "g1", topic: "orders", received: [{ partition: 0, offset: 4, key: "k" }] }];
  const t = createTracker();
  t.update(st(9, 0, { consumers }), 1000);
  const out = t.update(st(0, 0, { consumers }), 2000);
  assert.equal(out.staleBefore.orders, 2000);
  assert.ok(out.ignoredReads.orders.has("c1:0:4"));
});

test("topicTotals sums end and known lag", () => {
  const topic = { partitions: [{ partition: 0, end: 10 }, { partition: 1, end: 5 }] };
  assert.deepEqual(topicTotals(topic, [{ committed: { 0: 7, 1: 5 } }]), { end: 15, lag: 3, partial: false });
});

test("topicTotals flags a lag missing some partitions", () => {
  const topic = { partitions: [{ partition: 0, end: 10 }, { partition: 1, end: 5 }] };
  assert.deepEqual(topicTotals(topic, [{ committed: { 0: 7 } }]), { end: 15, lag: 3, partial: true });
});

test("topicTotals reports unknown instead of zero", () => {
  const topic = { partitions: [{ partition: 0, end: -1 }] };
  assert.deepEqual(topicTotals(topic, []), { end: null, lag: null, partial: false });
});
