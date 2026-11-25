import test from "node:test";
import assert from "node:assert/strict";
import { diffState } from "../static/ui/logic/diff.js";

const sends = (n, from = 1, extra = {}) => Array.from({ length: n }, (_, i) => ({ id: from + i, producer: "p1", topic: "orders", partition: i % 3, offset: i, key: `k${i}`, ...extra }));
const state = (o = {}) => ({
  topics: [{ name: "orders" }],
  topic: { name: "orders", partitions: [{ partition: 0, end: 0 }, { partition: 1, end: 0 }, { partition: 2, end: 0 }] },
  producers: [{ name: "p1", sends: [] }],
  consumers: [],
  ...o,
});

test("the first state produces no tokens", () => {
  const d = diffState(null, state({ producers: [{ name: "p1", sends: sends(3) }] }));
  assert.equal(d.tokens.length, 0);
  assert.equal(d.overflow, 0);
});

test("a new send produces one send token", () => {
  const d = diffState(state(), state({ producers: [{ name: "p1", sends: sends(1) }] }));
  assert.deepEqual(d.tokens, [{ kind: "send", from: "p1", to: 0, partition: 0, key: "k0" }]);
});

test("1000 new sends are capped at 12 tokens", () => {
  const d = diffState(state(), state({ producers: [{ name: "p1", sends: sends(1000) }] }));
  assert.equal(d.tokens.length, 12);
  assert.equal(d.overflow, 988);
});

test("a pending send gets no token until it completes", () => {
  const pending = state({ producers: [{ name: "p1", sends: sends(1, 1, { pending: true }) }] });
  assert.equal(diffState(state(), pending).tokens.length, 0);
  const done = state({ producers: [{ name: "p1", sends: sends(1) }] });
  assert.equal(diffState(pending, done).tokens.length, 1);
});

test("a failed send gets no token", () => {
  const d = diffState(state(), state({ producers: [{ name: "p1", sends: sends(1, 1, { err: "boom" }) }] }));
  assert.equal(d.tokens.length, 0);
});

test("a send to another topic gets no token", () => {
  const d = diffState(state(), state({ producers: [{ name: "p1", sends: sends(1, 1, { topic: "payments" }) }] }));
  assert.equal(d.tokens.length, 0);
});

test("a newly received record produces a read token", () => {
  const c = (received) => ({ name: "c1", group: "g1", topic: "orders", polls: received.length, received });
  const d = diffState(state({ consumers: [c([])] }), state({ consumers: [c([{ partition: 1, offset: 4, key: "k" }])] }));
  assert.deepEqual(d.tokens, [{ kind: "read", from: 1, to: "c1", partition: 1, key: "k" }]);
});

test("changed partitions are reported", () => {
  const next = state({ topic: { name: "orders", partitions: [{ partition: 0, end: 0 }, { partition: 1, end: 5 }, { partition: 2, end: 0 }] } });
  assert.deepEqual(diffState(state(), next).changedPartitions, [1]);
});

test("an end offset going down is a topic reset", () => {
  const before = state({ topic: { name: "orders", partitions: [{ partition: 0, end: 9 }] } });
  const after = state({ topic: { name: "orders", partitions: [{ partition: 0, end: 0 }] } });
  assert.equal(diffState(before, after).topicReset, true);
});

test("an unknown end offset is not a reset", () => {
  const before = state({ topic: { name: "orders", partitions: [{ partition: 0, end: 9 }] } });
  const after = state({ topic: { name: "orders", partitions: [{ partition: 0, end: -1 }] } });
  assert.equal(diffState(before, after).topicReset, false);
});

test("the topic disappearing is a topic reset", () => {
  assert.equal(diffState(state(), state({ topics: [], topic: null })).topicReset, true);
});

test("switching topic is not a reset and draws nothing", () => {
  const other = state({ topics: [{ name: "orders" }, { name: "payments" }], topic: { name: "payments", partitions: [{ partition: 0, end: 0 }] } });
  const d = diffState(state({ topics: [{ name: "orders" }, { name: "payments" }] }), other);
  assert.equal(d.topicReset, false);
  assert.equal(d.tokens.length, 0);
});
