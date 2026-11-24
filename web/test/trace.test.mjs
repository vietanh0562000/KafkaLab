import test from "node:test";
import assert from "node:assert/strict";
import { buildTrace } from "../static/ui/logic/trace.js";

const AT = "2026-11-24T10:00:00.000Z";
const record = { partition: 0, offset: 42, key: "xe-1", value: "v1" };
const send = (o = {}) => ({ id: 1, producer: "p1", acks: "all", topic: "orders", key: "xe-1", value: "v1", partition: 0, offset: 42, tookMs: 12, at: AT, ...o });
const consumer = (o = {}) => ({ name: "c1", group: "g1", topic: "orders", assigned: [0], positions: { 0: 43 }, received: [{ partition: 0, offset: 42, key: "xe-1", value: "v1" }], ...o });
const trace = (o = {}) => buildTrace({ topic: "orders", record, partition: 0, producers: [{ name: "p1", sends: [send()] }], consumers: [consumer()], groups: [{ name: "g1", committed: { 0: 43 } }], staleBefore: 0, ignoredReads: new Set(), ...o });

test("a send with the same offset, key and value is verified", () => {
  const t = trace();
  assert.equal(t.send.status, "ok");
  assert.equal(t.send.producer, "p1");
  assert.equal(t.send.tookMs, 12);
});

test("same offset with a different value is unverified", () => {
  assert.equal(trace({ producers: [{ name: "p1", sends: [send({ value: "other" })] }] }).send.status, "unverified");
});

test("acks=0 is never matched by offset", () => {
  const t = trace({ producers: [{ name: "p1", sends: [send({ acks: "0" })] }] });
  assert.equal(t.send.status, "unverified");
  assert.equal(t.send.reason, "acks0");
});

test("a real ack wins over an acks=0 send reporting the same offset", () => {
  const t = trace({ producers: [{ name: "p0", sends: [send({ acks: "0", producer: "p0" })] }, { name: "p1", sends: [send()] }] });
  assert.equal(t.send.status, "ok");
  assert.equal(t.send.producer, "p1");
});

test("a send older than staleBefore is ignored", () => {
  assert.equal(trace({ staleBefore: new Date(AT).getTime() + 1 }).send.status, "evicted");
});

test("a failed or pending send is not evidence", () => {
  assert.equal(trace({ producers: [{ name: "p1", sends: [send({ err: "boom" })] }] }).send.status, "evicted");
  assert.equal(trace({ producers: [{ name: "p1", sends: [send({ pending: true })] }] }).send.status, "evicted");
});

test("no send in the buffer is evicted", () => {
  assert.equal(trace({ producers: [] }).send.status, "evicted");
});

test("two groups get separate read and commit rows", () => {
  const t = trace({
    consumers: [consumer(), consumer({ name: "c2", group: "g2", positions: {}, assigned: [], received: [] })],
    groups: [{ name: "g1", committed: { 0: 43 } }, { name: "g2", committed: {} }],
  });
  assert.deepEqual(t.reads.map((r) => [r.group, r.consumer, r.status]), [["g1", "c1", "ok"], ["g2", "c2", "pending"]]);
  assert.deepEqual(t.commits.map((c) => [c.group, c.status]), [["g1", "ok"], ["g2", "pending"]]);
});

test("committed points at the next record to read", () => {
  assert.equal(trace({ groups: [{ name: "g1", committed: { 0: 42 } }] }).commits[0].status, "pending");
  assert.equal(trace({ groups: [{ name: "g1", committed: { 0: 43 } }] }).commits[0].status, "ok");
});

test("a consumer record with different content is unverified", () => {
  const c = consumer({ received: [{ partition: 0, offset: 42, key: "xe-9", value: "v1" }] });
  assert.equal(trace({ consumers: [c] }).reads[0].status, "unverified");
});

test("a read past this offset that left the buffer is evicted", () => {
  assert.equal(trace({ consumers: [consumer({ received: [], positions: { 0: 50 } })] }).reads[0].status, "evicted");
});

test("reads from before a topic was recreated are ignored", () => {
  const t = trace({ ignoredReads: new Set(["c1:0:42"]), consumers: [consumer({ positions: {} })] });
  assert.equal(t.reads[0].status, "pending");
});

test("a redelivered read is reported", () => {
  const c = consumer({ received: [{ partition: 0, offset: 42, key: "xe-1", value: "v1" }, { partition: 0, offset: 42, key: "xe-1", value: "v1", redelivered: true }] });
  assert.equal(trace({ consumers: [c] }).reads[0].redelivered, true);
});

test("consumers of another topic do not appear", () => {
  assert.equal(trace({ consumers: [consumer({ topic: "payments" })], groups: [] }).reads.length, 0);
});
