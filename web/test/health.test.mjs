import test from "node:test";
import assert from "node:assert/strict";
import { lagOf, partitionHealth, edgeColor } from "../static/ui/logic/health.js";

const up = (id) => ({ id, state: "running" });
const down = (id) => ({ id, state: "exited" });
const brokers = [up(1), up(2), up(3)];
const part = (o = {}) => ({ leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3], ...o });

test("no leader is red", () => {
  assert.equal(partitionHealth({ partition: part({ leader: -1 }), brokers, minIsr: 2 }), "red");
});

test("a dead broker still listed as leader is red", () => {
  assert.equal(partitionHealth({ partition: part(), brokers: [down(1), up(2), up(3)], minIsr: 2 }), "red");
});

test("ISR below minIsr is red", () => {
  assert.equal(partitionHealth({ partition: part({ isr: [1] }), brokers, minIsr: 2 }), "red");
});

test("dead follower out of ISR with ISR at minIsr is yellow", () => {
  assert.equal(partitionHealth({ partition: part({ isr: [1, 2] }), brokers: [up(1), up(2), down(3)], minIsr: 2 }), "yellow");
});

test("dead broker still in ISR is yellow", () => {
  assert.equal(partitionHealth({ partition: part(), brokers: [up(1), up(2), down(3)], minIsr: 2 }), "yellow");
});

test("unknown broker state is not treated as dead", () => {
  assert.equal(partitionHealth({ partition: part(), brokers: [up(1), up(2), { id: 3, state: "" }], minIsr: 2 }), "ok");
});

test("all replicas in sync is ok", () => {
  assert.equal(partitionHealth({ partition: part(), brokers, minIsr: 2 }), "ok");
});

const NOW = 100000;
const edge = (o) => edgeColor({ partitionHealth: "ok", lastSend: null, lagSince: null, members: 0, lastChangeAt: null, now: NOW, ...o });

test("a failed send stays red for 30 s", () => {
  assert.equal(edge({ lastSend: { ok: false, at: NOW - 29000 } }), "red");
  assert.notEqual(edge({ lastSend: { ok: false, at: NOW - 31000 } }), "red");
});

test("a later successful send clears red", () => {
  assert.notEqual(edge({ lastSend: { ok: true, at: NOW - 1000 } }), "red");
});

test("lag held 10 s with a member is yellow", () => {
  assert.equal(edge({ lagSince: NOW - 10000, members: 1 }), "yellow");
});

test("lag held 9 s is not yellow", () => {
  assert.notEqual(edge({ lagSince: NOW - 9000, members: 1 }), "yellow");
});

test("lag with no members is not yellow", () => {
  assert.notEqual(edge({ lagSince: NOW - 60000, members: 0 }), "yellow");
});

test("red outranks yellow and blue", () => {
  assert.equal(edge({ partitionHealth: "red", lagSince: NOW - 60000, members: 1, lastChangeAt: NOW }), "red");
});

test("yellow partition health outranks recent flow", () => {
  assert.equal(edge({ partitionHealth: "yellow", lastChangeAt: NOW }), "yellow");
});

test("a change within 5 s is blue, older is idle", () => {
  assert.equal(edge({ lastChangeAt: NOW - 4000 }), "blue");
  assert.equal(edge({ lastChangeAt: NOW - 6000 }), "idle");
});

test("lagOf treats unknown offsets as unknown", () => {
  assert.equal(lagOf(-1, 3), null);
  assert.equal(lagOf(10, undefined), null);
  assert.equal(lagOf(10, 3), 7);
});
