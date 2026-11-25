import test from "node:test";
import assert from "node:assert/strict";
import { mergeLastGood } from "../static/ui/logic/merge.js";

test("keeps cached brokers when the brokers source failed", () => {
  const cache = { brokers: [{ id: 1 }] };
  const out = mergeLastGood({ brokers: null, errors: { brokers: "timeout" } }, cache, "orders");
  assert.deepEqual(out.brokers, [{ id: 1 }]);
});

test("replaces cached brokers when the source answered", () => {
  const cache = { brokers: [{ id: 1 }] };
  const out = mergeLastGood({ brokers: [{ id: 2 }], errors: {} }, cache, "orders");
  assert.deepEqual(out.brokers, [{ id: 2 }]);
  assert.deepEqual(cache.brokers, [{ id: 2 }]);
});

test("caches groups per topic so one topic's commits never appear on another", () => {
  const cache = {};
  mergeLastGood({ groups: [{ name: "g1" }], errors: {} }, cache, "orders");
  const other = mergeLastGood({ groups: null, errors: { groups: "timeout" } }, cache, "payments");
  assert.deepEqual(other.groups, []);
  const same = mergeLastGood({ groups: null, errors: { groups: "timeout" } }, cache, "orders");
  assert.deepEqual(same.groups, [{ name: "g1" }]);
});

test("a failed source with nothing cached becomes an empty list", () => {
  const out = mergeLastGood({ topics: null, errors: { topics: "down" } }, {}, "orders");
  assert.deepEqual(out.topics, []);
});
