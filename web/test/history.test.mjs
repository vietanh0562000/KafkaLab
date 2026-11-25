import test from "node:test";
import assert from "node:assert/strict";
import { createHistory } from "../static/ui/logic/history.js";

test("rate is records per second between samples", () => {
  const h = createHistory();
  h.sample("t", 0, { end: 0, lag: 0 });
  h.sample("t", 2000, { end: 100, lag: 5 });
  assert.deepEqual(h.series("t").at(-1), { at: 2000, rate: 50, lag: 5 });
});

test("samples older than 15 minutes are dropped", () => {
  const h = createHistory();
  h.sample("t", 0, { end: 0, lag: 0 });
  h.sample("t", 1000, { end: 1, lag: 0 });
  h.sample("t", 900000 + 1500, { end: 2, lag: 0 });
  assert.ok(h.series("t").filter(Boolean).every((p) => p.at >= 1500));
});

test("a sampling gap over 5 s yields a null point", () => {
  const h = createHistory();
  h.sample("t", 0, { end: 0, lag: 0 });
  h.sample("t", 1000, { end: 10, lag: 0 });
  h.sample("t", 7000, { end: 20, lag: 0 });
  assert.ok(h.series("t").includes(null));
});

test("a falling end offset does not produce a negative rate", () => {
  const h = createHistory();
  h.sample("t", 0, { end: 100, lag: 0 });
  h.sample("t", 1000, { end: 0, lag: 0 });
  assert.ok(h.series("t").filter(Boolean).every((p) => p.rate === null || p.rate >= 0));
});

test("an unknown end offset has unknown rate, never zero", () => {
  const h = createHistory();
  h.sample("t", 0, { end: 10, lag: 0 });
  h.sample("t", 1000, { end: null, lag: null });
  assert.equal(h.series("t").at(-1).rate, null);
});

test("topics keep separate series", () => {
  const h = createHistory();
  h.sample("a", 0, { end: 0, lag: 0 });
  assert.equal(h.series("b").length, 0);
});
