import test from "node:test";
import assert from "node:assert/strict";
import { stepOf, serviceStatus } from "../static/ui/logic/stepper.js";

const s = (o) => stepOf({ guess: "", running: false, result: null, revealed: false, ...o });

test("nothing written yet is the predict step", () => assert.equal(s({}), "predict"));
test("a blank guess is still predict", () => assert.equal(s({ guess: "   " }), "predict"));
test("a written guess moves to run", () => assert.equal(s({ guess: "lag tăng" }), "run"));
test("running is observe", () => assert.equal(s({ running: true }), "observe"));
test("a finished result stays on observe", () => assert.equal(s({ result: { passed: true } }), "observe"));
test("revealed always wins", () => {
  assert.equal(s({ revealed: true }), "explain");
  assert.equal(s({ revealed: true, running: true, guess: "x" }), "explain");
});

test("no service container is missing", () => {
  assert.equal(serviceStatus({ "lab-kafka-1": "running" }), "missing");
  assert.equal(serviceStatus(null), "missing");
});
test("service containers that are all down are stopped", () => {
  assert.equal(serviceStatus({ "lab-gap-svc-1": "exited" }), "stopped");
});
test("one running service container is running", () => {
  assert.equal(serviceStatus({ "lab-gap-svc-1": "exited", "lab-gap-svc-2": "running" }), "running");
});
