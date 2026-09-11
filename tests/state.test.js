import assert from "node:assert/strict";
import test from "node:test";
import { AppState, publicTransfer } from "../src/state.js";

test("transfer IDs/tokens are unique and upload token is private", () => {
  const state = new AppState();
  const a = state.createTransfer("restaurant-001", 60_000);
  const b = state.createTransfer("restaurant-001", 60_000);
  assert.notEqual(a.id, b.id);
  assert.notEqual(a.uploadToken, b.uploadToken);
  assert.equal(a.status, "requested");
  assert.equal("uploadToken" in publicTransfer(a), false);
});

test("transfer state update preserves identity", () => {
  const state = new AppState();
  const t = state.createTransfer("restaurant-001", 60_000);
  const updated = state.updateTransfer(t.id, { status: "receiving", receivedBytes: 1024 });
  assert.equal(updated.clientId, "restaurant-001");
  assert.equal(updated.status, "receiving");
  assert.equal(updated.receivedBytes, 1024);
});
