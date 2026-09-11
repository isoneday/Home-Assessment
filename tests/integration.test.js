import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(predicate, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const value = await predicate();
      if (value) return value;
    } catch {}
    await sleep(100);
  }
  throw new Error("timed out waiting for condition");
}

test("end-to-end: server triggers a private client and receives streamed file", { timeout: 20_000 }, async () => {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "silentmode-assignment-"));
  const port = 18000 + Math.floor(Math.random() * 1000);
  const serverUrl = `http://127.0.0.1:${port}`;
  const source = path.join(tmp, "file_to_download.txt");
  const downloadDir = path.join(tmp, "downloads");
  const payload = Buffer.alloc(2 * 1024 * 1024, "integration-test\n");
  await writeFile(source, payload);

  const common = {
    ...process.env,
    SERVER_URL: serverUrl,
    CLIENT_ID: "restaurant-001",
    CLIENT_TOKEN: "client-secret-001",
    ADMIN_API_KEY: "admin-secret",
  };
  const server = spawn(process.execPath, ["src/server.js"], {
    cwd: process.cwd(),
    env: {
      ...common,
      PORT: String(port),
      PUBLIC_BASE_URL: serverUrl,
      DOWNLOAD_DIR: downloadDir,
      CLIENT_TOKENS_JSON: JSON.stringify({ "restaurant-001": "client-secret-001" }),
    },
    stdio: "ignore",
  });
  const client = spawn(process.execPath, ["src/client.js"], {
    cwd: process.cwd(),
    env: { ...common, FILE_PATH: source },
    stdio: "ignore",
  });

  try {
    await waitFor(async () => (await fetch(`${serverUrl}/health`)).ok);
    await waitFor(async () => {
      const r = await fetch(`${serverUrl}/api/v1/clients`, { headers: { "X-API-Key": "admin-secret" } });
      return r.ok && (await r.json()).length === 1;
    });

    const started = await fetch(`${serverUrl}/api/v1/clients/restaurant-001/downloads`, {
      method: "POST",
      headers: { "X-API-Key": "admin-secret" },
    });
    assert.equal(started.status, 202);
    const startBody = await started.json();

    const completed = await waitFor(async () => {
      const r = await fetch(`${serverUrl}${startBody.status_url}`, { headers: { "X-API-Key": "admin-secret" } });
      const body = await r.json();
      return body.status === "completed" ? body : body.status === "failed" ? Promise.reject(new Error(body.error)) : false;
    });

    assert.equal(completed.receivedBytes, payload.length);
    const received = await readFile(completed.savedPath);
    assert.deepEqual(received, payload);
    assert.equal(completed.sha256, createHash("sha256").update(payload).digest("hex"));

    const lateFailure = await fetch(`${serverUrl}${startBody.status_url}/client-status`, {
      method: "POST",
      headers: {
        Authorization: "Bearer client-secret-001",
        "X-Client-Id": "restaurant-001",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ status: "failed", error: "late client callback" }),
    });
    assert.equal(lateFailure.status, 409);
    assert.equal((await lateFailure.json()).error, "transfer_terminal");

    const finalStatus = await fetch(`${serverUrl}${startBody.status_url}`, { headers: { "X-API-Key": "admin-secret" } });
    assert.equal((await finalStatus.json()).status, "completed");
  } finally {
    client.kill("SIGTERM");
    server.kill("SIGTERM");
    await rm(tmp, { recursive: true, force: true });
  }
});
