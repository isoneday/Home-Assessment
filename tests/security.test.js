import assert from "node:assert/strict";
import http from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

async function startServer(t) {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "silentmode-security-"));
  const port = 19000 + Math.floor(Math.random() * 1000);
  const serverUrl = `http://127.0.0.1:${port}`;
  const server = spawn(process.execPath, ["src/server.js"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      PUBLIC_BASE_URL: serverUrl,
      DOWNLOAD_DIR: path.join(tmp, "downloads"),
      ADMIN_API_KEY: "admin-secret",
      CLIENT_TOKENS_JSON: JSON.stringify({ "restaurant-001": "client-secret-001" }),
    },
    stdio: "ignore",
  });

  t.after(async () => {
    server.kill("SIGTERM");
    await rm(tmp, { recursive: true, force: true });
  });

  await waitFor(async () => (await fetch(`${serverUrl}/health`)).ok);
  return { serverUrl, tmp };
}

async function openControlConnection(serverUrl, clientId = "restaurant-001") {
  const url = new URL("/client/events", serverUrl);
  url.searchParams.set("client_id", clientId);

  return new Promise((resolve, reject) => {
    const req = http.request(
      url,
      {
        method: "GET",
        headers: {
          Accept: "text/event-stream",
          Authorization: "Bearer client-secret-001",
        },
      },
      (res) => {
        if (res.statusCode === 200) {
          res.resume();
          resolve({ req, res });
        } else {
          reject(new Error(`control connection failed: ${res.statusCode}`));
        }
      },
    );
    req.on("error", reject);
    req.end();
  });
}

test("admin APIs reject missing or invalid credentials", async (t) => {
  const { serverUrl } = await startServer(t);

  const missing = await fetch(`${serverUrl}/api/v1/clients`);
  assert.equal(missing.status, 401);

  const invalid = await fetch(`${serverUrl}/api/v1/clients`, {
    headers: { "X-API-Key": "wrong-secret" },
  });
  assert.equal(invalid.status, 401);
});

test("download trigger fails clearly when target client is not connected", async (t) => {
  const { serverUrl } = await startServer(t);

  const response = await fetch(`${serverUrl}/api/v1/clients/restaurant-001/downloads`, {
    method: "POST",
    headers: { "X-API-Key": "admin-secret" },
  });
  assert.equal(response.status, 409);
  assert.equal((await response.json()).error, "client_not_connected");
});

test("client status callback validates JSON and acknowledged file size", async (t) => {
  const { serverUrl } = await startServer(t);
  const control = await openControlConnection(serverUrl);
  t.after(() => {
    control.res.destroy();
    control.req.destroy();
  });

  await waitFor(async () => {
    const response = await fetch(`${serverUrl}/api/v1/clients`, { headers: { "X-API-Key": "admin-secret" } });
    return response.ok && (await response.json()).length === 1;
  });

  const started = await fetch(`${serverUrl}/api/v1/clients/restaurant-001/downloads`, {
    method: "POST",
    headers: { "X-API-Key": "admin-secret" },
  });
  assert.equal(started.status, 202);
  const startBody = await started.json();

  const invalidJson = await fetch(`${serverUrl}${startBody.status_url}/client-status`, {
    method: "POST",
    headers: {
      Authorization: "Bearer client-secret-001",
      "X-Client-Id": "restaurant-001",
      "Content-Type": "application/json",
    },
    body: "{",
  });
  assert.equal(invalidJson.status, 400);
  assert.equal((await invalidJson.json()).error, "invalid_json");

  const invalidShape = await fetch(`${serverUrl}${startBody.status_url}/client-status`, {
    method: "POST",
    headers: {
      Authorization: "Bearer client-secret-001",
      "X-Client-Id": "restaurant-001",
      "Content-Type": "application/json",
    },
    body: "null",
  });
  assert.equal(invalidShape.status, 400);
  assert.equal((await invalidShape.json()).error, "invalid_status");

  const invalidSize = await fetch(`${serverUrl}${startBody.status_url}/client-status`, {
    method: "POST",
    headers: {
      Authorization: "Bearer client-secret-001",
      "X-Client-Id": "restaurant-001",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ status: "client_acknowledged", file_name: "file_to_download.txt", file_size: -1 }),
  });
  assert.equal(invalidSize.status, 400);
  assert.equal((await invalidSize.json()).error, "invalid_file_size");
});

test("client reports a failed transfer when the configured file is missing", { timeout: 20_000 }, async (t) => {
  const { serverUrl, tmp } = await startServer(t);
  const client = spawn(process.execPath, ["src/client.js"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      SERVER_URL: serverUrl,
      CLIENT_ID: "restaurant-001",
      CLIENT_TOKEN: "client-secret-001",
      FILE_PATH: path.join(tmp, "missing-file.txt"),
    },
    stdio: "ignore",
  });
  t.after(() => client.kill("SIGTERM"));

  await waitFor(async () => {
    const response = await fetch(`${serverUrl}/api/v1/clients`, { headers: { "X-API-Key": "admin-secret" } });
    return response.ok && (await response.json()).length === 1;
  });

  const started = await fetch(`${serverUrl}/api/v1/clients/restaurant-001/downloads`, {
    method: "POST",
    headers: { "X-API-Key": "admin-secret" },
  });
  assert.equal(started.status, 202);
  const startBody = await started.json();

  const failed = await waitFor(async () => {
    const response = await fetch(`${serverUrl}${startBody.status_url}`, { headers: { "X-API-Key": "admin-secret" } });
    const transfer = await response.json();
    return transfer.status === "failed" ? transfer : false;
  });
  assert.match(failed.error, /no such file or directory|ENOENT/i);

  const fileResponse = await fetch(`${serverUrl}${startBody.file_url}`, { headers: { "X-API-Key": "admin-secret" } });
  assert.equal(fileResponse.status, 409);
  assert.equal((await fileResponse.json()).error, "transfer_not_completed");
});
