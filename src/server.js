import { createHash, timingSafeEqual } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rename, stat, unlink } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { URL } from "node:url";
import { AppState, publicTransfer } from "./state.js";

const PORT = Number(process.env.PORT ?? 8080);
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL ?? `http://localhost:${PORT}`).replace(/\/$/, "");
const ADMIN_API_KEY = process.env.ADMIN_API_KEY ?? "admin-secret";
const DOWNLOAD_DIR = path.resolve(process.env.DOWNLOAD_DIR ?? "./downloads");
const MAX_FILE_BYTES = Number(process.env.MAX_FILE_BYTES ?? 150 * 1024 * 1024);
const TRANSFER_TTL_MS = Number(process.env.TRANSFER_TTL_MS ?? 10 * 60 * 1000);
const CLIENT_TOKENS = JSON.parse(
  process.env.CLIENT_TOKENS_JSON ??
    '{"restaurant-001":"client-secret-001","restaurant-002":"client-secret-002"}',
);

const state = new AppState();

function constantTimeEqual(a = "", b = "") {
  const aa = Buffer.from(a);
  const bb = Buffer.from(b);
  return aa.length === bb.length && timingSafeEqual(aa, bb);
}

function bearerToken(value) {
  return value?.startsWith("Bearer ") ? value.slice(7).trim() : undefined;
}

function sendJson(res, status, body) {
  const data = Buffer.from(JSON.stringify(body));
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": data.length,
    "cache-control": "no-store",
  });
  res.end(data);
}

function adminAuthorized(req) {
  return constantTimeEqual(String(req.headers["x-api-key"] ?? ""), ADMIN_API_KEY);
}

function clientAuthorized(req, clientId) {
  const expected = CLIENT_TOKENS[clientId];
  const supplied = bearerToken(req.headers.authorization);
  return Boolean(expected && supplied && constantTimeEqual(supplied, expected));
}

async function readJson(req, maxBytes = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > maxBytes) throw new Error("request body too large");
    chunks.push(chunk);
  }
  if (size === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function writeSse(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function registerClient(req, res, clientId) {
  if (!clientAuthorized(req, clientId)) {
    sendJson(res, 401, { error: "unauthorized_client" });
    return;
  }

  const previous = state.clients.get(clientId);
  if (previous && previous.res !== res) previous.res.end();

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });
  res.flushHeaders?.();

  const now = new Date().toISOString();
  state.clients.set(clientId, { clientId, res, connectedAt: now, lastSeenAt: now });
  writeSse(res, "connected", { client_id: clientId, server_time: now });
  console.log(`[control] connected client=${clientId}`);

  const cleanup = () => {
    const current = state.clients.get(clientId);
    if (current?.res === res) state.clients.delete(clientId);
    console.log(`[control] disconnected client=${clientId}`);
  };
  req.on("close", cleanup);
}

async function receiveFile(req, res, transfer) {
  const token = bearerToken(req.headers.authorization);
  if (!token || !constantTimeEqual(token, transfer.uploadToken)) {
    sendJson(res, 401, { error: "invalid_upload_token" });
    return;
  }
  if (String(req.headers["x-client-id"] ?? "") !== transfer.clientId) {
    sendJson(res, 403, { error: "client_mismatch" });
    return;
  }
  if (Date.now() > new Date(transfer.expiresAt).getTime()) {
    state.updateTransfer(transfer.id, { status: "failed", error: "upload token expired" });
    sendJson(res, 410, { error: "transfer_expired" });
    return;
  }
  if (transfer.status === "completed") {
    sendJson(res, 409, { error: "transfer_already_completed" });
    return;
  }

  const declaredLength = Number(req.headers["content-length"] ?? 0);
  if (declaredLength > MAX_FILE_BYTES) {
    state.updateTransfer(transfer.id, { status: "failed", error: "file exceeds configured size limit" });
    sendJson(res, 413, { error: "file_too_large" });
    return;
  }

  const clientDir = path.join(DOWNLOAD_DIR, transfer.clientId);
  const finalPath = path.join(clientDir, `${transfer.id}.bin`);
  const tempPath = `${finalPath}.part`;
  await mkdir(clientDir, { recursive: true });

  const hash = createHash("sha256");
  let receivedBytes = 0;
  let tooLarge = false;
  req.on("data", (chunk) => {
    receivedBytes += chunk.length;
    hash.update(chunk);
    if (receivedBytes > MAX_FILE_BYTES && !tooLarge) {
      tooLarge = true;
      req.destroy(new Error("file exceeds configured size limit"));
    }
    // Update progress at roughly every 1 MiB boundary without buffering the file.
    if ((receivedBytes & ((1 << 20) - 1)) < chunk.length) {
      state.updateTransfer(transfer.id, { receivedBytes });
    }
  });

  state.updateTransfer(transfer.id, { status: "receiving", receivedBytes: 0 });

  try {
    await pipeline(req, createWriteStream(tempPath, { flags: "wx" }));
    if (declaredLength > 0 && declaredLength !== receivedBytes) {
      throw new Error(`content-length mismatch: declared=${declaredLength}, received=${receivedBytes}`);
    }

    const digest = hash.digest("hex");
    await rename(tempPath, finalPath);
    const completed = state.updateTransfer(transfer.id, {
      status: "completed",
      receivedBytes,
      sha256: digest,
      savedPath: finalPath,
      error: undefined,
    });
    sendJson(res, 201, { transfer: publicTransfer(completed) });
  } catch (error) {
    await unlink(tempPath).catch(() => undefined);
    const message = error instanceof Error ? error.message : "upload failed";
    state.updateTransfer(transfer.id, { status: "failed", receivedBytes, error: message });
    if (!res.headersSent) {
      sendJson(res, tooLarge ? 413 : 500, {
        error: tooLarge ? "file_too_large" : "upload_failed",
        message,
      });
    } else {
      res.destroy();
    }
  }
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    const method = req.method ?? "GET";

    if (method === "GET" && url.pathname === "/health") {
      sendJson(res, 200, { status: "ok", connected_clients: state.clients.size });
      return;
    }

    if (method === "GET" && url.pathname === "/client/events") {
      const clientId = url.searchParams.get("client_id") ?? "";
      if (!clientId) {
        sendJson(res, 400, { error: "client_id_required" });
        return;
      }
      registerClient(req, res, clientId);
      return;
    }

    if (method === "GET" && url.pathname === "/api/v1/clients") {
      if (!adminAuthorized(req)) return sendJson(res, 401, { error: "unauthorized" });
      sendJson(
        res,
        200,
        Array.from(state.clients.values()).map((client) => ({
          client_id: client.clientId,
          connected_at: client.connectedAt,
          last_seen_at: client.lastSeenAt,
        })),
      );
      return;
    }

    const triggerMatch = url.pathname.match(/^\/api\/v1\/clients\/([^/]+)\/downloads$/);
    if (method === "POST" && triggerMatch) {
      if (!adminAuthorized(req)) return sendJson(res, 401, { error: "unauthorized" });
      const clientId = decodeURIComponent(triggerMatch[1]);
      const client = state.clients.get(clientId);
      if (!client || client.res.writableEnded || client.res.destroyed) {
        sendJson(res, 409, { error: "client_not_connected", client_id: clientId });
        return;
      }

      const transfer = state.createTransfer(clientId, TRANSFER_TTL_MS);
      const command = {
        type: "download_file",
        transfer_id: transfer.id,
        upload_url: `${PUBLIC_BASE_URL}/api/v1/transfers/${transfer.id}/content`,
        upload_token: transfer.uploadToken,
        expires_at: transfer.expiresAt,
      };
      writeSse(client.res, "download_file", command);
      sendJson(res, 202, {
        transfer: publicTransfer(transfer),
        status_url: `/api/v1/transfers/${transfer.id}`,
        file_url: `/api/v1/transfers/${transfer.id}/file`,
      });
      return;
    }

    const statusPostMatch = url.pathname.match(/^\/api\/v1\/transfers\/([^/]+)\/client-status$/);
    if (method === "POST" && statusPostMatch) {
      const transfer = state.transfers.get(statusPostMatch[1]);
      if (!transfer) return sendJson(res, 404, { error: "transfer_not_found" });
      const clientId = String(req.headers["x-client-id"] ?? "");
      if (clientId !== transfer.clientId || !clientAuthorized(req, clientId)) {
        return sendJson(res, 401, { error: "unauthorized_client" });
      }
      const body = await readJson(req);
      if (body.status === "client_acknowledged") {
        state.updateTransfer(transfer.id, {
          status: "client_acknowledged",
          fileName: body.file_name,
          expectedBytes: body.file_size,
        });
      } else if (body.status === "failed") {
        state.updateTransfer(transfer.id, {
          status: "failed",
          error: body.error ?? "client reported failure",
        });
      } else {
        return sendJson(res, 400, { error: "invalid_status" });
      }
      sendJson(res, 200, { transfer: publicTransfer(state.transfers.get(transfer.id)) });
      return;
    }

    const contentMatch = url.pathname.match(/^\/api\/v1\/transfers\/([^/]+)\/content$/);
    if (method === "PUT" && contentMatch) {
      const transfer = state.transfers.get(contentMatch[1]);
      if (!transfer) return sendJson(res, 404, { error: "transfer_not_found" });
      await receiveFile(req, res, transfer);
      return;
    }

    const transferMatch = url.pathname.match(/^\/api\/v1\/transfers\/([^/]+)$/);
    if (method === "GET" && transferMatch) {
      if (!adminAuthorized(req)) return sendJson(res, 401, { error: "unauthorized" });
      const transfer = state.transfers.get(transferMatch[1]);
      if (!transfer) return sendJson(res, 404, { error: "transfer_not_found" });
      sendJson(res, 200, publicTransfer(transfer));
      return;
    }

    const fileMatch = url.pathname.match(/^\/api\/v1\/transfers\/([^/]+)\/file$/);
    if (method === "GET" && fileMatch) {
      if (!adminAuthorized(req)) return sendJson(res, 401, { error: "unauthorized" });
      const transfer = state.transfers.get(fileMatch[1]);
      if (!transfer) return sendJson(res, 404, { error: "transfer_not_found" });
      if (transfer.status !== "completed" || !transfer.savedPath) {
        return sendJson(res, 409, { error: "transfer_not_completed", status: transfer.status });
      }
      const info = await stat(transfer.savedPath);
      const downloadName = transfer.fileName ?? `${transfer.id}.bin`;
      res.writeHead(200, {
        "content-type": "application/octet-stream",
        "content-length": info.size,
        "content-disposition": `attachment; filename="${downloadName.replaceAll('"', '')}"`,
        "cache-control": "no-store",
      });
      await pipeline(createReadStream(transfer.savedPath), res);
      return;
    }

    sendJson(res, 404, { error: "not_found" });
  } catch (error) {
    console.error(error);
    if (!res.headersSent) sendJson(res, 500, { error: "internal_server_error" });
    else res.destroy();
  }
});

// SSE heartbeat keeps NAT/proxy state alive and lets the server observe dead sockets.
const heartbeat = setInterval(() => {
  const now = new Date().toISOString();
  for (const [clientId, client] of state.clients) {
    if (client.res.writableEnded || client.res.destroyed) {
      state.clients.delete(clientId);
      continue;
    }
    client.lastSeenAt = now;
    client.res.write(`: heartbeat ${now}\n\n`);
  }
}, 20_000);
heartbeat.unref();

server.listen(PORT, () => {
  console.log(`server listening on http://localhost:${PORT}`);
  console.log(`download directory: ${DOWNLOAD_DIR}`);
});

function shutdown() {
  clearInterval(heartbeat);
  for (const client of state.clients.values()) client.res.end();
  server.close(() => process.exit(0));
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
