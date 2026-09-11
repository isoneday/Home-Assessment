import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import http from "node:http";
import https from "node:https";
import os from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";

const SERVER_URL = (process.env.SERVER_URL ?? "http://localhost:8080").replace(/\/$/, "");
const CLIENT_ID = process.env.CLIENT_ID ?? "restaurant-001";
const CLIENT_TOKEN = process.env.CLIENT_TOKEN ?? "client-secret-001";
const FILE_PATH = process.env.FILE_PATH ?? path.join(os.homedir(), "file_to_download.txt");

let stopping = false;
let currentRequest;
const activeTransfers = new Set();

function transportFor(url) {
  return url.protocol === "https:" ? https : http;
}

async function postClientStatus(transferId, body) {
  const url = new URL(`${SERVER_URL}/api/v1/transfers/${transferId}/client-status`);
  const payload = Buffer.from(JSON.stringify(body));
  await new Promise((resolve, reject) => {
    const req = transportFor(url).request(
      url,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${CLIENT_TOKEN}`,
          "X-Client-Id": CLIENT_ID,
          "Content-Type": "application/json",
          "Content-Length": payload.length,
        },
        timeout: 15_000,
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          if ((res.statusCode ?? 500) >= 200 && (res.statusCode ?? 500) < 300) resolve();
          else reject(new Error(`status update failed: ${res.statusCode} ${Buffer.concat(chunks)}`));
        });
      },
    );
    req.on("timeout", () => req.destroy(new Error("status update timed out")));
    req.on("error", reject);
    req.end(payload);
  });
}

async function uploadFile(command) {
  if (activeTransfers.has(command.transfer_id)) return;
  activeTransfers.add(command.transfer_id);
  try {
    if (Date.now() > new Date(command.expires_at).getTime()) {
      throw new Error("download command expired before upload started");
    }

    const info = await stat(FILE_PATH);
    if (!info.isFile()) throw new Error(`${FILE_PATH} is not a regular file`);

    await postClientStatus(command.transfer_id, {
      status: "client_acknowledged",
      file_name: path.basename(FILE_PATH),
      file_size: info.size,
    });

    console.log(`[transfer ${command.transfer_id}] streaming ${info.size} bytes from ${FILE_PATH}`);
    const url = new URL(command.upload_url);
    await new Promise((resolve, reject) => {
      const req = transportFor(url).request(
        url,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${command.upload_token}`,
            "X-Client-Id": CLIENT_ID,
            "Content-Type": "application/octet-stream",
            "Content-Length": info.size,
          },
          timeout: 5 * 60 * 1000,
        },
        (res) => {
          const chunks = [];
          res.on("data", (chunk) => chunks.push(chunk));
          res.on("end", () => {
            const body = Buffer.concat(chunks).toString("utf8");
            if ((res.statusCode ?? 500) >= 200 && (res.statusCode ?? 500) < 300) resolve();
            else reject(new Error(`upload failed: ${res.statusCode} ${body}`));
          });
        },
      );
      req.on("timeout", () => req.destroy(new Error("upload timed out")));
      req.on("error", reject);
      pipeline(createReadStream(FILE_PATH, { highWaterMark: 256 * 1024 }), req).catch(reject);
    });
    console.log(`[transfer ${command.transfer_id}] completed`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[transfer ${command.transfer_id}] failed: ${message}`);
    await postClientStatus(command.transfer_id, { status: "failed", error: message }).catch(() => undefined);
  } finally {
    activeTransfers.delete(command.transfer_id);
  }
}

async function connectOnce() {
  const url = new URL(`${SERVER_URL}/client/events`);
  url.searchParams.set("client_id", CLIENT_ID);

  return new Promise((resolve, reject) => {
    const req = transportFor(url).request(
      url,
      {
        method: "GET",
        headers: {
          Accept: "text/event-stream",
          Authorization: `Bearer ${CLIENT_TOKEN}`,
          "Cache-Control": "no-cache",
        },
        timeout: 0,
      },
      (res) => {
        if (res.statusCode !== 200) {
          const chunks = [];
          res.on("data", (chunk) => chunks.push(chunk));
          res.on("end", () => reject(new Error(`control connection rejected: ${res.statusCode} ${Buffer.concat(chunks)}`)));
          return;
        }

        console.log(`[control] connected as ${CLIENT_ID}`);
        let buffer = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          buffer += chunk;
          while (true) {
            const boundary = buffer.indexOf("\n\n");
            if (boundary === -1) break;
            const frame = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            const dataLines = frame
              .split("\n")
              .filter((line) => line.startsWith("data: "))
              .map((line) => line.slice(6));
            if (dataLines.length === 0) continue;
            try {
              const message = JSON.parse(dataLines.join("\n"));
              if (message.type === "download_file") void uploadFile(message);
            } catch (error) {
              console.warn("[control] ignored invalid event", error);
            }
          }
        });
        res.on("end", resolve);
        res.on("close", resolve);
        res.on("error", reject);
      },
    );
    currentRequest = req;
    req.on("error", reject);
    req.end();
  });
}

async function main() {
  let attempt = 0;
  console.log(`client_id=${CLIENT_ID}`);
  console.log(`file_path=${FILE_PATH}`);
  while (!stopping) {
    try {
      await connectOnce();
      attempt = 0;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[control] connection failed: ${message}`);
      attempt += 1;
    }
    if (stopping) break;
    const delay = Math.min(30_000, 1000 * 2 ** Math.min(attempt, 5)) + Math.floor(Math.random() * 500);
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
}

function stop() {
  stopping = true;
  currentRequest?.destroy();
}
process.on("SIGINT", stop);
process.on("SIGTERM", stop);

void main();
