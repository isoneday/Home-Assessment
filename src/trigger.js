import { createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const args = process.argv.slice(2);
const clientId = args.find((arg) => !arg.startsWith("--")) ?? "restaurant-001";
const SERVER_URL = (process.env.SERVER_URL ?? "http://localhost:8080").replace(/\/$/, "");
const ADMIN_API_KEY = process.env.ADMIN_API_KEY ?? "admin-secret";
const wait = !args.includes("--no-wait");
const output = args.find((arg) => arg.startsWith("--output="))?.slice("--output=".length);

async function api(pathname, init = {}) {
  const response = await fetch(`${SERVER_URL}${pathname}`, {
    ...init,
    headers: { "X-API-Key": ADMIN_API_KEY, ...(init.headers ?? {}) },
  });
  if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  return response;
}

const started = await api(`/api/v1/clients/${encodeURIComponent(clientId)}/downloads`, { method: "POST" });
const body = await started.json();
console.log(`transfer_id=${body.transfer.id}`);
console.log(`status=${body.transfer.status}`);

if (!wait) process.exit(0);

while (true) {
  await new Promise((resolve) => setTimeout(resolve, 500));
  const response = await api(body.status_url);
  const transfer = await response.json();
  const progress = transfer.expectedBytes
    ? `${transfer.receivedBytes}/${transfer.expectedBytes}`
    : String(transfer.receivedBytes ?? 0);
  process.stdout.write(`\rstatus=${transfer.status} bytes=${progress}    `);

  if (transfer.status === "completed") {
    process.stdout.write("\n");
    console.log(`sha256=${transfer.sha256}`);
    console.log(`server_saved_path=${transfer.savedPath}`);
    if (output) {
      const fileResponse = await api(body.file_url);
      if (!fileResponse.body) throw new Error("file response did not contain a body");
      await pipeline(Readable.fromWeb(fileResponse.body), createWriteStream(output));
      console.log(`copied_from_server_to=${output}`);
    }
    break;
  }
  if (transfer.status === "failed") {
    process.stdout.write("\n");
    throw new Error(`transfer failed: ${transfer.error}`);
  }
}
