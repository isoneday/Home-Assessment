import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const target = process.env.FILE_PATH ?? path.join(os.homedir(), "file_to_download.txt");
const sizeMb = Number(process.env.FILE_SIZE_MB ?? 100);
const totalBytes = sizeMb * 1024 * 1024;
const chunk = Buffer.alloc(1024 * 1024, "SILENTMODE-HOME-ASSIGNMENT\n");

await mkdir(path.dirname(target), { recursive: true });
const out = createWriteStream(target);
let written = 0;
while (written < totalBytes) {
  const remaining = totalBytes - written;
  const next = remaining >= chunk.length ? chunk : chunk.subarray(0, remaining);
  if (!out.write(next)) await new Promise((resolve) => out.once("drain", resolve));
  written += next.length;
}
await new Promise((resolve, reject) => out.end((error) => (error ? reject(error) : resolve())));
console.log(`generated ${written} bytes at ${target}`);
