import { randomBytes, randomUUID } from "node:crypto";

export class AppState {
  constructor() {
    /** @type {Map<string, {clientId:string,res:import('node:http').ServerResponse,connectedAt:string,lastSeenAt:string}>} */
    this.clients = new Map();
    /** @type {Map<string, any>} */
    this.transfers = new Map();
  }

  createTransfer(clientId, ttlMs) {
    const now = new Date();
    const transfer = {
      id: randomUUID(),
      clientId,
      status: "requested",
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
      expiresAt: new Date(now.getTime() + ttlMs).toISOString(),
      uploadToken: randomBytes(32).toString("base64url"),
      receivedBytes: 0,
    };
    this.transfers.set(transfer.id, transfer);
    return transfer;
  }

  updateTransfer(id, patch) {
    const current = this.transfers.get(id);
    if (!current) return undefined;
    const updated = { ...current, ...patch, updatedAt: new Date().toISOString() };
    this.transfers.set(id, updated);
    return updated;
  }
}

export function publicTransfer(transfer) {
  const { uploadToken: _secret, ...safe } = transfer;
  return safe;
}
