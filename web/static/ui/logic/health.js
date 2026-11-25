const SEND_ERROR_MS = 30000;
const LAG_HELD_MS = 10000;
const ACTIVE_MS = 5000;

// Lag is unknown, not zero, while either offset is unknown.
export function lagOf(end, committed) {
  if (end === undefined || end === null || end < 0) return null;
  if (committed === undefined || committed === null || committed < 0) return null;
  return Math.max(0, end - committed);
}

// An empty docker state means unknown, so nothing is called dead without evidence.
export const isDead = (broker) => Boolean(broker && broker.state && broker.state !== "running");

export function partitionHealth({ partition: p, brokers, minIsr }) {
  const byId = new Map((brokers || []).map((b) => [b.id, b]));
  if (p.leader < 0 || isDead(byId.get(p.leader))) return "red";
  if (minIsr > 0 && p.isr.length < minIsr) return "red";
  const weak = p.replicas.some((r) => !p.isr.includes(r) || isDead(byId.get(r)));
  return weak ? "yellow" : "ok";
}

export function edgeColor({ partitionHealth: health, lastSend, lagSince, members, lastChangeAt, now }) {
  if (health === "red") return "red";
  if (lastSend && !lastSend.ok && now - lastSend.at <= SEND_ERROR_MS) return "red";
  if (health === "yellow") return "yellow";
  if (lagSince !== null && lagSince !== undefined && members > 0 && now - lagSince >= LAG_HELD_MS) return "yellow";
  if (lastChangeAt !== null && lastChangeAt !== undefined && now - lastChangeAt <= ACTIVE_MS) return "blue";
  return "idle";
}
