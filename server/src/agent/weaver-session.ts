import { Database } from "bun:sqlite";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";
import type { AgentSessionFileAccess } from "./session-file-access";
import type { SessionFile } from "./session-types";
import { isRecord } from "./session-utils";

// This is a read-only transcript projection, not an execution-state restore.
// Keep SQLite (including its live WAL) on the host that owns the agent.
const MAX_TRANSCRIPT_BYTES = 32 * 1024 * 1024;
type Item = Record<string, unknown>;
type TailEvent = { seq: number; op: string; event_json: string };

export function weaverDatabasePath(cwd: string, home = homedir()) {
  if (!isAbsolute(cwd)) throw new Error("Weaver requires an absolute pane CWD");
  // Go net/url.PathEscape (encodePathSegment), used by Weaver's session store.
  const escaped = Array.from(Buffer.from(cwd), (byte) => {
    const char = String.fromCharCode(byte);
    return /[A-Za-z0-9\-_.~$&+=:@]/.test(char)
      ? char
      : `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }).join("");
  return join(home, ".weaver", "sessions", escaped, "weaver.sqlite3");
}

function timestamp(value: string) {
  // Weaver uses filesystem-safe RFC3339 timestamps with '-' in the time part.
  const iso = value.replace(/T(\d{2})-(\d{2})-(\d{2})/, "T$1:$2:$3");
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) throw new Error("Invalid Weaver session timestamp");
  return ms;
}

export function projectWeaverItems(snapshot: unknown, tail: TailEvent[]) {
  if (
    !isRecord(snapshot) ||
    (snapshot.ver !== 1 && snapshot.ver !== 2) ||
    (snapshot.items !== null && !Array.isArray(snapshot.items))
  ) {
    throw new Error("Unsupported Weaver checkpoint format");
  }
  if (snapshot.stream !== -1) {
    throw new Error(
      "Weaver checkpoint is mid-stream; retry after a safe checkpoint",
    );
  }
  const items: Item[] = (snapshot.items ?? []).map((item: unknown) => {
    if (!isRecord(item)) throw new Error("Invalid Weaver checkpoint item");
    return { ...item };
  });
  let streaming = false;
  let streamStart = items.length;
  let queued: Item[] = [];
  const calls = new Map<string, Item>();
  for (const row of tail) {
    const event: unknown = JSON.parse(row.event_json);
    if (!isRecord(event) || event.s !== row.seq || event.o !== row.op) {
      throw new Error("Invalid Weaver WAL event");
    }
    if (row.op === "begin_stream") {
      if (!streaming) streamStart = items.length;
      streaming = true;
      continue;
    }
    if (row.op === "end_stream") {
      streaming = false;
      calls.clear();
      items.push(...queued);
      queued = [];
      continue;
    }
    // Metadata does not change the displayed conversation.
    if (row.op === "patch_item_metadata") continue;
    if (
      !["queue_item", "queue_item_before_send", "append_stream_item"].includes(
        row.op,
      )
    ) {
      throw new Error(`Unsupported Weaver WAL operation: ${row.op}`);
    }
    if (!isRecord(event.i)) throw new Error("Invalid Weaver WAL item");
    const item = { ...event.i };
    if (row.op !== "append_stream_item") {
      if (streaming) queued.push(item);
      else items.push(item);
      continue;
    }
    if (!streaming)
      throw new Error("Weaver stream item without stream boundary");
    const last = items.at(-1);
    if (
      item.kind === "assistant_text" &&
      items.length > streamStart &&
      last?.kind === "assistant_text" &&
      !last.meta &&
      !item.meta
    ) {
      last.text = String(last.text ?? "") + String(item.text ?? "");
      continue;
    }
    if (
      (item.kind === "tool_call_chunk" || item.kind === "tool_call") &&
      typeof item.id === "string"
    ) {
      const prior = calls.get(item.id);
      if (prior) {
        if (item.kind === "tool_call") Object.assign(prior, item);
        else prior.args = String(prior.args ?? "") + String(item.args ?? "");
        continue;
      }
      calls.set(item.id, item);
    }
    items.push(item);
  }
  items.push(...queued);
  // Do not expose provider opaque state or recovery/control records.
  return items
    .filter((item) =>
      [
        "user_text",
        "assistant_text",
        "assistant_instruction",
        "tool_call",
        "tool_result",
      ].includes(String(item.kind)),
    )
    .map((item) => {
      const result: Item = { kind: item.kind };
      for (const key of ["text", "id", "name", "args", "output"]) {
        if (typeof item[key] === "string") result[key] = item[key];
      }
      return result;
    });
}

export function readWeaverSession(databasePath: string, sessionId: string) {
  const db = new Database(databasePath, { readonly: true });
  try {
    db.exec("PRAGMA query_only = ON; PRAGMA busy_timeout = 1000; BEGIN");
    const budget = db
      .query(`SELECT length(CAST(c.snapshot_json AS BLOB)) +
      COALESCE((SELECT SUM(length(CAST(e.event_json AS BLOB))) FROM thread_wal_events e
        WHERE e.branch_id = s.branch_id AND e.seq > c.seq), 0) AS bytes
      FROM sessions s JOIN thread_checkpoints c ON c.branch_id = s.branch_id
      WHERE s.id = ?`)
      .get(sessionId) as { bytes: number } | null;
    if (budget && budget.bytes > MAX_TRANSCRIPT_BYTES) {
      throw new Error("Weaver transcript exceeds 32 MB preview limit");
    }
    const row = db
      .query(`SELECT s.model, s.started_at, b.updated_at,
      b.last_seq, c.seq, c.snapshot_json FROM sessions s
      JOIN thread_branches b ON b.id = s.branch_id
      JOIN thread_checkpoints c ON c.branch_id = b.id WHERE s.id = ?`)
      .get(sessionId) as {
      model: string;
      started_at: string;
      updated_at: string;
      last_seq: number;
      seq: number;
      snapshot_json: string;
    } | null;
    if (!row) return null;
    const tail = db
      .query(`SELECT e.seq, e.op, e.event_json FROM thread_wal_events e
      JOIN sessions s ON s.branch_id = e.branch_id
      WHERE s.id = ? AND e.seq > ? ORDER BY e.seq`)
      .all(sessionId, row.seq) as TailEvent[];
    let expected = row.seq + 1;
    let bytes = Buffer.byteLength(row.snapshot_json);
    for (const event of tail) {
      if (event.seq !== expected++)
        throw new Error("Incomplete Weaver WAL tail");
      bytes += Buffer.byteLength(event.event_json);
    }
    if (expected - 1 !== row.last_seq)
      throw new Error("Incomplete Weaver WAL tail");
    if (bytes > MAX_TRANSCRIPT_BYTES)
      throw new Error("Weaver transcript exceeds 32 MB preview limit");
    const records = projectWeaverItems(JSON.parse(row.snapshot_json), tail);
    const text =
      records.map((record) => JSON.stringify(record)).join("\n") + "\n";
    return {
      text,
      model: row.model,
      createdAtMs: timestamp(row.started_at),
      mtimeMs: timestamp(row.updated_at),
      revision: `${row.seq}:${row.last_seq}`,
    };
  } finally {
    db.close();
  }
}

// Virtual JSONL resources let all existing History, ATIF, preview and download
// paths share the same selected-session projection. No SQLite file is exported.
export function withWeaverSessions(
  base: AgentSessionFileAccess,
  home = homedir(),
): AgentSessionFileAccess {
  const resources = new Map<string, { database: string; id: string }>();
  function read(path: string) {
    const resource = resources.get(path);
    if (!resource) return null;
    const session = readWeaverSession(resource.database, resource.id);
    if (!session) throw new Error("Weaver session disappeared");
    return session;
  }
  function descriptor(path: string): SessionFile | null {
    const session = read(path);
    if (!session) return null;
    return {
      path,
      mtimeMs: session.mtimeMs,
      createdAtMs: session.createdAtMs,
      size: Buffer.byteLength(session.text),
      changeToken: session.revision,
      sessionId: resources.get(path)!.id,
      modelName: session.model,
      identity: `${path}:${session.createdAtMs}`,
    };
  }
  return {
    ...base,
    async findWeaverSession(id, cwd) {
      if (base.remote)
        throw new Error(
          "Weaver session inspection currently supports local connections only",
        );
      const database = weaverDatabasePath(cwd, home);
      if (!(await base.statFile(database))) return null;
      if (!readWeaverSession(database, id)) return null;
      const path = `${database}/session-${encodeURIComponent(id)}.jsonl`;
      resources.set(path, { database, id });
      // Bound descriptors retained by a long-lived connection.
      if (resources.size > 128)
        resources.delete(resources.keys().next().value!);
      return descriptor(path);
    },
    async statFile(path) {
      return resources.has(path) ? descriptor(path) : base.statFile(path);
    },
    async readText(path) {
      return read(path)?.text ?? base.readText(path);
    },
    async readPrefix(path, limit) {
      const session = read(path);
      return session
        ? Buffer.from(session.text).subarray(0, limit)
        : base.readPrefix(path, limit);
    },
    async readDownloadBody(path) {
      return read(path)?.text ?? base.readDownloadBody(path);
    },
  };
}
