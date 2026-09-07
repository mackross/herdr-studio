import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createAgentSessionHandlers } from "./agent-sessions";
import { localAgentSessionFiles } from "./session-file-access";
import { resolveAgentSession } from "./session-resolver";
import {
  projectWeaverItems,
  readWeaverSession,
  weaverDatabasePath,
  withWeaverSessions,
} from "./weaver-session";

const homes: string[] = [];
afterEach(() => {
  for (const home of homes.splice(0))
    rmSync(home, { recursive: true, force: true });
});
const time = "2026-09-07T20-43-15.167011000Z";
const snapshot = {
  ver: 1,
  stream: -1,
  items: [{ kind: "user_text", text: "Hello" }],
};
function fixture() {
  const home = mkdtempSync(join(tmpdir(), "studio-weaver-"));
  homes.push(home);
  const cwd = "/workspace/a b&c+日本";
  const path = weaverDatabasePath(cwd, home);
  mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path);
  db.exec(`PRAGMA journal_mode=WAL;
    CREATE TABLE sessions(id TEXT PRIMARY KEY, branch_id TEXT, model TEXT, started_at TEXT);
    CREATE TABLE thread_branches(id TEXT PRIMARY KEY, updated_at TEXT, last_seq INTEGER);
    CREATE TABLE thread_checkpoints(branch_id TEXT, seq INTEGER, snapshot_json TEXT);
    CREATE TABLE thread_wal_events(branch_id TEXT, seq INTEGER, op TEXT, event_json TEXT);`);
  db.query("INSERT INTO sessions VALUES (?, ?, ?, ?)").run(
    "s1",
    "b1",
    "test-model",
    time,
  );
  db.query("INSERT INTO thread_branches VALUES (?, ?, ?)").run("b1", time, 1);
  db.query("INSERT INTO thread_checkpoints VALUES (?, ?, ?)").run(
    "b1",
    1,
    JSON.stringify(snapshot),
  );
  let seq = 1;
  function append(op: string, item: Record<string, unknown> = {}) {
    seq++;
    db.query("INSERT INTO thread_wal_events VALUES (?, ?, ?, ?)").run(
      "b1",
      seq,
      op,
      JSON.stringify({ s: seq, o: op, i: item }),
    );
    db.query("UPDATE thread_branches SET last_seq = ? WHERE id = 'b1'").run(
      seq,
    );
  }
  const files = withWeaverSessions(localAgentSessionFiles, home);
  const herdrCall = async () => ({
    agent: {
      agent: "weaver",
      pane_id: "w1:p1",
      cwd: "/wrong",
      foreground_cwd: cwd,
      agent_session: {
        source: "custom:weaver",
        agent: "weaver",
        kind: "id",
        value: "s1",
      },
    },
  });
  return { path, db, append, files, herdrCall };
}

describe("Weaver sessions", () => {
  test("matches Go PathEscape, including UTF-8 and reserved punctuation", () => {
    expect(
      weaverDatabasePath("/a b&c+d=e:f@g$h,!'()*/日本", "/home/test"),
    ).toBe(
      "/home/test/.weaver/sessions/%2Fa%20b&c+d=e:f@g$h%2C%21%27%28%29%2A%2F%E6%97%A5%E6%9C%AC/weaver.sqlite3",
    );
    expect(() => weaverDatabasePath("relative")).toThrow("absolute");
  });
  test("reads a live SQLite WAL, merges chunks, and exports only the selected session", async () => {
    const f = fixture();
    try {
      f.append("begin_stream");
      f.append("append_stream_item", { kind: "assistant_text", text: "Hi " });
      f.append("append_stream_item", { kind: "assistant_text", text: "there" });
      f.append("append_stream_item", {
        kind: "reasoning",
        opaque: "SECRET PROVIDER STATE",
      });
      f.append("append_stream_item", {
        kind: "tool_call_chunk",
        id: "c1",
        name: "edit",
        args: "{",
      });
      f.append("append_stream_item", {
        kind: "tool_call",
        id: "c1",
        name: "edit",
        args: '{"path":"a.go"}',
      });
      f.append("end_stream");
      f.append("queue_item", {
        kind: "tool_result",
        id: "c1",
        output: "Edited a.go",
      });
      const handlers = createAgentSessionHandlers({
        files: f.files,
        herdrCall: f.herdrCall,
      });
      const params = { pane_id: "w1:p1" };
      const history = await handlers.readHistory(params);
      if (!("messages" in history)) throw new Error("Expected legacy History");
      expect(history.messages.map((m) => m.text)).toEqual([
        "Hello",
        "Hi there",
      ]);
      const summary = await handlers.readSummary({
        ...params,
        include_trajectory: true,
        include_text: true,
      });
      expect(summary.stats.token_usage).toBeNull();
      expect(summary.trajectory?.agent.name).toBe("weaver");
      expect(
        summary.trajectory?.steps.flatMap((s) => s.tool_calls ?? []),
      ).toEqual([
        {
          tool_call_id: "c1",
          function_name: "edit",
          arguments: { path: "a.go" },
        },
      ]);
      expect(summary.text).not.toContain("SECRET PROVIDER STATE");
      const incremental = await handlers.readHistory({
        ...params,
        history_version: 2,
      });
      if (!("entries" in incremental))
        throw new Error("Expected History v2 snapshot");
      expect(incremental.entries.length).toBeGreaterThan(2);
      const atif = await handlers.downloadAtif(params);
      expect((await atif.json()).agent.name).toBe("weaver");
      const response = await handlers.downloadFile(params);
      const body = await response.text();
      expect(body).toContain("Hi there");
      expect(body).not.toContain("SQLite");
      expect(Number(response.headers.get("content-length"))).toBe(
        Buffer.byteLength(body),
      );
      // New messages are visible without checkpointing the application or SQLite.
      f.append("queue_item", { kind: "user_text", text: "Next" });
      const next = await handlers.readHistory(params);
      if (!("messages" in next)) throw new Error("Expected legacy History");
      expect(next.messages.at(-1)?.text).toBe("Next");
      expect(next.file?.changeToken).not.toBe(history.file?.changeToken);
      expect(readWeaverSession(f.path, "s1' OR 1=1 --")).toBeNull();
      expect(f.db.query("SELECT COUNT(*) AS n FROM sessions").get()).toEqual({
        n: 1,
      });
    } finally {
      f.db.close();
    }
  });
  test("missing session and remote connections fail clearly", async () => {
    const missing = await resolveAgentSession({ pane_id: "p1" }, async () => ({
      agent: { agent: "weaver" },
    }));
    expect(missing.status).toBe("missing_session");
    expect(missing.command).toBeUndefined();
    const f = fixture();
    try {
      await expect(
        resolveAgentSession({ pane_id: "p1" }, f.herdrCall, {
          ...f.files,
          remote: true,
        }),
      ).rejects.toThrow("local connections only");
      expect(
        await f.files.findWeaverSession!("missing", "/missing-cwd"),
      ).toBeNull();
    } finally {
      f.db.close();
    }
  });
  test("supports version 2 and empty checkpoints", () => {
    expect(projectWeaverItems({ ver: 2, stream: -1, items: null }, [])).toEqual(
      [],
    );
    expect(projectWeaverItems({ ...snapshot, ver: 2 }, [])).toEqual(
      snapshot.items,
    );
  });
  test("rejects corrupt or unsupported durable data rather than displaying partial history", () => {
    expect(() => projectWeaverItems({ ...snapshot, ver: 99 }, [])).toThrow(
      "Unsupported",
    );
    const f = fixture();
    try {
      f.db.exec("UPDATE thread_branches SET last_seq = 10");
      expect(() => readWeaverSession(f.path, "s1")).toThrow("Incomplete");
    } finally {
      f.db.close();
    }
  });
});
