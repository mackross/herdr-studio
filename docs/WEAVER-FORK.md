# Weaver support in this fork

Fork: <https://github.com/mackross/herdr-studio>
Upstream: <https://github.com/powerfooI/herdr-studio>

## Scope

- Local Weaver History (including incremental History), messages, tool calls
  and results, Session Inspector, and ATIF export.
- Reads `~/.weaver/sessions/<Go-PathEscape(cwd)>/weaver.sqlite3` using the
  native session ID reported by Herdr. Uses the foreground CWD, not the
  workspace root, and never guesses a latest session.
- Read-only SQLite transactions include committed SQLite WAL data. Checkpoint
  versions 1 and 2 and the application WAL tail are projected into a transcript.
  Unknown WAL operations, missing sequences, and unsafe mid-stream checkpoints
  fail explicitly instead of silently showing incomplete history.
- Export raw produces **selected-session projected JSONL**, not a SQLite backup.
  The displayed session file is a virtual resource below the database path.
  Other sessions and opaque provider/recovery state are not exported.
- Weaver does not currently persist per-message timestamps or token totals in
  these records. Timestamps use the stable session-start fallback; token totals
  remain unavailable rather than being invented.
- SSH Weaver inspection is not implemented. Remote connections fail explicitly
  rather than looking at the Studio server's local home directory.
- A 32 MiB input budget bounds a single transcript preview/export. Very large
  sessions require a future paged native reader.

## Last step is independent

Changes > Last step captures Git trees when workspace activity changes from
quiet to working and back to quiet. It does not read native transcripts or
resume commands. It needs Studio to observe a full activity cycle, and multiple
working panes in the same workspace share that cycle. Supported lifecycle
reporting in `mackross/herdr` supplies Weaver's status transitions.

The Herdr `weaver-support` branch adds `weaver --session <id>` restore and
session-replacement handling. Its Rust tests and binaries are built with GitHub
Actions, not on this development machine.

## Build and maintain

Until fork-specific release archives are published, build from source:

```sh
bun install --frozen-lockfile
(cd web && bun install --frozen-lockfile)
(cd server && bun install --frozen-lockfile)
bun run precommit
bun run build:darwin-arm64
```

The plugin downloader and application updater point at `mackross/herdr-studio`
so an upstream binary cannot silently remove Weaver support. Do not run the
plugin's download build until a matching fork release exists; use
`bun scripts/studio-plugin.ts build-source` instead.

Keep `upstream` pointed at the original repository and `origin` at the fork.
Bring upstream changes into a review branch, run CI, and merge through a fork PR.
Changes here are intentionally confined to the session adapter, fork update
URLs, and regression coverage so future upstream merges remain manageable.
