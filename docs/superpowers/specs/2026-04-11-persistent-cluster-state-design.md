# Persistent Cluster State Design

## Problem

Cluster state (`Metadata`, `IndexMetadata`) is stored purely in-memory via `InMemoryPersistedState`. On process exit, all index metadata is lost even though index data files remain on disk. On restart, indices must be recreated via API calls.

## Goal

Persist cluster state to disk so that a node automatically recovers all indices on restart. Mirror Elasticsearch's `GatewayMetaState` pattern, simplified to JSON file-based storage.

## Approach

JSON file-based persistence with atomic writes. The existing `PersistedState` interface provides the abstraction layer — a new `FilePersistedState` implementation writes to disk while the interface remains unchanged for future backends (Lucene-based, network-backed, etc.).

## Architecture

```
Node startup:
  GatewayMetaState.Start()
    -> FilePersistedState.Load()             (read JSON from disk)
    -> for each index in metadata:
        -> index.NewIndexService(meta, ...)   (reopen shards)
    -> populate indexServices map

Runtime mutation:
  ClusterState.UpdateMetadata(fn)
    -> fn(current) -> new metadata
    -> FilePersistedState.SetMetadata()
      -> serialize to JSON
      -> write tmp file + atomic rename
```

## File Format & Storage

**Location:** `{dataPath}/_state/cluster_state.json`

**Format:**

```json
{
  "version": 3,
  "metadata": {
    "indices": {
      "my_index": {
        "name": "my_index",
        "state": "open",
        "settings": {
          "number_of_shards": 1,
          "number_of_replicas": 0,
          "refresh_interval": "1s"
        },
        "mapping": {
          "properties": {
            "title": { "type": "text", "analyzer": "standard" },
            "count": { "type": "integer" }
          }
        }
      }
    }
  }
}
```

Key decisions:

- **Version field** — monotonically increasing counter, incremented on each `SetMetadata()` call. Used for corruption detection and future distributed consensus.
- **Atomic writes** — write to `cluster_state.tmp` then `os.Rename()` to `cluster_state.json`. Rename is atomic on POSIX, so a crash mid-write won't corrupt the state file.
- **Human-readable** — JSON for easy inspection/debugging at this project stage.
- **`FieldType` and `IndexState`** — serialized as strings (`"text"`, `"integer"`, `"open"`, `"closed"`). Custom `MarshalJSON`/`UnmarshalJSON` on those types.
- **`RefreshInterval`** — serialized as a duration string (`"1s"`, `"-1"`).

## Components

### No changes to existing interfaces

- `PersistedState` interface stays as-is (`GetMetadata()` / `SetMetadata()`)
- `ClusterState` stays as-is — already delegates to `PersistedState`

### New components

| Component | File | Responsibility |
|-----------|------|----------------|
| `FilePersistedState` | `server/cluster/file_persisted_state.go` | Implements `PersistedState`. Loads from / writes to JSON file atomically. Tracks version. |
| `GatewayMetaState` | `server/gateway/gateway_meta_state.go` | Mirrors ES `GatewayMetaState`. Loads persisted state, recovers index services, wires into node. |

### `FilePersistedState`

```go
type FilePersistedState struct {
    stateDir string      // {dataPath}/_state/
    metadata *Metadata
    version  int64       // monotonically increasing
}
```

- `Load()` — reads JSON from disk, deserializes, returns error if corrupt
- `SetMetadata()` — increments version, serializes, atomic write
- `GetMetadata()` — returns in-memory pointer (same as `InMemoryPersistedState`)

### `GatewayMetaState`

```go
type GatewayMetaState struct {
    persistedState *cluster.FilePersistedState
}
```

- `Start(dataPath, registry) -> (*cluster.ClusterState, map[string]*index.IndexService, error)`
- Loads persisted state, iterates indices, calls `index.NewIndexService()` for each, returns fully reconstructed state

### Node startup change

```go
// Before:
cs := cluster.NewClusterState()  // empty, in-memory

// After:
gw := gateway.NewGatewayMetaState()
cs, indexServices, err := gw.Start(config.DataPath, registry)
```

## Recovery Flow

**Happy path:**

1. `GatewayMetaState.Start()` checks if `{dataPath}/_state/cluster_state.json` exists
2. If exists: `FilePersistedState.Load()` reads and deserializes
3. If not exists: creates `FilePersistedState` with empty `Metadata` (fresh node)
4. For each index in loaded metadata:
   - Compute `indexDataPath` = `{dataPath}/nodes/0/indices/{indexName}`
   - Call `index.NewIndexService(meta, mapping, indexDataPath, registry)`
   - Add to `indexServices` map
5. Create `ClusterState` with the loaded `FilePersistedState`
6. Return `(clusterState, indexServices, nil)`

**Error scenarios:**

| Scenario | Behavior |
|----------|----------|
| State file missing | Fresh start — empty metadata, no recovery |
| State file corrupt (invalid JSON) | Return error, node refuses to start (mirrors ES `CorruptStateException`) |
| State file exists but index data dir missing | Skip that index, log warning. Remove from metadata and persist cleaned state. |
| Index shard fails to reopen | Return error for that index. Node logs error and continues with remaining indices (partial recovery). |
| Temp file exists on startup (`cluster_state.tmp`) | Delete it — indicates crash during previous write. The `.json` file is the last good state. |

**Key principle:** Fail loud on metadata corruption (source of truth is broken). Be lenient on missing index data (can be cleaned up).

## Testing Strategy

| Test | Scope | What it verifies |
|------|-------|------------------|
| `FilePersistedState` unit tests | `server/cluster/` | Roundtrip write/read. Atomic write safety. Version incrementing. Empty state handling. |
| `GatewayMetaState` unit tests | `server/gateway/` | Recovery with 0, 1, N indices. Missing index data dir -> skip + cleanup. Corrupt state file -> error. Stale tmp file -> cleanup. |
| JSON serialization tests | `server/cluster/` | `FieldType`, `IndexState`, `RefreshInterval` marshal/unmarshal as strings. Full `Metadata` roundtrip. |
| Node integration test | `server/node/` | Create index -> stop node -> start new node with same `dataPath` -> index is recovered and queryable. |

All tests use `t.TempDir()` per project convention.
