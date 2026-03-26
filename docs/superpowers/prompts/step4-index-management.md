# Step 4: Index Management Actions + REST

## What to do

Implement Step 4 from the design doc (`docs/elasticsearch-alt-design.md`) and progress tracker (`docs/progress.md`). This step wires up index creation, deletion, and retrieval end-to-end through REST → Transport Action → Index Service layers.

**Do NOT commit changes — I will review them first.**

## Prior art

- Steps 1-3 are complete. Read `docs/progress.md` for status.
- The design doc has full specifications for all types, logic, and REST endpoints.
- Use the `superpowers:writing-plans` skill to create a plan before implementing, then use `superpowers:subagent-driven-development` to execute it.

## Deliverable

`PUT /myindex`, `GET /myindex`, `DELETE /myindex` work via curl against a running Node.

## What needs to be built

### 1. Transport Actions (`server/action/`)

Create the `server/action/` package with three transport actions:

**TransportCreateIndexAction:**
- Request: `{Name, Settings, Mappings}`
- Response: `{Acknowledged, Index}`
- Logic: validate index name (not empty, no special chars, not duplicate) → build IndexMetadata → update ClusterState → create IndexService → register in node's index service map
- Needs access to: ClusterState, index service map, data path, analyzer

**TransportDeleteIndexAction:**
- Request: `{Name}`
- Response: `{Acknowledged}`
- Logic: verify index exists → close IndexService → remove from ClusterState → clean up data directory

**TransportGetIndexAction:**
- Request: `{Name}`
- Response: index metadata (settings + mappings)
- Logic: look up index in ClusterState, return metadata

All three implement `transport.ActionHandler` interface (`Name() string`).

### 2. REST Handlers (`server/rest/action/`)

Create REST handlers that parse HTTP requests and delegate to transport actions:

**RestCreateIndexAction:**
- Route: `PUT /{index}`
- Parses: index name from path param `{index}`, settings + mappings from JSON body
- Body format: `{"settings": {"number_of_shards": 1}, "mappings": {"properties": {"title": {"type": "text"}}}}`
- Returns: `{"acknowledged": true, "index": "myindex"}`

**RestDeleteIndexAction:**
- Route: `DELETE /{index}`
- Parses: index name from path
- Returns: `{"acknowledged": true}`

**RestGetIndexAction:**
- Route: `GET /{index}`
- Parses: index name from path
- Returns: `{"myindex": {"settings": {...}, "mappings": {...}}}`

Each implements `rest.RestHandler` interface (`Routes() []Route`, `HandleRequest(req, resp)`).

### 3. Node Wiring (`server/node/`)

Modify Node to:
- Hold `indexServices map[string]*index.IndexService` (new field)
- Hold a default analyzer (created in NewNode)
- Create and register all transport actions + REST handlers in NewNode
- Close all IndexServices in Stop()
- Expose `IndexService(name string) *index.IndexService` accessor

### 4. ClusterState Update

The `IndexMetadata` struct in `server/cluster/metadata.go` currently lacks a `Mapping` field. Add:
```go
Mapping *mapping.MappingDefinition
```
This is needed so GetIndexAction can return the mapping.

**Note:** This creates an import from `cluster` → `mapping`. Check that this doesn't create a circular dependency. If it does, store the mapping as `map[string]any` or a similar decoupled representation instead.

## Existing infrastructure to use

**REST layer** (`server/rest/`):
- `RestController.RegisterHandler(handler RestHandler)` — registers multi-route handler
- `RestHandler` interface: `Routes() []Route`, `HandleRequest(req *RestRequest, resp *RestResponseWriter)`
- `RestRequest{Method, Params map[string]string, Body []byte}` — path params extracted by `{paramName}` syntax
- `RestResponseWriter.WriteJSON(status int, body any)` — JSON response
- `RestResponseWriter.WriteError(status int, errType string, reason string)` — ES-format error

**Transport** (`server/transport/`):
- `ActionHandler` interface: `Name() string`
- `ActionRegistry.Register(handler)`, `.Get(name) ActionHandler`

**Index layer** (`server/index/`):
- `index.NewIndexService(meta *cluster.IndexMetadata, m *mapping.MappingDefinition, dataPath string, analyzer *analysis.Analyzer) (*IndexService, error)`
- `IndexService.Close() error`
- `IndexService.Shard(id int) *IndexShard`
- `IndexService.Mapping() *mapping.MappingDefinition`
- `IndexService.NumShards() int`
- `index.RouteShard(id string, numShards int) int`

**Cluster** (`server/cluster/`):
- `ClusterState.Metadata() *Metadata` (read-locked)
- `ClusterState.UpdateMetadata(fn func(*Metadata) *Metadata)` (write-locked)
- `Metadata.Indices map[string]*IndexMetadata`
- `IndexMetadata{Name, Settings, NumShards, State}`
- `IndexSettings{NumberOfShards, NumberOfReplicas}`

**Node** (`server/node/`):
- `NodeConfig{DataPath, HTTPPort}`
- `Node.Start() (string, error)` — returns listen address
- `Node.Stop() error`
- `Node.ClusterState()`, `Node.ActionRegistry()`, `Node.RestController()` — accessors

**Data directory layout:** `{DataPath}/nodes/0/indices/{index_name}/` → passed to `NewIndexService`

## Testing approach

- Unit tests for each transport action (test business logic in isolation)
- E2E test in `server/node/` or a dedicated test: start Node, use `net/http` client to PUT/GET/DELETE indices, verify responses and error cases (duplicate create, delete nonexistent, etc.)
- Error cases to test: empty index name, duplicate index, index not found, invalid JSON body

## ES-compatible error format

```json
{"error": {"type": "resource_already_exists_exception", "reason": "index [myindex] already exists"}, "status": 400}
{"error": {"type": "index_not_found_exception", "reason": "no such index [myindex]"}, "status": 404}
```
