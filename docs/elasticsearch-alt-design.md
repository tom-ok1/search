# GoSearch Server: Elasticsearch-Alternative Design

## Overview

This document describes the design for an Elasticsearch-compatible server layer built on top of GoSearch's existing Lucene-like search engine implementation. The architecture mirrors Elasticsearch's layered design to ensure authenticity and a clear extension path toward a distributed multi-node system.

### Goals

- Provide an Elasticsearch-compatible REST API for indexing and searching documents
- Follow Elasticsearch's internal architecture (REST → Transport Action → Index/Shard → Lucene)
- Build on existing GoSearch packages (`index/`, `search/`, `analysis/`, `document/`, `store/`, `fst/`)
- Design for single-node first, with abstractions that enable multi-node extension without rewrites

### Non-Goals (v1)

- Cluster coordination, node discovery, shard replication
- Aggregations, scroll/search_after, highlights, suggesters
- Dynamic mapping, aliases, index templates
- Ingest pipelines, scripting
- Authentication, authorization

---

## Architecture

### Layered Design

Following Elasticsearch's architecture, the server is organized into clearly separated layers. Each layer only knows about its own types and the layer directly below it.

```
┌─────────────────────────────────────────────────┐
│                   HTTP Client                    │
└──────────────────────┬──────────────────────────┘
                       │ JSON/HTTP
┌──────────────────────▼──────────────────────────┐
│              REST Layer (rest/)                   │
│  RestController, RestSearchAction, RestIndexAction│
│  Parses HTTP, builds typed request objects        │
└──────────────────────┬──────────────────────────┘
                       │ Typed Request/Response
┌──────────────────────▼──────────────────────────┐
│         Transport Action Layer (action/)          │
│  TransportSearchAction, TransportIndexAction      │
│  Business logic, index resolution, shard routing  │
│  Query-then-fetch coordination                    │
└──────────────────────┬──────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────┐
│          Index Service Layer (index/)             │
│  IndexService, IndexShard, Engine                 │
│  Per-index shard management, refresh, flush       │
└──────────────────────┬──────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────┐
│         GoSearch Lucene Layer (existing)          │
│  IndexWriter, IndexReader, IndexSearcher          │
│  TermQuery, BooleanQuery, PhraseQuery, BM25       │
│  FST, DocValues, Segments, MergePolicy            │
└─────────────────────────────────────────────────┘
```

### Package Structure

```
server/
├── node/           # Node lifecycle, wires all services together
├── rest/           # HTTP server, route registration, request/response encoding
│   └── action/     # REST handlers (RestSearchAction, RestIndexAction, etc.)
├── action/         # Transport actions (core business logic)
├── index/          # IndexService, IndexShard, Engine wrapper
├── cluster/        # ClusterState, Metadata, IndexMetadata
├── mapping/        # Field type registry, mapping definitions, document parsing
└── transport/      # Transport interface (local impl now, network later)
```

---

## Component Design

### 1. Node (`server/node/`)

The `Node` is the entry point that creates and wires all services at startup.

```go
type Node struct {
    clusterState    *cluster.ClusterState
    indexServices   map[string]*index.IndexService
    restController  *rest.RestController
    transportActions *transport.ActionRegistry
    httpServer      *http.Server
}

func NewNode(config NodeConfig) (*Node, error)
func (n *Node) Start() error
func (n *Node) Stop() error
```

**Responsibilities:**
- Initialize ClusterState with empty metadata
- Create RestController and register all REST handlers
- Create transport action registry and register all actions
- Start HTTP server
- Manage graceful shutdown

This mirrors Elasticsearch's `Node` class which serves as the composition root.

### 2. Cluster State (`server/cluster/`)

Even on a single node, ClusterState is the authoritative registry of what indices exist and their configurations. This is critical for future multi-node extension where ClusterState is published across nodes.

```go
// cluster/state.go
type ClusterState struct {
    mu       sync.RWMutex
    metadata *Metadata
}

func (cs *ClusterState) Metadata() *Metadata
func (cs *ClusterState) UpdateMetadata(fn func(*Metadata) *Metadata)

// cluster/metadata.go
type Metadata struct {
    Indices map[string]*IndexMetadata
}

type IndexMetadata struct {
    Name      string
    Settings  IndexSettings
    Mapping   *mapping.MappingDefinition
    NumShards int    // 1 for v1, concept exists for future
    State     IndexState  // OPEN, CLOSE
}

type IndexSettings struct {
    NumberOfShards   int
    NumberOfReplicas int  // ignored in v1, stored for compatibility
}
```

**Future extension:** In a multi-node system, ClusterState becomes immutable and version-stamped. The master node publishes new versions; other nodes apply them. The current mutable design can be replaced with an immutable publish/apply model without changing consumers.

### 3. Mapping (`server/mapping/`)

Defines field types and translates JSON documents into Lucene documents according to the schema.

```go
// mapping/field_type.go
type FieldType string

const (
    FieldTypeText    FieldType = "text"     // analyzed, inverted index
    FieldTypeKeyword FieldType = "keyword"  // not analyzed, single term
    FieldTypeLong    FieldType = "long"     // int64, numeric doc values
    FieldTypeDouble  FieldType = "double"   // float64, numeric doc values
    FieldTypeBoolean FieldType = "boolean"  // indexed as "true"/"false" keyword
)

// mapping/mapping.go
type MappingDefinition struct {
    Properties map[string]FieldMapping
}

type FieldMapping struct {
    Type     FieldType
    Analyzer string   // for text fields, defaults to standard
}
```

```go
// mapping/parser.go
func ParseDocument(id string, source []byte, mapping *MappingDefinition) (*document.Document, error)
```

**ParseDocument** performs the following for each field in the JSON source:

| Field Type | Lucene Fields Created |
|---|---|
| `text` | `FieldTypeText` (analyzed with configured analyzer) |
| `keyword` | `FieldTypeKeyword` (single term, not analyzed) |
| `long` | `FieldTypeKeyword` (indexed for term queries) + `FieldTypeNumericDocValues` (for sorting) |
| `double` | `FieldTypeKeyword` (indexed) + `FieldTypeNumericDocValues` (for sorting) |
| `boolean` | `FieldTypeKeyword` (indexed as `"true"` / `"false"`) |

Additionally, every document gets:
- `_id` field: `FieldTypeKeyword` with the document ID
- `_source` field: `FieldTypeStored` with the raw JSON bytes

**v1: Explicit mappings only.** Fields not defined in the mapping are ignored. Dynamic mapping (auto-detecting types from values) is a planned future enhancement.

### 4. Index Service & Shard (`server/index/`)

Mirrors Elasticsearch's `IndexService` / `IndexShard` / `Engine` hierarchy.

```go
// index/service.go
type IndexService struct {
    metadata *cluster.IndexMetadata
    shards   map[int]*IndexShard
    mapping  *mapping.MappingDefinition
}

func NewIndexService(meta *cluster.IndexMetadata, dataPath string) (*IndexService, error)
func (is *IndexService) Shard(id int) *IndexShard
func (is *IndexService) Close() error
```

```go
// index/shard.go
type IndexShard struct {
    shardID   int
    engine    *Engine
    indexName string
}

func (s *IndexShard) Index(id string, source []byte) error
func (s *IndexShard) Delete(id string) error
func (s *IndexShard) Refresh() error
func (s *IndexShard) Searcher() *search.IndexSearcher
```

```go
// index/engine.go
type Engine struct {
    writer    *goindex.IndexWriter
    reader    *goindex.IndexReader
    searcher  *search.IndexSearcher
    directory *store.FSDirectory
    mu        sync.RWMutex  // protects reader/searcher swap on refresh
}

func NewEngine(dir *store.FSDirectory) (*Engine, error)
func (e *Engine) Index(doc *document.Document) error
func (e *Engine) Delete(term string, value string) error
func (e *Engine) Refresh() error
func (e *Engine) Searcher() *search.IndexSearcher
func (e *Engine) Close() error
```

**Refresh** reopens the IndexReader and IndexSearcher from the IndexWriter, making newly indexed documents searchable. This matches Elasticsearch's refresh semantics.

**Shard routing** for v1 is trivial (1 shard), but the routing function exists:
```go
func routeShard(id string, numShards int) int {
    return int(hash(id) % uint32(numShards))
}
```

### 5. Transport Layer (`server/transport/`)

The transport layer defines the boundary between "what to do" and "where to do it." In v1 this is local function calls; later it becomes network RPC.

```go
// transport/action.go
type ActionHandler interface {
    Name() string
}

type ActionRegistry struct {
    handlers map[string]ActionHandler
}

func (r *ActionRegistry) Register(handler ActionHandler)
```

Transport actions are typed per request/response:

```go
// Implemented by each concrete action
type TypedAction[Req any, Resp any] interface {
    ActionHandler
    Execute(req Req) (Resp, error)
}
```

**Future extension:** Replace `Execute(req) (resp, error)` with a network-aware dispatcher that serializes the request, routes to the correct node based on ClusterState, and deserializes the response. The action logic itself doesn't change.

### 6. REST Layer (`server/rest/`)

HTTP server with route-based dispatch, mirroring Elasticsearch's `RestController`.

```go
// rest/controller.go
type RestController struct {
    router *http.ServeMux
}

func NewRestController() *RestController
func (rc *RestController) RegisterHandler(handler RestHandler)
func (rc *RestController) ServeHTTP(w http.ResponseWriter, r *http.Request)
```

```go
// rest/handler.go
type RestHandler interface {
    Routes() []Route
    HandleRequest(req *RestRequest, resp *RestResponseWriter)
}

type Route struct {
    Method  string   // GET, POST, PUT, DELETE
    Path    string   // e.g., "/{index}/_search"
}

// rest/request.go
type RestRequest struct {
    Method  string
    Params  map[string]string   // URL path params + query params
    Body    []byte
}

// rest/response.go
type RestResponseWriter struct {
    http.ResponseWriter
}

func (w *RestResponseWriter) WriteJSON(status int, body interface{})
func (w *RestResponseWriter) WriteError(status int, err error)
```

### 7. Transport Actions (`server/action/`)

Each action contains the business logic for one operation. This is where Elasticsearch's core logic lives.

#### TransportCreateIndexAction

```go
type CreateIndexRequest struct {
    Name     string
    Settings cluster.IndexSettings
    Mappings mapping.MappingDefinition
}

type CreateIndexResponse struct {
    Acknowledged bool
    Index        string
}
```

Logic:
1. Validate index name (not empty, no special chars, not duplicate)
2. Build `IndexMetadata` from request
3. Update `ClusterState.Metadata`
4. Create `IndexService` with shard(s)
5. Register in node's index service map

#### TransportDeleteIndexAction

```go
type DeleteIndexRequest struct {
    Name string
}
```

Logic:
1. Verify index exists in ClusterState
2. Close IndexService (close shards, engine, writer)
3. Remove from ClusterState
4. Clean up data directory

#### TransportIndexAction (Index Document)

```go
type IndexRequest struct {
    Index  string
    ID     string
    Source json.RawMessage
}

type IndexResponse struct {
    Index  string
    ID     string
    Result string   // "created" or "updated"
}
```

Logic:
1. Resolve index from ClusterState
2. Route to shard: `hash(id) % numShards`
3. `mapping.ParseDocument(id, source, mapping)` → lucene Document
4. `shard.Index(id, source)` → engine writes to IndexWriter

#### TransportDeleteAction (Delete Document)

```go
type DeleteRequest struct {
    Index string
    ID    string
}
```

Logic:
1. Resolve index → shard
2. `shard.Delete(id)` → engine deletes by `_id` term

#### TransportGetAction (Get Document)

```go
type GetRequest struct {
    Index string
    ID    string
}

type GetResponse struct {
    Index  string
    ID     string
    Found  bool
    Source json.RawMessage
}
```

Logic:
1. Resolve index → shard
2. Search by `_id` term query (single result)
3. Retrieve `_source` stored field
4. Note: requires prior refresh to find the document (not real-time in v1)

#### TransportSearchAction

```go
type SearchRequest struct {
    Index  string
    Source SearchSourceBuilder
}

type SearchSourceBuilder struct {
    Query json.RawMessage
    Size  int   // default 10
    Sort  []SortField
}

type SearchResponse struct {
    Took     int64       // milliseconds
    Hits     SearchHits
}

type SearchHits struct {
    Total    TotalHits
    MaxScore float64
    Hits     []SearchHit
}

type SearchHit struct {
    Index  string          `json:"_index"`
    ID     string          `json:"_id"`
    Score  float64         `json:"_score"`
    Source json.RawMessage `json:"_source"`
}

type TotalHits struct {
    Value    int    `json:"value"`
    Relation string `json:"relation"`  // "eq"
}
```

Logic (query-then-fetch):
1. Resolve index → IndexService
2. Parse query DSL → lucene Query (see Query DSL section)
3. **Query phase** — for each shard:
   - Get searcher from engine
   - Create collector (TopKCollector or TopFieldCollector based on sort)
   - `searcher.Search(query, collector)`
   - Collect top doc IDs + scores
4. **Merge phase** — merge results from all shards (trivial with 1 shard, but the merge exists for future extension)
5. **Fetch phase** — for each top hit:
   - Retrieve `_source` stored field by doc ID
   - Retrieve `_id` field
   - Build SearchHit
6. Return SearchResponse with timing

This two-phase design is what makes distributed search possible later — query phase runs on every shard/node, coordinator merges top-N, then fetch phase only runs where the winning documents live.

#### TransportBulkAction

```go
type BulkRequest struct {
    Items []BulkItem
}

type BulkItem struct {
    Action string          // "index", "delete"
    Index  string
    ID     string
    Source json.RawMessage // nil for delete
}

type BulkResponse struct {
    Took   int64
    Errors bool
    Items  []BulkItemResponse
}

type BulkItemResponse struct {
    Action string
    Index  string
    ID     string
    Status int
    Error  *ErrorDetail
}
```

Logic:
1. Parse NDJSON body (action line + optional source line pairs)
2. Group items by index/shard
3. For each item, delegate to index/delete logic
4. Collect per-item results, track if any errors occurred

#### TransportRefreshAction

```go
type RefreshRequest struct {
    Index string
}
```

Logic:
1. Resolve index → IndexService
2. For each shard: `shard.Refresh()` → engine reopens reader/searcher

---

## Query DSL Parser

Translates JSON query DSL into GoSearch's existing `search.Query` types.

### Supported Queries (v1)

| Query DSL | GoSearch Query | Description |
|---|---|---|
| `{"term": {"field": "value"}}` | `TermQuery` | Exact term match, no analysis |
| `{"match": {"field": "text"}}` | `BooleanQuery(SHOULD, [TermQuery...])` | Analyzed text match |
| `{"bool": {"must":[], "should":[], "must_not":[]}}` | `BooleanQuery` | Boolean combination |

### Parser Design

```go
// action/search/query_parser.go
type QueryParser struct {
    mapping  *mapping.MappingDefinition
    analyzers map[string]*analysis.Analyzer
}

func (p *QueryParser) ParseQuery(queryJSON map[string]interface{}) (search.Query, error)
```

The parser inspects the top-level key and dispatches:

- **`term`**: Direct `search.NewTermQuery(field, value)`
- **`match`**:
  1. Look up the field's analyzer from the mapping
  2. Analyze the input text into tokens
  3. If single token → `TermQuery`
  4. If multiple tokens → `BooleanQuery` with SHOULD clauses (one `TermQuery` per token)
- **`bool`**: Recursively parse each clause array, build `BooleanQuery` with appropriate clause types (MUST, SHOULD, MUST_NOT)

### Future Query Types

These map cleanly to future GoSearch additions:
- `match_phrase` → `PhraseQuery` (already exists in GoSearch)
- `range` → Range query (needs new query type in lucene layer)
- `multi_match` → Multiple `match` queries combined
- `exists` → Doc count check on field

---

## REST API Endpoints

### Index Management

| Method | Path | Handler | Transport Action |
|---|---|---|---|
| `PUT` | `/{index}` | `RestCreateIndexAction` | `TransportCreateIndexAction` |
| `DELETE` | `/{index}` | `RestDeleteIndexAction` | `TransportDeleteIndexAction` |
| `GET` | `/{index}` | `RestGetIndexAction` | `TransportGetIndexAction` |

### Document CRUD

| Method | Path | Handler | Transport Action |
|---|---|---|---|
| `PUT` | `/{index}/_doc/{id}` | `RestIndexAction` | `TransportIndexAction` |
| `POST` | `/{index}/_doc` | `RestIndexAction` | `TransportIndexAction` (auto-gen ID) |
| `GET` | `/{index}/_doc/{id}` | `RestGetAction` | `TransportGetAction` |
| `DELETE` | `/{index}/_doc/{id}` | `RestDeleteAction` | `TransportDeleteAction` |

### Search

| Method | Path | Handler | Transport Action |
|---|---|---|---|
| `GET` | `/{index}/_search` | `RestSearchAction` | `TransportSearchAction` |
| `POST` | `/{index}/_search` | `RestSearchAction` | `TransportSearchAction` |

### Bulk

| Method | Path | Handler | Transport Action |
|---|---|---|---|
| `POST` | `/_bulk` | `RestBulkAction` | `TransportBulkAction` |
| `POST` | `/{index}/_bulk` | `RestBulkAction` | `TransportBulkAction` |

### Operations

| Method | Path | Handler | Transport Action |
|---|---|---|---|
| `POST` | `/{index}/_refresh` | `RestRefreshAction` | `TransportRefreshAction` |

---

## Data Directory Layout

```
data/
└── nodes/
    └── 0/
        └── indices/
            └── {index_name}/
                └── 0/              # shard 0
                    └── index/      # lucene segment files
                        ├── seg_0.meta
                        ├── seg_0.title.tfst
                        ├── seg_0.title.tdat
                        ├── seg_0.stored
                        └── ...
```

This follows Elasticsearch's data directory convention and naturally extends to multiple shards and nodes.

---

## Implementation Plan

### Step 1: Core Infrastructure

Set up the package structure, Node lifecycle, and ClusterState.

- Create `server/` package directory structure
- Implement `cluster.ClusterState` and `cluster.Metadata` with in-memory state management
- Implement `node.Node` with Start/Stop lifecycle
- Implement `transport.ActionRegistry` for action registration and dispatch
- Set up basic HTTP server in `rest.RestController` with route registration

**Deliverable:** A node that starts, listens on HTTP, and returns 404 for all routes.

### Step 2: Mapping & Document Parsing

Build the schema system that translates JSON documents into Lucene documents.

- Define `mapping.FieldType` constants and `mapping.MappingDefinition`
- Implement `mapping.ParseDocument`: JSON source + mapping → `document.Document`
  - Handle text, keyword, long, double, boolean field types
  - Add `_id` as keyword field
  - Add `_source` as stored field (raw JSON bytes)
- Add unit tests for document parsing with each field type

**Deliverable:** Given a mapping and JSON doc, produces correct Lucene document.

### Step 3: Index & Shard Layer

Build the Engine, IndexShard, and IndexService that wrap the existing Lucene layer.

- Implement `index.Engine`: wraps IndexWriter, manages reader/searcher, provides refresh
- Implement `index.IndexShard`: owns Engine, exposes Index/Delete/Refresh/Searcher
- Implement `index.IndexService`: manages shards for an index, holds mapping reference
- Shard routing function (`hash(id) % numShards`)

**Deliverable:** Programmatic API to create an index, write documents, refresh, and get a searcher.

### Step 4: Index Management Actions + REST

Wire up index creation and deletion end-to-end.

- Implement `TransportCreateIndexAction`: validate, update ClusterState, create IndexService
- Implement `TransportDeleteIndexAction`: close, remove from ClusterState, clean up
- Implement `TransportGetIndexAction`: return index metadata (settings + mappings)
- Implement `RestCreateIndexAction`, `RestDeleteIndexAction`, `RestGetIndexAction`
- Register routes and actions in Node

**Deliverable:** `PUT /myindex`, `GET /myindex`, `DELETE /myindex` work via curl.

### Step 5: Document Indexing Actions + REST

Wire up document CRUD.

- Implement `TransportIndexAction`: resolve index, parse doc, route to shard, write
- Implement `TransportGetAction`: search by `_id`, retrieve `_source`
- Implement `TransportDeleteAction`: delete by `_id` term
- Implement `TransportRefreshAction`: refresh all shards in an index
- Implement REST handlers: `RestIndexAction`, `RestGetAction`, `RestDeleteAction`, `RestRefreshAction`

**Deliverable:** Full document lifecycle works: index → refresh → get → delete.

### Step 6: Query DSL Parser

Build the JSON query → Lucene query translator.

- Implement `QueryParser` with mapping-aware analysis
- Support `term` query: direct TermQuery construction
- Support `match` query: analyze text → BooleanQuery(SHOULD) of TermQueries
- Support `bool` query: recursive parsing of must/should/must_not clauses
- Unit tests for each query type and nested combinations

**Deliverable:** JSON query DSL correctly produces Lucene query trees.

### Step 7: Search Action + REST

Wire up the search endpoint with query-then-fetch.

- Implement `TransportSearchAction`:
  - Query phase: parse query, execute on each shard's searcher, collect top hits
  - Merge phase: merge results across shards
  - Fetch phase: retrieve `_source` and `_id` for top hits
- Support `size` parameter (default 10)
- Support sort (by score or field values using existing sort infrastructure)
- Implement `RestSearchAction` (GET + POST)
- Build SearchResponse JSON matching Elasticsearch's format

**Deliverable:** `POST /myindex/_search {"query": {"match": {"title": "hello"}}}` returns ranked results.

### Step 8: Bulk API

Add batch indexing support.

- Implement NDJSON parser for bulk request body
- Implement `TransportBulkAction`: iterate items, delegate to index/delete per item
- Collect per-item results, track errors
- Implement `RestBulkAction`

**Deliverable:** `POST /_bulk` with NDJSON body indexes multiple documents efficiently.

### Step 9: Integration Tests & Error Handling

Harden the system with end-to-end tests and proper error responses.

- E2E test: create index → bulk index docs → refresh → search → verify results
- E2E test: error cases (index not found, invalid mapping, malformed query)
- Consistent error response format matching Elasticsearch:
  ```json
  {"error": {"type": "index_not_found_exception", "reason": "no such index [foo]"}, "status": 404}
  ```
- Input validation across all endpoints

**Deliverable:** Robust, well-tested server with Elasticsearch-compatible error responses.

---

## Future Roadmap

This design explicitly supports the following extensions:

### Phase 2: Enhanced Single-Node
- **Dynamic mapping**: Auto-detect field types from document values
- **Real-time get**: In-memory version map for un-refreshed documents (like ES's `LiveVersionMap`)
- **Auto-refresh**: Configurable `refresh_interval` setting with background timer
- **More query types**: `match_phrase` (→ PhraseQuery), `range`, `multi_match`, `exists`
- **Aggregations**: Leverage existing NumericDocValues/SortedDocValues for terms/range aggs
- **`_cat` APIs**: Human-readable status endpoints
- **Translog**: Write-ahead log for crash recovery

### Phase 3: Multi-Shard
- **Multiple shards per index**: Actual shard routing, per-shard directories
- **Query-then-fetch across shards**: Distributed frequency scoring (DFS)
- **Shard-level refresh/flush control**: Independent shard lifecycle

### Phase 4: Multi-Node Cluster
- **Network transport**: Replace local action dispatch with gRPC/custom binary protocol
- **Cluster state publication**: Master node publishes immutable ClusterState versions
- **Node discovery**: Seed-based node discovery and join protocol
- **Shard allocation**: Allocate shards across nodes, rebalance on topology changes
- **Replica shards**: Write to primary, replicate to replicas, search across all copies
- **Distributed search coordination**: Fan-out query phase, merge, targeted fetch phase

### Phase 5: Production Readiness
- **Snapshot/restore**: Backup index data to external storage
- **Index lifecycle management**: Automated rollover, shrink, delete
- **Security**: Authentication, authorization, field-level security
- **Monitoring**: Metrics, slow query log, node stats APIs

---

## Appendix: Existing GoSearch Building Blocks

The following packages from the existing codebase are used directly:

| Package | What It Provides | Used By |
|---|---|---|
| `analysis/` | Tokenizer, TokenFilter, Analyzer | Mapping (text field analysis), QueryParser (match query) |
| `document/` | Document, Field, FieldType | Mapping (ParseDocument output) |
| `search/` | TermQuery, BooleanQuery, PhraseQuery, BM25, IndexSearcher, Collectors | TransportSearchAction, QueryParser |
| `index/` | IndexWriter, IndexReader, DWPT, merge policy, segments | Engine (core write/read path) |
| `store/` | FSDirectory, MMap | Engine (storage backend) |
| `fst/` | FST builder/reader | Used internally by index package |

No modifications to existing packages are expected for v1. The `server/` package is purely additive.
