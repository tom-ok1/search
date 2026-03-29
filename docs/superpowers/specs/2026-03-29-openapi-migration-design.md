# OpenAPI Migration Design

Migrate the server layer from hand-written HTTP handlers to an OpenAPI-first approach using oapi-codegen, minimizing maintenance cost by generating types, server interfaces, and validation from a single schema.

## Approach

Full replacement (not incremental). All 12 endpoints migrate at once. No backward compatibility constraints.

## OpenAPI Spec (`api/openapi.yaml`)

A single YAML spec covering all endpoints:

- **Index Management:** `PUT /{index}`, `GET /{index}`, `DELETE /{index}`
- **Document CRUD:** `PUT /{index}/_doc/{id}`, `POST /{index}/_doc/{id}`, `GET /{index}/_doc/{id}`, `DELETE /{index}/_doc/{id}`
- **Search:** `GET /{index}/_search`, `POST /{index}/_search`
- **Bulk:** `POST /_bulk`, `POST /{index}/_bulk`
- **Admin:** `POST /{index}/_refresh`

Schema details:

- Index name path parameter validated via `pattern: ^[a-z0-9][a-z0-9._-]*$`
- Request/response bodies defined as named schemas matching current ES-compatible JSON shapes
- Common `ErrorResponse` schema: `{"error": {"type": "...", "reason": "..."}, "status": N}`
- Bulk endpoint uses `text/x-ndjson` content type; body passed as raw string (NDJSON not expressible in OpenAPI schema)
- Search query body uses loosely typed `query` object (recursive DSL validated in application code)

## Code Generation

**Tool:** `oapi-codegen` v2 (`github.com/oapi-codegen/oapi-codegen/v2`)

**Config:** `api/oapi-codegen.yaml`

```yaml
package: api
output: api/generated.go
generate:
  chi-server: true
  models: true
  strict-server: true
```

- `strict-server` generates `StrictServerInterface` with typed Go inputs/outputs (not raw `http.Request`)
- `models` generates Go structs from OpenAPI schemas
- All generated code in `api/` package at project root
- `go:generate` directive in `api/generate.go` for `go generate ./...`

## Validation

**Automatic via OAPI validation middleware** (`oapi-codegen/pkg/chi-middleware`):

- Path param patterns (index name regex)
- Required fields in request bodies
- Type checking for JSON body fields
- Enum values

**Stays in application code:**

- Business logic validation (index exists/doesn't exist — depends on cluster state)
- Query DSL validation (recursive, handled by `QueryParser`)
- Bulk NDJSON parsing

## Architecture After Migration

```
Before:  HTTP -> RestController -> RestAction -> TransportAction -> IndexService -> Engine
After:   HTTP -> Chi + OAPIValidation -> StrictServerImpl -> TransportAction -> IndexService -> Engine
```

### Deleted

- `server/rest/` — entire package (controller.go, request.go, response.go, all action/*.go)

### New

- `api/` — openapi.yaml, oapi-codegen.yaml, generate.go, generated.go
- `server/handler/` — single `StrictServerImpl` struct implementing `StrictServerInterface`, delegating to transport actions

### Modified

- `server/node/node.go` — rewritten to wire Chi router, validation middleware, and handler
- `server/action/` — hand-written request/response types replaced with generated types where they overlap. Types that don't map to OpenAPI (e.g., internal `BulkItem` during NDJSON parsing) stay as-is. Transport action `Execute` signatures updated to use generated types.

### Unchanged

- `server/action/query_parser.go`
- `server/index/`, `server/cluster/`, `server/mapping/`
- All business logic and error semantics

## Error Handling

- **Validation errors** — handled by OAPI middleware. Error formatter customized to produce ES-style `{"error": {"type": "...", "reason": "..."}, "status": N}` responses.
- **Business errors** — transport action errors mapped to HTTP status + ES error type in the handler. Introduce typed errors in `server/action/` (`IndexNotFoundError`, `IndexAlreadyExistsError`, etc.) to replace current string matching.

## Testing Strategy

- **Transport action tests** (`server/action/*_test.go`) — stay, update type references to generated types
- **Integration tests** — stay as primary regression check (same JSON shapes, same status codes)
- **Validation tests** — add tests confirming invalid index names, missing fields, etc. are rejected by middleware
- **Generated code** — not tested directly (tested by oapi-codegen)

## Dependencies Added

- `github.com/go-chi/chi/v5`
- `github.com/oapi-codegen/oapi-codegen/v2` (dev/generate dependency)
- `github.com/oapi-codegen/runtime`
- `github.com/getkin/kin-openapi` (transitive, used by validation middleware)
