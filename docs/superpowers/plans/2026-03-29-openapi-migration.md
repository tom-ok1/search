# OpenAPI Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hand-written REST handlers with oapi-codegen generated types, Chi server interface, and OpenAPI validation middleware.

**Architecture:** Write an `api/openapi.yaml` spec, generate a strict server interface and models with oapi-codegen, implement a `StrictServerImpl` adapter in `server/handler/` that delegates to existing transport actions, and rewire `server/node/node.go` to use Chi. Delete `server/rest/` entirely.

**Tech Stack:** oapi-codegen v2, go-chi/chi v5, kin-openapi (validation middleware)

---

### Task 1: Add dependencies and code generation scaffolding

**Files:**
- Modify: `go.mod`
- Create: `api/oapi-codegen.yaml`
- Create: `api/generate.go`

- [ ] **Step 1: Install oapi-codegen CLI**

Run:
```bash
go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
```

- [ ] **Step 2: Add Go module dependencies**

Run:
```bash
go get github.com/go-chi/chi/v5
go get github.com/oapi-codegen/oapi-codegen/v2
go get github.com/oapi-codegen/runtime
go get github.com/getkin/kin-openapi
go get github.com/oapi-codegen/netherwork-middleware
```

- [ ] **Step 3: Create oapi-codegen config**

Create `api/oapi-codegen.yaml`:
```yaml
package: api
output: generated.go
generate:
  chi-server: true
  models: true
  strict-server: true
```

- [ ] **Step 4: Create generate.go**

Create `api/generate.go`:
```go
package api

//go:generate oapi-codegen --config oapi-codegen.yaml openapi.yaml
```

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum api/oapi-codegen.yaml api/generate.go
git commit -m "chore: add oapi-codegen dependencies and generation config"
```

---

### Task 2: Write OpenAPI spec - Index Management endpoints

**Files:**
- Create: `api/openapi.yaml`

- [ ] **Step 1: Create the OpenAPI spec with index management endpoints**

Create `api/openapi.yaml`:
```yaml
openapi: "3.0.3"
info:
  title: GoSearch API
  version: "1.0.0"
  description: Elasticsearch-compatible search engine API

paths:
  /{index}:
    put:
      operationId: createIndex
      summary: Create a new index
      tags: [index]
      parameters:
        - $ref: '#/components/parameters/IndexName'
      requestBody:
        required: false
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreateIndexRequest'
      responses:
        '200':
          description: Index created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CreateIndexResponse'
        '400':
          description: Invalid request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

    get:
      operationId: getIndex
      summary: Get index metadata
      tags: [index]
      parameters:
        - $ref: '#/components/parameters/IndexName'
      responses:
        '200':
          description: Index metadata
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/GetIndexResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

    delete:
      operationId: deleteIndex
      summary: Delete an index
      tags: [index]
      parameters:
        - $ref: '#/components/parameters/IndexName'
      responses:
        '200':
          description: Index deleted
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DeleteIndexResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /{index}/_doc/{id}:
    put:
      operationId: indexDocumentPut
      summary: Index a document (PUT)
      tags: [document]
      parameters:
        - $ref: '#/components/parameters/IndexName'
        - $ref: '#/components/parameters/DocId'
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/DocumentSource'
      responses:
        '201':
          description: Document indexed
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/IndexDocumentResponse'
        '400':
          description: Parsing error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

    post:
      operationId: indexDocumentPost
      summary: Index a document (POST)
      tags: [document]
      parameters:
        - $ref: '#/components/parameters/IndexName'
        - $ref: '#/components/parameters/DocId'
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/DocumentSource'
      responses:
        '201':
          description: Document indexed
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/IndexDocumentResponse'
        '400':
          description: Parsing error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

    get:
      operationId: getDocument
      summary: Get a document by ID
      tags: [document]
      parameters:
        - $ref: '#/components/parameters/IndexName'
        - $ref: '#/components/parameters/DocId'
      responses:
        '200':
          description: Document found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/GetDocumentResponse'
        '404':
          description: Document or index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/GetDocumentResponse'

    delete:
      operationId: deleteDocument
      summary: Delete a document by ID
      tags: [document]
      parameters:
        - $ref: '#/components/parameters/IndexName'
        - $ref: '#/components/parameters/DocId'
      responses:
        '200':
          description: Document deleted
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DeleteDocumentResponse'
        '404':
          description: Document or index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /{index}/_search:
    get:
      operationId: searchIndexGet
      summary: Search an index (GET)
      tags: [search]
      parameters:
        - $ref: '#/components/parameters/IndexName'
        - name: size
          in: query
          schema:
            type: integer
            default: 10
      requestBody:
        required: false
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/SearchRequest'
      responses:
        '200':
          description: Search results
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SearchResponse'
        '400':
          description: Query parsing error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

    post:
      operationId: searchIndexPost
      summary: Search an index (POST)
      tags: [search]
      parameters:
        - $ref: '#/components/parameters/IndexName'
        - name: size
          in: query
          schema:
            type: integer
            default: 10
      requestBody:
        required: false
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/SearchRequest'
      responses:
        '200':
          description: Search results
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SearchResponse'
        '400':
          description: Query parsing error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /_bulk:
    post:
      operationId: bulkGlobal
      summary: Bulk operations (global)
      tags: [bulk]
      requestBody:
        required: true
        content:
          application/x-ndjson:
            schema:
              type: string
      responses:
        '200':
          description: Bulk results
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/BulkResponse'
        '400':
          description: Parse error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /{index}/_bulk:
    post:
      operationId: bulkIndex
      summary: Bulk operations (index-scoped)
      tags: [bulk]
      parameters:
        - $ref: '#/components/parameters/IndexName'
      requestBody:
        required: true
        content:
          application/x-ndjson:
            schema:
              type: string
      responses:
        '200':
          description: Bulk results
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/BulkResponse'
        '400':
          description: Parse error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /{index}/_refresh:
    post:
      operationId: refreshIndex
      summary: Refresh an index
      tags: [admin]
      parameters:
        - $ref: '#/components/parameters/IndexName'
      responses:
        '200':
          description: Refresh result
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/RefreshResponse'
        '404':
          description: Index not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

components:
  parameters:
    IndexName:
      name: index
      in: path
      required: true
      schema:
        type: string
        pattern: '^[a-z0-9][a-z0-9._-]*$'
    DocId:
      name: id
      in: path
      required: true
      schema:
        type: string
        minLength: 1

  schemas:
    ErrorResponse:
      type: object
      required: [error, status]
      properties:
        error:
          type: object
          required: [type, reason]
          properties:
            type:
              type: string
            reason:
              type: string
        status:
          type: integer

    FieldMapping:
      type: object
      required: [type]
      properties:
        type:
          type: string
          enum: [text, keyword, long, double, boolean]
        analyzer:
          type: string

    MappingsDefinition:
      type: object
      properties:
        properties:
          type: object
          additionalProperties:
            $ref: '#/components/schemas/FieldMapping'

    IndexSettings:
      type: object
      properties:
        number_of_shards:
          type: integer
          minimum: 1
        number_of_replicas:
          type: integer
          minimum: 0

    CreateIndexRequest:
      type: object
      properties:
        settings:
          $ref: '#/components/schemas/IndexSettings'
        mappings:
          $ref: '#/components/schemas/MappingsDefinition'

    CreateIndexResponse:
      type: object
      required: [acknowledged, index]
      properties:
        acknowledged:
          type: boolean
        index:
          type: string

    DeleteIndexResponse:
      type: object
      required: [acknowledged]
      properties:
        acknowledged:
          type: boolean

    GetIndexResponse:
      type: object
      additionalProperties:
        type: object
        properties:
          settings:
            $ref: '#/components/schemas/IndexSettings'
          mappings:
            $ref: '#/components/schemas/MappingsDefinition'

    DocumentSource:
      type: object
      additionalProperties: true

    IndexDocumentResponse:
      type: object
      required: [_index, _id, result]
      properties:
        _index:
          type: string
        _id:
          type: string
        result:
          type: string

    GetDocumentResponse:
      type: object
      required: [_index, _id, found]
      properties:
        _index:
          type: string
        _id:
          type: string
        found:
          type: boolean
        _source:
          type: object
          additionalProperties: true

    DeleteDocumentResponse:
      type: object
      required: [_index, _id, result]
      properties:
        _index:
          type: string
        _id:
          type: string
        result:
          type: string

    SearchRequest:
      type: object
      properties:
        query:
          type: object
          additionalProperties: true
        size:
          type: integer

    SearchResponse:
      type: object
      required: [took, hits]
      properties:
        took:
          type: integer
          format: int64
        hits:
          $ref: '#/components/schemas/SearchHits'

    SearchHits:
      type: object
      required: [total, max_score, hits]
      properties:
        total:
          $ref: '#/components/schemas/TotalHits'
        max_score:
          type: number
          format: double
        hits:
          type: array
          items:
            $ref: '#/components/schemas/SearchHit'

    TotalHits:
      type: object
      required: [value, relation]
      properties:
        value:
          type: integer
        relation:
          type: string

    SearchHit:
      type: object
      required: [_index, _id, _score, _source]
      properties:
        _index:
          type: string
        _id:
          type: string
        _score:
          type: number
          format: double
        _source:
          type: object
          additionalProperties: true

    BulkResponse:
      type: object
      required: [took, errors, items]
      properties:
        took:
          type: integer
          format: int64
        errors:
          type: boolean
        items:
          type: array
          items:
            type: object
            additionalProperties:
              $ref: '#/components/schemas/BulkItemResult'

    BulkItemResult:
      type: object
      required: [_index, _id, status]
      properties:
        _index:
          type: string
        _id:
          type: string
        status:
          type: integer
        error:
          type: object
          properties:
            type:
              type: string
            reason:
              type: string

    RefreshResponse:
      type: object
      required: [_shards]
      properties:
        _shards:
          type: object
          required: [total, successful, failed]
          properties:
            total:
              type: integer
            successful:
              type: integer
            failed:
              type: integer
```

- [ ] **Step 2: Validate the spec parses correctly**

Run:
```bash
cd api && oapi-codegen --config oapi-codegen.yaml openapi.yaml
```

Expected: `generated.go` is created in `api/` without errors.

- [ ] **Step 3: Verify the generated code compiles**

Run:
```bash
go build ./api/...
```

Expected: compiles without errors.

- [ ] **Step 4: Commit**

```bash
git add api/openapi.yaml api/generated.go
git commit -m "feat: add OpenAPI spec and generated server/model code"
```

---

### Task 3: Introduce typed errors in transport actions

**Files:**
- Create: `server/action/errors.go`
- Modify: `server/action/create_index.go`
- Modify: `server/action/delete_index.go`
- Modify: `server/action/get_index.go`
- Modify: `server/action/index_document.go`
- Modify: `server/action/get_document.go`
- Modify: `server/action/delete_document.go`
- Modify: `server/action/search.go`
- Modify: `server/action/refresh.go`

- [ ] **Step 1: Create typed error types**

Create `server/action/errors.go`:
```go
package action

import "fmt"

type IndexNotFoundError struct {
	Index string
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("no such index [%s]", e.Index)
}

type IndexAlreadyExistsError struct {
	Index string
}

func (e *IndexAlreadyExistsError) Error() string {
	return fmt.Sprintf("index [%s] already exists", e.Index)
}

type InvalidIndexNameError struct {
	Index  string
	Reason string
}

func (e *InvalidIndexNameError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("invalid index name [%s]: %s", e.Index, e.Reason)
	}
	return fmt.Sprintf("invalid index name [%s]", e.Index)
}

type QueryParsingError struct {
	Reason string
}

func (e *QueryParsingError) Error() string {
	return fmt.Sprintf("parse query: %s", e.Reason)
}

type MapperParsingError struct {
	Reason string
}

func (e *MapperParsingError) Error() string {
	return e.Reason
}
```

- [ ] **Step 2: Update create_index.go to use typed errors**

In `server/action/create_index.go`, replace error returns in `Execute`:

Replace:
```go
if req.Name == "" {
    return CreateIndexResponse{}, fmt.Errorf("index name must not be empty")
}
if !validIndexName.MatchString(req.Name) {
    return CreateIndexResponse{}, fmt.Errorf("invalid index name [%s]: must be lowercase, start with a letter or digit, and contain only [a-z0-9._-]", req.Name)
}
```
With:
```go
if req.Name == "" {
    return CreateIndexResponse{}, &InvalidIndexNameError{Index: "", Reason: "must not be empty"}
}
if !validIndexName.MatchString(req.Name) {
    return CreateIndexResponse{}, &InvalidIndexNameError{Index: req.Name, Reason: "must be lowercase, start with a letter or digit, and contain only [a-z0-9._-]"}
}
```

Replace:
```go
if a.clusterState.Metadata().Indices[req.Name] != nil {
    return CreateIndexResponse{}, fmt.Errorf("index [%s] already exists", req.Name)
}
```
With:
```go
if a.clusterState.Metadata().Indices[req.Name] != nil {
    return CreateIndexResponse{}, &IndexAlreadyExistsError{Index: req.Name}
}
```

- [ ] **Step 3: Update all other actions to use `&IndexNotFoundError{}`**

In every transport action file that returns `fmt.Errorf("no such index [%s]", ...)`, replace with `&IndexNotFoundError{Index: indexName}`. Files: `delete_index.go`, `get_index.go`, `index_document.go`, `get_document.go`, `delete_document.go`, `search.go`, `refresh.go`, `bulk.go` (inside `executeItem`).

- [ ] **Step 4: Update search.go to use QueryParsingError**

In `server/action/search.go`, replace:
```go
return SearchResponse{}, fmt.Errorf("parse query: %w", err)
```
With:
```go
return SearchResponse{}, &QueryParsingError{Reason: err.Error()}
```

- [ ] **Step 5: Run existing tests to verify no regressions**

Run:
```bash
go test ./server/action/...
```

Expected: all tests pass. Tests use string matching on error messages, and the typed errors produce the same message strings.

- [ ] **Step 6: Commit**

```bash
git add server/action/errors.go server/action/*.go
git commit -m "refactor: introduce typed errors in transport actions"
```

---

### Task 4: Implement StrictServerImpl handler

**Files:**
- Create: `server/handler/handler.go`

This is the adapter that implements the generated `StrictServerInterface`, delegating each method to the corresponding transport action.

- [ ] **Step 1: Create the handler file**

Create `server/handler/handler.go`:
```go
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"gosearch/api"
	"gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

type Handler struct {
	createIndex    *action.TransportCreateIndexAction
	deleteIndex    *action.TransportDeleteIndexAction
	getIndex       *action.TransportGetIndexAction
	indexDoc       *action.TransportIndexAction
	getDoc         *action.TransportGetAction
	deleteDoc      *action.TransportDeleteDocumentAction
	searchAction   *action.TransportSearchAction
	bulkAction     *action.TransportBulkAction
	refreshAction  *action.TransportRefreshAction
}

func NewHandler(
	createIndex *action.TransportCreateIndexAction,
	deleteIndex *action.TransportDeleteIndexAction,
	getIndex *action.TransportGetIndexAction,
	indexDoc *action.TransportIndexAction,
	getDoc *action.TransportGetAction,
	deleteDoc *action.TransportDeleteDocumentAction,
	searchAction *action.TransportSearchAction,
	bulkAction *action.TransportBulkAction,
	refreshAction *action.TransportRefreshAction,
) *Handler {
	return &Handler{
		createIndex:   createIndex,
		deleteIndex:   deleteIndex,
		getIndex:      getIndex,
		indexDoc:      indexDoc,
		getDoc:        getDoc,
		deleteDoc:     deleteDoc,
		searchAction:  searchAction,
		bulkAction:    bulkAction,
		refreshAction: refreshAction,
	}
}
```

The method signatures here depend on the exact generated `StrictServerInterface`. After generation, implement each method following this pattern. The exact method names and parameter types will come from the generated code. Below is the intended logic for each handler method — adapt signatures to match the generated interface.

- [ ] **Step 2: Implement CreateIndex**

Add to `handler.go`:
```go
func (h *Handler) CreateIndex(ctx context.Context, request api.CreateIndexRequestObject) (api.CreateIndexResponseObject, error) {
	req := action.CreateIndexRequest{
		Name: request.Index,
	}
	if request.Body != nil {
		if request.Body.Settings != nil {
			if request.Body.Settings.NumberOfShards != nil {
				req.Settings.NumberOfShards = *request.Body.Settings.NumberOfShards
			}
			if request.Body.Settings.NumberOfReplicas != nil {
				req.Settings.NumberOfReplicas = *request.Body.Settings.NumberOfReplicas
			}
		}
		if request.Body.Mappings != nil && request.Body.Mappings.Properties != nil {
			props := make(map[string]mapping.FieldMapping)
			for name, fm := range *request.Body.Mappings.Properties {
				m := mapping.FieldMapping{Type: mapping.FieldType(fm.Type)}
				if fm.Analyzer != nil {
					m.Analyzer = *fm.Analyzer
				}
				props[name] = m
			}
			req.Mappings = &mapping.MappingDefinition{Properties: props}
		}
	}

	result, err := h.createIndex.Execute(req)
	if err != nil {
		return mapError(err)
	}

	return api.CreateIndex200JSONResponse{
		Acknowledged: result.Acknowledged,
		Index:        result.Index,
	}, nil
}
```

- [ ] **Step 3: Implement DeleteIndex**

```go
func (h *Handler) DeleteIndex(ctx context.Context, request api.DeleteIndexRequestObject) (api.DeleteIndexResponseObject, error) {
	result, err := h.deleteIndex.Execute(action.DeleteIndexRequest{Name: request.Index})
	if err != nil {
		return mapError(err)
	}
	return api.DeleteIndex200JSONResponse{Acknowledged: result.Acknowledged}, nil
}
```

- [ ] **Step 4: Implement GetIndex**

```go
func (h *Handler) GetIndex(ctx context.Context, request api.GetIndexRequestObject) (api.GetIndexResponseObject, error) {
	result, err := h.getIndex.Execute(action.GetIndexRequest{Name: request.Index})
	if err != nil {
		return mapError(err)
	}

	props := make(map[string]api.FieldMapping)
	if result.Mapping != nil {
		for name, fm := range result.Mapping.Properties {
			m := api.FieldMapping{Type: string(fm.Type)}
			if fm.Analyzer != "" {
				m.Analyzer = &fm.Analyzer
			}
			props[name] = m
		}
	}

	resp := api.GetIndexResponse(map[string]struct {
		Mappings *api.MappingsDefinition `json:"mappings,omitempty"`
		Settings *api.IndexSettings      `json:"settings,omitempty"`
	}{
		request.Index: {
			Settings: &api.IndexSettings{
				NumberOfShards:   &result.Settings.NumberOfShards,
				NumberOfReplicas: &result.Settings.NumberOfReplicas,
			},
			Mappings: &api.MappingsDefinition{Properties: &props},
		},
	})
	return api.GetIndex200JSONResponse(resp), nil
}
```

- [ ] **Step 5: Implement IndexDocumentPut and IndexDocumentPost**

```go
func (h *Handler) IndexDocumentPut(ctx context.Context, request api.IndexDocumentPutRequestObject) (api.IndexDocumentPutResponseObject, error) {
	return h.indexDocument(request.Index, request.Id, request.Body)
}

func (h *Handler) IndexDocumentPost(ctx context.Context, request api.IndexDocumentPostRequestObject) (api.IndexDocumentPostResponseObject, error) {
	return h.indexDocument(request.Index, request.Id, request.Body)
}

func (h *Handler) indexDocument(index, id string, body *api.DocumentSource) (any, error) {
	source, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	result, err := h.indexDoc.Execute(action.IndexDocumentRequest{
		Index:  index,
		ID:     id,
		Source: source,
	})
	if err != nil {
		return mapError(err)
	}
	return api.IndexDocumentPut201JSONResponse{
		Index:  result.Index,
		Id:     result.ID,
		Result: result.Result,
	}, nil
}
```

Note: The exact return types for Put vs Post may differ in the generated code. Adjust the shared `indexDocument` helper to return the correct generated response type for each. If the generated types are identical in shape, a type assertion or separate wrappers may be needed.

- [ ] **Step 6: Implement GetDocument**

```go
func (h *Handler) GetDocument(ctx context.Context, request api.GetDocumentRequestObject) (api.GetDocumentResponseObject, error) {
	result, err := h.getDoc.Execute(action.GetDocumentRequest{
		Index: request.Index,
		ID:    request.Id,
	})
	if err != nil {
		return mapError(err)
	}

	if !result.Found {
		return api.GetDocument404JSONResponse{
			Index: result.Index,
			Id:    result.ID,
			Found: false,
		}, nil
	}

	var source map[string]interface{}
	json.Unmarshal(result.Source, &source)

	return api.GetDocument200JSONResponse{
		Index:  result.Index,
		Id:     result.ID,
		Found:  true,
		Source:  &source,
	}, nil
}
```

- [ ] **Step 7: Implement DeleteDocument**

```go
func (h *Handler) DeleteDocument(ctx context.Context, request api.DeleteDocumentRequestObject) (api.DeleteDocumentResponseObject, error) {
	result, err := h.deleteDoc.Execute(action.DeleteDocumentRequest{
		Index: request.Index,
		ID:    request.Id,
	})
	if err != nil {
		return mapError(err)
	}

	resp := api.DeleteDocumentResponse{
		Index:  result.Index,
		Id:     result.ID,
		Result: result.Result,
	}

	if result.Result == "not_found" {
		return api.DeleteDocument404JSONResponse(resp), nil
	}
	return api.DeleteDocument200JSONResponse(resp), nil
}
```

- [ ] **Step 8: Implement SearchIndexGet and SearchIndexPost**

```go
func (h *Handler) SearchIndexGet(ctx context.Context, request api.SearchIndexGetRequestObject) (api.SearchIndexGetResponseObject, error) {
	return h.searchIndex(request.Index, request.Params.Size, request.Body)
}

func (h *Handler) SearchIndexPost(ctx context.Context, request api.SearchIndexPostRequestObject) (api.SearchIndexPostResponseObject, error) {
	return h.searchIndex(request.Index, request.Params.Size, request.Body)
}

func (h *Handler) searchIndex(index string, querySize *int, body *api.SearchRequest) (any, error) {
	var queryJSON map[string]any
	size := 10

	if body != nil {
		if body.Query != nil {
			queryJSON = body.Query.AdditionalProperties
		}
		if body.Size != nil {
			size = *body.Size
		}
	}

	// Query param overrides body
	if querySize != nil {
		size = *querySize
	}

	if queryJSON == nil {
		queryJSON = map[string]any{"match_all": map[string]any{}}
	}

	result, err := h.searchAction.Execute(action.SearchRequest{
		Index:     index,
		QueryJSON: queryJSON,
		Size:      size,
	})
	if err != nil {
		return mapError(err)
	}

	hits := make([]api.SearchHit, len(result.Hits.Hits))
	for i, hit := range result.Hits.Hits {
		var source map[string]interface{}
		json.Unmarshal(hit.Source, &source)
		hits[i] = api.SearchHit{
			Index:  hit.Index,
			Id:     hit.ID,
			Score:  hit.Score,
			Source: &source,
		}
	}

	return api.SearchIndexGet200JSONResponse{
		Took: result.Took,
		Hits: api.SearchHits{
			Total:    api.TotalHits{Value: result.Hits.Total.Value, Relation: result.Hits.Total.Relation},
			MaxScore: result.Hits.MaxScore,
			Hits:     hits,
		},
	}, nil
}
```

- [ ] **Step 9: Implement BulkGlobal and BulkIndex**

```go
func (h *Handler) BulkGlobal(ctx context.Context, request api.BulkGlobalRequestObject) (api.BulkGlobalResponseObject, error) {
	return h.bulk("", request.Body)
}

func (h *Handler) BulkIndex(ctx context.Context, request api.BulkIndexRequestObject) (api.BulkIndexResponseObject, error) {
	return h.bulk(request.Index, request.Body)
}

func (h *Handler) bulk(defaultIndex string, body io.Reader) (any, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "parse_exception", "failed to read bulk body"), nil
	}

	items, err := parseBulkNDJSON(data, defaultIndex)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "parse_exception", "failed to parse bulk request: "+err.Error()), nil
	}

	result, err := h.bulkAction.Execute(action.BulkRequest{Items: items})
	if err != nil {
		return errorResponse(http.StatusInternalServerError, "bulk_exception", err.Error()), nil
	}

	responseItems := make([]map[string]api.BulkItemResult, len(result.Items))
	for i, item := range result.Items {
		entry := api.BulkItemResult{
			Index:  item.Index,
			Id:     item.ID,
			Status: item.Status,
		}
		if item.Error != nil {
			entry.Error = &struct {
				Reason *string `json:"reason,omitempty"`
				Type   *string `json:"type,omitempty"`
			}{
				Type:   &item.Error.Type,
				Reason: &item.Error.Reason,
			}
		}
		responseItems[i] = map[string]api.BulkItemResult{item.Action: entry}
	}

	return api.BulkGlobal200JSONResponse{
		Took:   result.Took,
		Errors: result.Errors,
		Items:  responseItems,
	}, nil
}
```

- [ ] **Step 10: Implement RefreshIndex**

```go
func (h *Handler) RefreshIndex(ctx context.Context, request api.RefreshIndexRequestObject) (api.RefreshIndexResponseObject, error) {
	result, err := h.refreshAction.Execute(action.RefreshRequest{Index: request.Index})
	if err != nil {
		return mapError(err)
	}

	return api.RefreshIndex200JSONResponse{
		Shards: struct {
			Failed     int `json:"failed"`
			Successful int `json:"successful"`
			Total      int `json:"total"`
		}{
			Total:      result.Shards,
			Successful: result.Shards,
			Failed:     0,
		},
	}, nil
}
```

- [ ] **Step 11: Implement mapError and parseBulkNDJSON helpers**

Add to `handler.go`:
```go
func mapError(err error) (any, error) {
	var notFound *action.IndexNotFoundError
	var alreadyExists *action.IndexAlreadyExistsError
	var invalidName *action.InvalidIndexNameError
	var queryParsing *action.QueryParsingError
	var mapperParsing *action.MapperParsingError

	switch {
	case errors.As(err, &notFound):
		return errorResponse(http.StatusNotFound, "index_not_found_exception", err.Error()), nil
	case errors.As(err, &alreadyExists):
		return errorResponse(http.StatusBadRequest, "resource_already_exists_exception", err.Error()), nil
	case errors.As(err, &invalidName):
		return errorResponse(http.StatusBadRequest, "invalid_index_name_exception", err.Error()), nil
	case errors.As(err, &queryParsing):
		return errorResponse(http.StatusBadRequest, "query_parsing_exception", err.Error()), nil
	case errors.As(err, &mapperParsing):
		return errorResponse(http.StatusBadRequest, "mapper_parsing_exception", err.Error()), nil
	default:
		return errorResponse(http.StatusInternalServerError, "internal_error", err.Error()), nil
	}
}

func errorResponse(status int, errType, reason string) api.ErrorResponse {
	return api.ErrorResponse{
		Error: struct {
			Reason string `json:"reason"`
			Type   string `json:"type"`
		}{
			Type:   errType,
			Reason: reason,
		},
		Status: status,
	}
}
```

Add the NDJSON parser (moved from `server/rest/action/bulk.go`):
```go
func parseBulkNDJSON(body []byte, defaultIndex string) ([]action.BulkItem, error) {
	var items []action.BulkItem
	scanner := bufio.NewScanner(bytes.NewReader(body))

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var actionLine map[string]json.RawMessage
		if err := json.Unmarshal(line, &actionLine); err != nil {
			return nil, err
		}

		for actionName, meta := range actionLine {
			var metaObj struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			}
			if err := json.Unmarshal(meta, &metaObj); err != nil {
				return nil, err
			}

			idx := metaObj.Index
			if idx == "" {
				idx = defaultIndex
			}

			item := action.BulkItem{
				Action: actionName,
				Index:  idx,
				ID:     metaObj.ID,
			}

			if actionName == "index" || actionName == "create" {
				if !scanner.Scan() {
					return nil, fmt.Errorf("missing source line for %s action", actionName)
				}
				sourceLine := bytes.TrimSpace(scanner.Bytes())
				item.Source = json.RawMessage(make([]byte, len(sourceLine)))
				copy(item.Source, sourceLine)
			}

			items = append(items, item)
		}
	}

	return items, scanner.Err()
}
```

- [ ] **Step 12: Verify the handler compiles**

Run:
```bash
go build ./server/handler/...
```

Expected: compiles. If generated type names differ from what's shown above, adjust to match.

- [ ] **Step 13: Commit**

```bash
git add server/handler/handler.go
git commit -m "feat: implement StrictServerImpl handler adapting transport actions"
```

---

### Task 5: Rewire Node to use Chi router

**Files:**
- Modify: `server/node/node.go`

- [ ] **Step 1: Rewrite node.go**

Replace the contents of `server/node/node.go` with:
```go
package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"gosearch/analysis"
	"gosearch/api"
	"gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/handler"
	"gosearch/server/index"
)

type NodeConfig struct {
	DataPath string
	HTTPPort int
}

type Node struct {
	config        NodeConfig
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	router        chi.Router
	registry      *analysis.AnalyzerRegistry
	httpServer    *http.Server
	listener      net.Listener
	stopped       bool
}

func NewNode(config NodeConfig) (*Node, error) {
	cs := cluster.NewClusterState()
	indexServices := make(map[string]*index.IndexService)
	registry := analysis.DefaultRegistry()

	// Create transport actions
	createAction := action.NewTransportCreateIndexAction(cs, indexServices, config.DataPath, registry)
	deleteAction := action.NewTransportDeleteIndexAction(cs, indexServices, config.DataPath)
	getAction := action.NewTransportGetIndexAction(cs)
	indexDocAction := action.NewTransportIndexAction(cs, indexServices)
	getDocAction := action.NewTransportGetAction(cs, indexServices)
	deleteDocAction := action.NewTransportDeleteDocumentAction(cs, indexServices)
	refreshAction := action.NewTransportRefreshAction(cs, indexServices)
	searchAction := action.NewTransportSearchAction(cs, indexServices, registry)
	bulkAction := action.NewTransportBulkAction(cs, indexServices)

	// Create handler
	h := handler.NewHandler(
		createAction, deleteAction, getAction,
		indexDocAction, getDocAction, deleteDocAction,
		searchAction, bulkAction, refreshAction,
	)

	// Create Chi router with generated routes
	strictHandler := api.NewStrictHandlerWithOptions(h, nil, api.StrictHTTPServerOptions{})
	router := chi.NewRouter()
	api.HandlerFromMux(strictHandler, router)

	n := &Node{
		config:        config,
		clusterState:  cs,
		indexServices: indexServices,
		router:        router,
		registry:      registry,
	}

	return n, nil
}

func (n *Node) Start() (string, error) {
	addr := fmt.Sprintf(":%d", n.config.HTTPPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	n.listener = listener

	n.httpServer = &http.Server{
		Handler: n.router,
	}

	go n.httpServer.Serve(listener)

	return listener.Addr().String(), nil
}

func (n *Node) Stop() error {
	if n.stopped {
		return nil
	}
	n.stopped = true

	for name, svc := range n.indexServices {
		svc.Close()
		delete(n.indexServices, name)
	}

	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return n.httpServer.Shutdown(ctx)
	}
	return nil
}

func (n *Node) ClusterState() *cluster.ClusterState {
	return n.clusterState
}

func (n *Node) IndexService(name string) *index.IndexService {
	return n.indexServices[name]
}
```

Note: `RestController()` and `ActionRegistry()` accessors are removed since they are no longer needed. If any code references them, it will fail to compile — which is the desired behavior (we want to find and remove all old REST layer references).

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./server/...
```

Expected: compiles. If there are compilation errors from other packages referencing `RestController` or the old `rest` package, those references need to be removed (should only be in test files or the deleted REST layer).

- [ ] **Step 3: Commit**

```bash
git add server/node/node.go
git commit -m "feat: rewire Node to use Chi router with generated handler"
```

---

### Task 6: Delete the old REST layer

**Files:**
- Delete: `server/rest/controller.go`
- Delete: `server/rest/request.go`
- Delete: `server/rest/response.go`
- Delete: `server/rest/action/bulk.go`
- Delete: `server/rest/action/create_index.go`
- Delete: `server/rest/action/delete_document.go`
- Delete: `server/rest/action/delete_index.go`
- Delete: `server/rest/action/get_document.go`
- Delete: `server/rest/action/get_index.go`
- Delete: `server/rest/action/index_document.go`
- Delete: `server/rest/action/refresh.go`
- Delete: `server/rest/action/search.go`
- Modify: `server/node/node.go` (if any leftover imports)

- [ ] **Step 1: Delete the entire server/rest directory**

Run:
```bash
rm -rf server/rest
```

- [ ] **Step 2: Remove the transport/action.go registry if no longer used**

Check if `server/transport/action.go` (`ActionRegistry`) is referenced anywhere outside the old REST layer and node.go. If it's only used by the old node.go wiring (which we already removed), delete it:

Run:
```bash
rm -rf server/transport
```

- [ ] **Step 3: Verify everything compiles**

Run:
```bash
go build ./...
```

Expected: compiles without errors.

- [ ] **Step 4: Run all tests**

Run:
```bash
go test ./...
```

Expected: all tests in `server/action/` pass. There should be no tests left in `server/rest/` since we deleted it.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: delete old REST layer and transport registry"
```

---

### Task 7: Add OpenAPI validation middleware

**Files:**
- Modify: `server/node/node.go`

- [ ] **Step 1: Add validation middleware to the router**

In `server/node/node.go`, add the OpenAPI validation middleware. Update the `NewNode` function to load the spec and apply middleware:

Add imports:
```go
import (
	// ... existing imports ...
	"embed"
	oapimiddleware "github.com/oapi-codegen/netherwork-middleware"
	"github.com/getkin/kin-openapi/openapi3"
)
```

Before the `api.HandlerFromMux` call, add:
```go
// Load and validate OpenAPI spec
spec, err := api.GetSwagger()
if err != nil {
    return nil, fmt.Errorf("load openapi spec: %w", err)
}
spec.Servers = nil // Clear servers so validation doesn't check host

// Add validation middleware
router.Use(oapimiddleware.OapiRequestValidatorWithOptions(spec, &oapimiddleware.Options{
    ErrorHandler: func(w http.ResponseWriter, message string, statusCode int) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(statusCode)
        json.NewEncoder(w).Encode(map[string]any{
            "error": map[string]any{
                "type":   "validation_exception",
                "reason": message,
            },
            "status": statusCode,
        })
    },
}))
```

Note: The exact middleware package import path may vary — check what oapi-codegen v2 currently recommends. It may be `github.com/oapi-codegen/oapi-codegen/v2/pkg/chi-middleware` instead.

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./server/...
```

- [ ] **Step 3: Commit**

```bash
git add server/node/node.go go.mod go.sum
git commit -m "feat: add OpenAPI request validation middleware"
```

---

### Task 8: Integration tests

**Files:**
- Create: `server/handler/handler_test.go`

- [ ] **Step 1: Write integration tests for the full HTTP stack**

Create `server/handler/handler_test.go`:
```go
package handler_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"gosearch/server/node"
)

func startTestNode(t *testing.T) string {
	t.Helper()
	n, err := node.NewNode(node.NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	addr, err := n.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { n.Stop() })
	return "http://" + addr
}

func TestCreateAndGetIndex(t *testing.T) {
	base := startTestNode(t)

	// Create index
	body := `{"settings":{"number_of_shards":1},"mappings":{"properties":{"title":{"type":"text"}}}}`
	resp, err := http.NewRequest("PUT", base+"/testindex", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp2 := doRequest(t, resp)
	if resp2.StatusCode != 200 {
		t.Fatalf("create index: status %d", resp2.StatusCode)
	}

	// Get index
	req, _ := http.NewRequest("GET", base+"/testindex", nil)
	resp2 = doRequest(t, req)
	if resp2.StatusCode != 200 {
		t.Fatalf("get index: status %d", resp2.StatusCode)
	}
}

func TestIndexAndGetDocument(t *testing.T) {
	base := startTestNode(t)

	// Create index
	body := `{"mappings":{"properties":{"title":{"type":"text"}}}}`
	req, _ := http.NewRequest("PUT", base+"/docs", bytes.NewBufferString(body))
	doRequest(t, req)

	// Index document
	req, _ = http.NewRequest("PUT", base+"/docs/_doc/1", bytes.NewBufferString(`{"title":"hello world"}`))
	resp := doRequest(t, req)
	if resp.StatusCode != 201 {
		t.Fatalf("index doc: status %d", resp.StatusCode)
	}

	// Refresh
	req, _ = http.NewRequest("POST", base+"/docs/_refresh", nil)
	doRequest(t, req)

	// Get document
	req, _ = http.NewRequest("GET", base+"/docs/_doc/1", nil)
	resp = doRequest(t, req)
	if resp.StatusCode != 200 {
		t.Fatalf("get doc: status %d", resp.StatusCode)
	}
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	if result["found"] != true {
		t.Errorf("expected found=true, got %v", result["found"])
	}
}

func TestSearchEndpoint(t *testing.T) {
	base := startTestNode(t)

	// Setup: create index, add docs, refresh
	req, _ := http.NewRequest("PUT", base+"/products", bytes.NewBufferString(`{"mappings":{"properties":{"name":{"type":"text"}}}}`))
	doRequest(t, req)

	for i, name := range []string{"wireless mouse", "wireless keyboard", "wired mouse"} {
		req, _ = http.NewRequest("PUT", base+fmt.Sprintf("/products/_doc/%d", i+1), bytes.NewBufferString(fmt.Sprintf(`{"name":"%s"}`, name)))
		doRequest(t, req)
	}

	req, _ = http.NewRequest("POST", base+"/products/_refresh", nil)
	doRequest(t, req)

	// Search
	req, _ = http.NewRequest("POST", base+"/products/_search", bytes.NewBufferString(`{"query":{"match":{"name":"wireless"}}}`))
	resp := doRequest(t, req)
	if resp.StatusCode != 200 {
		t.Fatalf("search: status %d", resp.StatusCode)
	}
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	hits := result["hits"].(map[string]any)
	total := hits["total"].(map[string]any)
	if int(total["value"].(float64)) != 2 {
		t.Errorf("expected 2 hits, got %v", total["value"])
	}
}

func TestValidation_InvalidIndexName(t *testing.T) {
	base := startTestNode(t)

	// Index name with uppercase should be rejected by validation middleware
	req, _ := http.NewRequest("PUT", base+"/INVALID", bytes.NewBufferString(`{}`))
	resp := doRequest(t, req)
	if resp.StatusCode != 400 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 for invalid index name, got %d: %s", resp.StatusCode, body)
	}
}

func TestBulkEndpoint(t *testing.T) {
	base := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", base+"/bulk-test", bytes.NewBufferString(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	doRequest(t, req)

	// Bulk index
	ndjson := `{"index":{"_index":"bulk-test","_id":"1"}}
{"title":"doc one"}
{"index":{"_index":"bulk-test","_id":"2"}}
{"title":"doc two"}
`
	req, _ = http.NewRequest("POST", base+"/_bulk", bytes.NewBufferString(ndjson))
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp := doRequest(t, req)
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("bulk: status %d: %s", resp.StatusCode, body)
	}
}

func doRequest(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	if req.Header.Get("Content-Type") == "" && req.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", req.Method, req.URL, err)
	}
	return resp
}
```

- [ ] **Step 2: Run the integration tests**

Run:
```bash
go test ./server/handler/... -v
```

Expected: all tests pass.

- [ ] **Step 3: Run all tests to confirm no regressions**

Run:
```bash
go test ./...
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add server/handler/handler_test.go
git commit -m "test: add integration tests for OpenAPI-generated HTTP layer"
```

---

### Task 9: Clean up unused code

**Files:**
- Modify: `server/action/*.go` (remove unused imports if any)
- Potentially modify: `go.mod` (tidy)

- [ ] **Step 1: Run go mod tidy**

Run:
```bash
go mod tidy
```

- [ ] **Step 2: Run go vet**

Run:
```bash
go vet ./...
```

Expected: no issues.

- [ ] **Step 3: Run all tests one final time**

Run:
```bash
go test ./...
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: tidy modules and clean up unused code"
```
