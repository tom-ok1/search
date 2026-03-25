# GoSearch Server — Project Progress Tracker

## Overview

Building an Elasticsearch-compatible server layer on top of GoSearch's Lucene-like search engine.

**Design doc:** `docs/elasticsearch-alt-design.md`
**Plans dir:** `docs/superpowers/plans/`

---

## Step 1: Core Infrastructure
**Plan:** `docs/superpowers/plans/2026-03-25-step1-core-infrastructure.md`
**Status:** NOT STARTED

- [ ] Task 1: Cluster State & Metadata (`server/cluster/`)
- [ ] Task 2: Transport Action Registry (`server/transport/`)
- [ ] Task 3: REST Controller (`server/rest/`)
- [ ] Task 4: Node Lifecycle (`server/node/`)
- [ ] Task 5: End-to-end integration test

**Deliverable:** Node starts, listens on HTTP, returns 404 JSON for all routes.

---

## Step 2: Mapping & Document Parsing
**Status:** NOT STARTED

- [ ] Define `mapping.FieldType` constants and `mapping.MappingDefinition`
- [ ] Implement `mapping.ParseDocument` (JSON source + mapping → `document.Document`)
- [ ] Handle text, keyword, long, double, boolean field types
- [ ] Add `_id` and `_source` fields
- [ ] Unit tests for each field type

**Deliverable:** Given a mapping and JSON doc, produces correct Lucene document.

---

## Step 3: Index & Shard Layer
**Status:** NOT STARTED

- [ ] Implement `index.Engine` (wraps IndexWriter, manages reader/searcher, refresh)
- [ ] Implement `index.IndexShard` (owns Engine, Index/Delete/Refresh/Searcher)
- [ ] Implement `index.IndexService` (manages shards, holds mapping)
- [ ] Shard routing function

**Deliverable:** Programmatic API to create index, write docs, refresh, get searcher.

---

## Step 4: Index Management Actions + REST
**Status:** NOT STARTED

- [ ] `TransportCreateIndexAction` + `RestCreateIndexAction`
- [ ] `TransportDeleteIndexAction` + `RestDeleteIndexAction`
- [ ] `TransportGetIndexAction` + `RestGetIndexAction`
- [ ] Register routes and actions in Node

**Deliverable:** `PUT /myindex`, `GET /myindex`, `DELETE /myindex` work via curl.

---

## Step 5: Document Indexing Actions + REST
**Status:** NOT STARTED

- [ ] `TransportIndexAction` + `RestIndexAction`
- [ ] `TransportGetAction` + `RestGetAction`
- [ ] `TransportDeleteAction` + `RestDeleteAction`
- [ ] `TransportRefreshAction` + `RestRefreshAction`

**Deliverable:** Full document lifecycle: index → refresh → get → delete.

---

## Step 6: Query DSL Parser
**Status:** NOT STARTED

- [ ] `QueryParser` with mapping-aware analysis
- [ ] `term` query support
- [ ] `match` query support
- [ ] `bool` query support
- [ ] Unit tests for each query type

**Deliverable:** JSON query DSL correctly produces Lucene query trees.

---

## Step 7: Search Action + REST
**Status:** NOT STARTED

- [ ] `TransportSearchAction` (query-then-fetch)
- [ ] Support `size` parameter
- [ ] Support sort
- [ ] `RestSearchAction` (GET + POST)
- [ ] SearchResponse JSON matching Elasticsearch format

**Deliverable:** `POST /myindex/_search {"query": {"match": {"title": "hello"}}}` returns ranked results.

---

## Step 8: Bulk API
**Status:** NOT STARTED

- [ ] NDJSON parser
- [ ] `TransportBulkAction`
- [ ] `RestBulkAction`

**Deliverable:** `POST /_bulk` with NDJSON body indexes multiple documents.

---

## Step 9: Integration Tests & Error Handling
**Status:** NOT STARTED

- [ ] E2E test: create index → bulk index → refresh → search → verify
- [ ] E2E test: error cases (index not found, invalid mapping, malformed query)
- [ ] Consistent Elasticsearch-compatible error responses
- [ ] Input validation across all endpoints

**Deliverable:** Robust, well-tested server with ES-compatible error responses.
