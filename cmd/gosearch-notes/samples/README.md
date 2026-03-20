# Sample Notes

10 sample markdown notes for trying out `gosearch-notes`.

## Quick Start

```bash
# Build the CLI
go build -o gosearch-notes ./cmd/gosearch-notes/

# Import all samples
gosearch-notes import ./cmd/gosearch-notes/samples/

# Check what was indexed
gosearch-notes stats
```

## Try These Searches

```bash
# Text search — find notes about Go
gosearch-notes search "go"

# Phrase search — match exact phrase
gosearch-notes search --phrase "error handling"

# Filter by tag
gosearch-notes search --tag concurrency

# Filter by category
gosearch-notes search --category best-practices

# Exclude terms — find "go" notes but skip legacy ones
gosearch-notes search "go" --not "legacy deprecated"

# Sort by priority (highest first)
gosearch-notes search --tag go --sort priority

# Sort by creation date
gosearch-notes search "search" --sort created

# Combine filters
gosearch-notes search "index" --category concepts --sort priority

# Limit results
gosearch-notes search "go" --limit 3

# Delete by tag
gosearch-notes delete --tag legacy
```

## What's Included

| # | Title | Tags | Category | Priority |
|---|-------|------|----------|----------|
| 01 | Getting Started with Go | go, basics, beginner | tutorial | 9 |
| 02 | Error Handling Patterns in Go | go, errors, patterns | best-practices | 8 |
| 03 | Concurrency with Goroutines and Channels | go, concurrency, goroutines, channels | tutorial | 8 |
| 04 | Full-Text Search Fundamentals | search, indexing, information-retrieval | concepts | 7 |
| 05 | Writing Tests in Go | go, testing, basics | best-practices | 7 |
| 06 | Building an Inverted Index | search, indexing, data-structures | concepts | 6 |
| 07 | REST API Design Guidelines | api, rest, http, design | best-practices | 5 |
| 08 | Docker Basics for Go Applications | docker, containers, deployment | devops | 4 |
| 09 | Understanding BM25 Scoring | search, bm25, scoring, information-retrieval | concepts | 6 |
| 10 | Legacy System Migration Notes | legacy, migration, deprecated | internal | 2 |
