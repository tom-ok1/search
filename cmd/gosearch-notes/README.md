# gosearch-notes

A local knowledge base CLI built on the gosearch full-text search engine.

## Build

```bash
go build -o gosearch-notes ./cmd/gosearch-notes/
```

## Index Location

By default, the index is stored at `~/.gosearch-notes/index/`.

Override with the `GOSEARCH_INDEX_DIR` environment variable:

```bash
export GOSEARCH_INDEX_DIR=/path/to/custom/index
```

## Commands

### add

Add a single note to the index.

```bash
gosearch-notes add --title "Go Concurrency" --body "Goroutines and channels..." --tags "go,concurrency" --category tutorial --priority 8
```

| Flag | Default | Description |
|------|---------|-------------|
| `--title` | (required) | Note title |
| `--body` | `""` | Note body text |
| `--tags` | `""` | Comma-separated tags |
| `--category` | `uncategorized` | Category name |
| `--priority` | `5` | Priority level (1-10) |

### search

Search notes by text, tags, category, with sorting options.

```bash
# Basic text search
gosearch-notes search "error handling"

# Phrase search (terms must appear consecutively)
gosearch-notes search --phrase "error handling"

# Filter by tag and category
gosearch-notes search "go" --tag tutorial --category tech

# Exclude terms
gosearch-notes search "go" --not "legacy deprecated"

# Sort by priority (highest first by default)
gosearch-notes search --tag go --sort priority

# Sort by creation date (newest first by default)
gosearch-notes search "concurrency" --sort created

# Sort by category (alphabetical)
gosearch-notes search "go" --sort category

# Reverse the default sort direction
gosearch-notes search --tag go --sort priority --desc

# Limit results
gosearch-notes search "go" --limit 5
```

| Flag | Default | Description |
|------|---------|-------------|
| `--tag` | `""` | Filter by tag |
| `--category` | `""` | Filter by category |
| `--not` | `""` | Exclude documents containing these terms (space-separated) |
| `--phrase` | `false` | Match terms as an exact phrase |
| `--sort` | `score` | Sort by: `score`, `priority`, `created`, `category` |
| `--desc` | `false` | Reverse the default sort direction |
| `--limit` | `10` | Maximum number of results |

**Default sort directions:**
- `score` — highest first
- `priority` — highest first (use `--desc` for lowest first)
- `created` — newest first (use `--desc` for oldest first)
- `category` — alphabetical (use `--desc` for reverse)

### delete

Delete notes matching a field value. Prompts for confirmation before deleting.

```bash
# Delete all notes with tag "draft"
gosearch-notes delete --tag draft

# Delete notes matching a title term
gosearch-notes delete --title obsolete
```

| Flag | Description |
|------|-------------|
| `--tag` | Delete by tag match |
| `--title` | Delete by title term match |

### import

Bulk-import Markdown files from a directory (recursive).

```bash
gosearch-notes import ./notes/
```

Files with a YAML-style frontmatter block are parsed for metadata:

```markdown
---
title: My Note
tags: go, search
category: tech
priority: 8
---
The body text starts here...
```

Files without frontmatter use the filename (without extension) as the title and the entire content as the body.

### serve

Start an HTTP server for note management with auto-merge and near-real-time search.

```bash
gosearch-notes serve
gosearch-notes serve --port 9090 --commit-interval 10s --buffer-size 2000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8080` | HTTP port |
| `--commit-interval` | `5s` | How often to commit buffered documents |
| `--buffer-size` | `1000` | Index buffer size |

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/notes` | Add a note (JSON body: `{"title", "body", "tags", "category", "priority"}`) |
| `GET` | `/search` | Search notes (query params: `q`, `limit`, `sort`, `desc`, `not`, `tag`, `category`, `phrase`) |
| `GET` | `/stats` | Index statistics (`total_documents`, `live_documents`, `segments`) |
| `POST` | `/merge` | Force merge segments (optional `?max_segments=N`, default 1) |

```bash
# Add a note
curl -X POST localhost:8080/notes -d '{"title":"Go Patterns","body":"Useful concurrency patterns","tags":"go,concurrency","category":"tutorial","priority":8}'

# Search
curl "localhost:8080/search?q=concurrency&sort=priority&limit=5"

# Check stats
curl localhost:8080/stats

# Force merge to 1 segment
curl -X POST localhost:8080/merge
```

The server commits buffered documents at the configured interval, which triggers auto-merge via a tiered merge policy. Ctrl-C performs a graceful shutdown with a final commit.

### stats

Display index statistics.

```bash
gosearch-notes stats
```

Output:
```
Total documents:   10
Live documents:    8
Segments:          3
```

## Example Workflow

```bash
# Build
go build -o gosearch-notes ./cmd/gosearch-notes/

# Add some notes
gosearch-notes add --title "Go Error Handling" --body "Always check returned errors in Go" --tags "go,patterns" --category best-practices --priority 7
gosearch-notes add --title "Channel Patterns" --body "Use buffered channels for producer-consumer" --tags "go,concurrency" --category tutorial --priority 8
gosearch-notes add --title "Legacy API Notes" --body "Old REST endpoints to deprecate" --tags "api,legacy" --category tech --priority 2

# Search
gosearch-notes search "go" --sort priority
gosearch-notes search "channels" --tag concurrency
gosearch-notes search "go" --not legacy

# Import a directory of markdown notes
gosearch-notes import ./my-notes/

# Check index size
gosearch-notes stats

# Clean up drafts
gosearch-notes delete --tag legacy
```
