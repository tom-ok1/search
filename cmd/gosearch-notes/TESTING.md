# gosearch-notes Exploratory Testing Report

Date: 2026-03-20

## Crashes / Panics

### ~~`--limit 0` causes panic (Critical)~~ FIXED

`NewTopKCollector` and `NewTopFieldCollector` now clamp `k` to a minimum of 1, so `--limit 0` returns 1 result instead of panicking.

### ~~`--limit -1` causes panic (Critical)~~ FIXED

Same fix — `k < 1` is clamped to 1, preventing the `makeslice: cap out of range` panic.

### ~~Adding note with empty body crashes (High)~~ FIXED

The CLI now validates that `--body` is non-empty. Additionally, the segment writer skips writing postings/FST for fields with no indexed terms, preventing the crash at the indexer level for any empty text field.

## Bugs

### No priority validation (Medium)

The `--priority` flag accepts any integer despite docs stating the range is 1-10.

```
$ gosearch-notes add --title "X" --body "y" --priority -1    # accepted
$ gosearch-notes add --title "X" --body "y" --priority 999999999  # accepted
```

### Deleting non-existent tag reports success (Medium)

```
$ echo "y" | gosearch-notes delete --tag nonexistent
Delete all documents where tag = "nonexistent"? (y/n): Documents deleted successfully.
```

Should report "No matching documents found" or similar.

### Invalid sort field silently ignored (Medium)

```
$ gosearch-notes search "go" --sort invalid
```

Returns results with default sorting instead of reporting an error.

### ~~Sort by priority incorrect top-K with large dataset (Medium)~~ FIXED

`GetLeafCollector` was not calling `SetBottom` on newly created leaf comparators when moving to a new segment. The bottom value defaulted to zero, so no documents could beat the bottom after the heap filled. Fixed by initializing `SetBottom` on new leaf comparators when the heap is already full.

### Combined delete flags: `--title` silently ignored (Low)

```
$ gosearch-notes delete --tag test --title "something"
Delete all documents where tag = "test"? (y/n):
```

Only `--tag` is used. Should either combine both filters or return an error.

### Binary .md files imported without warning (Low)

```
$ dd if=/dev/urandom of=binary.md bs=1024 count=1
$ gosearch-notes import .
Imported 1 notes from 1 files.
```

No validation that the file contains valid text.

## Functional Limitations

### CJK / Unicode text not searchable

Notes with Japanese (and likely other CJK) content can be added but produce 0 search results. The tokenizer does not support CJK character segmentation.

```
$ gosearch-notes add --title "日本語テスト" --body "これはテストです"
Note added successfully.
$ gosearch-notes search "テスト"
=== Results (0 hits) ===
```

### No segment merging

Each `add` call creates a new segment. After 1000 adds, the index has 1000 segments. No auto-merge is triggered.

```
Total documents:   1000
Live documents:    1000
Segments:          1000
```

### Slow sequential inserts

1000 sequential note additions took ~6 minutes 13 seconds (~370ms per note). No batch API is available.

### BM25 scoring degradation

When all documents contain identical terms, all scores collapse to 0.001, making relevance ranking ineffective.

## Working Correctly

- Basic add / search / delete / import / stats workflow
- Phrase search (correct order matching, rejects wrong order)
- `--not` exclusion filter
- Tag and category filters (including combined)
- Sort by created / priority / category with `--desc` reversal
- Empty title validation (`--title ""` and missing `--title` both rejected)
- Non-numeric priority rejection (flag parser handles this)
- Concurrent read access (two simultaneous searches)
- Multiline body text
- Recursive directory import (including nested subdirectories)
- Non-.md files correctly skipped during import
- Graceful handling of missing/empty import directories
- Duplicate note insertion (both copies stored and searchable)
- Unknown command and missing command show usage help
- Delete confirmation prompt (y/n) works correctly
- Tags with spaces and special characters (c++, c#, .net)
- Very long titles (500 chars) and bodies (100K chars)
